import { pool } from "../config/db.js";
import { AppError } from "../Error/appError.js";
import logger from "../config/logger/index.js";
import config from "../config/index.js";
import { hashPassword, verifyPassword } from "../utils/password.js";
import { signAccessToken } from "../utils/jwt.js";
import {
  generateRefreshToken,
  generateOpaqueToken,
  generateOtpCode,
  hashToken,
  constantTimeEqual,
} from "../utils/tokens.js";
import {
  createUser,
  findUserByEmail,
  findUserById,
  findUserByIdWithPasswordHash,
  insertRefreshToken,
  findActiveRefreshTokenByHash,
  rotateRefreshToken,
  revokeRefreshToken,
  revokeAllRefreshTokensForUser,
  setEmailVerified,
  updateUserPassword,
  setTwoFactorEnabled,
  invalidatePendingEmailVerificationTokens,
  createEmailVerificationToken,
  findEmailVerificationTokenByHash,
  consumeEmailVerificationToken,
  invalidatePendingPasswordResetTokens,
  createPasswordResetToken,
  findPasswordResetTokenByHash,
  consumePasswordResetToken,
  invalidatePendingTwoFactorChallenges,
  createTwoFactorChallenge,
  findTwoFactorChallengeById,
  incrementTwoFactorAttempts,
  consumeTwoFactorChallenge,
} from "../repositories/auth.repository.js";
import { createTeam } from "../repositories/team.repository.js";
import { createMembership } from "../repositories/membership.repository.js";
import { SYSTEM_ROLE } from "../repositories/role.repository.js";
import {
  enqueueVerificationEmail,
  enqueuePasswordResetEmail,
  enqueueTwoFactorCodeEmail,
} from "../queues/mail.queue.js";

const REFRESH_TTL_MS = config.auth.refreshTokenTtlSeconds * 1000;
const EMAIL_VERIFICATION_TTL_MS = config.auth.emailVerificationTtlSeconds * 1000;
const PASSWORD_RESET_TTL_MS = config.auth.passwordResetTtlSeconds * 1000;
const TWO_FACTOR_CODE_TTL_MS = config.auth.twoFactorCodeTtlSeconds * 1000;

// A generic, timing-neutral message for anything auth-related that
// shouldn't reveal whether an email/token exists. Keeps signup/login/
// refresh from leaking enumeration signal, matching the discipline used
// on Snip's auth endpoints.
const INVALID_CREDENTIALS = "Invalid email or password";
const INVALID_REFRESH = "Invalid or expired refresh token";
const INVALID_OR_EXPIRED_TOKEN = "Invalid or expired token";
const INVALID_OR_EXPIRED_CODE = "Invalid or expired code";

// Same dummy hash used in loginService, reused wherever a password
// needs to be checked against a possibly-nonexistent user without
// leaking timing/enumeration signal.
const DUMMY_PASSWORD_HASH =
  "$argon2id$v=19$m=19456,t=2,p=1$AAAAAAAAAAAAAAAAAAAAAA$AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

// Identity-only now - no account_id/role. A user's role is per-team
// (team_memberships), resolved per-request by loadTeamContext, not
// baked into the token. See utils/jwt.js.
async function issueTokenPair({ userId }) {
  const accessToken = await signAccessToken({ userId });
  const { raw: refreshToken, hash: refreshTokenHash } = generateRefreshToken();
  return { accessToken, refreshToken, refreshTokenHash };
}

function buildAuthResponse({ accessToken, user }) {
  return {
    access_token: accessToken,
    token_type: "Bearer",
    expires_in: config.auth.accessTokenTtlSeconds,
    user: {
      id: user.id,
      email: user.email,
      email_verified: Boolean(user.email_verified_at),
      two_factor_enabled: Boolean(user.two_factor_enabled),
    },
  };
}

async function withTransaction(fn) {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const result = await fn(client);
    await client.query("COMMIT");
    return result;
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
}

async function issueEmailVerificationToken(client, { userId, email }) {
  await invalidatePendingEmailVerificationTokens(client, userId);
  const { raw, hash } = generateOpaqueToken();
  const expiresAt = new Date(Date.now() + EMAIL_VERIFICATION_TTL_MS);
  const token = await createEmailVerificationToken(client, {
    userId,
    tokenHash: hash,
    expiresAt,
  });
  return {
    tokenId: token.id,
    verifyUrl: `${config.dashboardOrigin}/verify-email/${raw}`,
    expiresAt,
    toEmail: email,
  };
}

/**
 * Creates a brand-new team + the signing-up user as its OWNER, then logs
 * them straight in. A user created this way has exactly one membership;
 * they can be invited into further teams later (phase 1.9).
 *
 * Also issues an email-verification token and enqueues the "verify your
 * email" send - signup succeeds either way, verification is best-effort
 * follow-up, not a gate on account creation.
 */
export async function signupService({ teamName, email, password }) {
  let verification;
  const result = await withTransaction(async (client) => {
    const passwordHash = await hashPassword(password);
    const user = await createUser(client, { email, passwordHash });
    const team = await createTeam(client, { name: teamName });
    await createMembership(client, {
      teamId: team.id,
      userId: user.id,
      roleId: SYSTEM_ROLE.OWNER,
    });

    const { accessToken, refreshToken, refreshTokenHash } =
      await issueTokenPair({ userId: user.id });

    await insertRefreshToken(client, {
      userId: user.id,
      tokenHash: refreshTokenHash,
      expiresAt: new Date(Date.now() + REFRESH_TTL_MS),
    });

    verification = await issueEmailVerificationToken(client, {
      userId: user.id,
      email: user.email,
    });

    return {
      ...buildAuthResponse({
        accessToken,
        user: { ...user, email_verified_at: null, two_factor_enabled: false },
      }),
      team: { id: team.id, name: team.name, role: "OWNER" },
      refreshToken,
    };
  });

  await enqueueVerificationEmail({
    tokenId: verification.tokenId,
    toEmail: verification.toEmail,
    verifyUrl: verification.verifyUrl,
    expiresAt: verification.expiresAt,
  });

  return result;
}

/**
 * Standard password check, then branches on two_factor_enabled: if 2FA
 * is on, no tokens are issued yet - a challenge is created and emailed,
 * and the caller must complete it via twoFactorLoginVerifyService.
 */
export async function loginService({ email, password, userAgent, ip }) {
  const user = await findUserByEmail(email);

  // Enumeration-safe: run a verify against a dummy hash even when the
  // user doesn't exist, so response timing doesn't leak existence.
  const passwordHash = user?.password_hash ?? DUMMY_PASSWORD_HASH;
  const isValid = await verifyPassword(passwordHash, password);

  if (!user || !isValid) {
    throw new AppError(INVALID_CREDENTIALS, 401);
  }

  if (user.two_factor_enabled) {
    const challenge = await withTransaction(async (client) => {
      await invalidatePendingTwoFactorChallenges(client, {
        userId: user.id,
        purpose: "login",
      });
      const { code, hash } = generateOtpCode();
      const expiresAt = new Date(Date.now() + TWO_FACTOR_CODE_TTL_MS);
      const row = await createTwoFactorChallenge(client, {
        userId: user.id,
        codeHash: hash,
        purpose: "login",
        expiresAt,
      });
      return { row, code, expiresAt };
    });

    await enqueueTwoFactorCodeEmail({
      challengeId: challenge.row.id,
      toEmail: user.email,
      code: challenge.code,
      expiresAt: challenge.expiresAt,
    });

    return {
      requires_2fa: true,
      challenge_id: challenge.row.id,
      expires_at: challenge.expiresAt,
    };
  }

  const { accessToken, refreshToken, refreshTokenHash } =
    await issueTokenPair({ userId: user.id });

  await withTransaction((client) =>
    insertRefreshToken(client, {
      userId: user.id,
      tokenHash: refreshTokenHash,
      expiresAt: new Date(Date.now() + REFRESH_TTL_MS),
      userAgent,
      ip,
    })
  );

  return { ...buildAuthResponse({ accessToken, user }), refreshToken };
}

/**
 * Completes a login started by loginService when 2FA is enabled. Wrong
 * codes increment the challenge's attempt counter; hitting the max
 * consumes the challenge outright so a new one has to be requested
 * rather than allowing indefinite guessing against one code.
 */
export async function twoFactorLoginVerifyService({
  challengeId,
  code,
  userAgent,
  ip,
}) {
  const challenge = await findTwoFactorChallengeById(challengeId);

  if (
    !challenge ||
    challenge.purpose !== "login" ||
    challenge.consumed_at ||
    challenge.expires_at < new Date()
  ) {
    throw new AppError(INVALID_OR_EXPIRED_CODE, 401);
  }

  if (challenge.attempts >= config.auth.twoFactorMaxAttempts) {
    await withTransaction((client) => consumeTwoFactorChallenge(client, challenge.id));
    throw new AppError("Too many attempts - request a new code", 429);
  }

  const isValid = constantTimeEqual(hashToken(code), challenge.code_hash);

  if (!isValid) {
    await withTransaction((client) =>
      incrementTwoFactorAttempts(client, challenge.id)
    );
    throw new AppError(INVALID_OR_EXPIRED_CODE, 401);
  }

  const user = await findUserById(challenge.user_id);
  if (!user) {
    throw new AppError(INVALID_OR_EXPIRED_CODE, 401);
  }

  const { accessToken, refreshToken, refreshTokenHash } =
    await issueTokenPair({ userId: user.id });

  await withTransaction(async (client) => {
    await consumeTwoFactorChallenge(client, challenge.id);
    await insertRefreshToken(client, {
      userId: user.id,
      tokenHash: refreshTokenHash,
      expiresAt: new Date(Date.now() + REFRESH_TTL_MS),
      userAgent,
      ip,
    });
  });

  return { ...buildAuthResponse({ accessToken, user }), refreshToken };
}

/**
 * Rotate-on-use refresh with reuse detection: presenting an already
 * revoked token nukes every active session for that user, since it's a
 * signal the token was stolen and used after the legitimate client
 * already rotated past it.
 */
export async function refreshService({ refreshToken, userAgent, ip }) {
  const tokenHash = hashToken(refreshToken);
  const existing = await findActiveRefreshTokenByHash(tokenHash);

  if (!existing) {
    throw new AppError(INVALID_REFRESH, 401);
  }

  if (existing.revoked_at) {
    logger.warn(
      { userId: existing.user_id, tokenId: existing.id },
      "[auth] refresh token reuse detected - revoking all sessions"
    );
    await revokeAllRefreshTokensForUser(pool, existing.user_id);
    throw new AppError(INVALID_REFRESH, 401);
  }

  if (existing.expires_at < new Date()) {
    throw new AppError(INVALID_REFRESH, 401);
  }

  const user = await findUserById(existing.user_id);
  if (!user) {
    throw new AppError(INVALID_REFRESH, 401);
  }

  const { accessToken, refreshToken: newRefreshToken, refreshTokenHash } =
    await issueTokenPair({ userId: user.id });

  await withTransaction(async (client) => {
    const newToken = await insertRefreshToken(client, {
      userId: user.id,
      tokenHash: refreshTokenHash,
      expiresAt: new Date(Date.now() + REFRESH_TTL_MS),
      userAgent,
      ip,
    });

    await rotateRefreshToken(client, {
      oldTokenId: existing.id,
      newTokenId: newToken.id,
    });
  });

  return {
    ...buildAuthResponse({ accessToken, user }),
    refreshToken: newRefreshToken,
  };
}

export async function logoutService({ refreshToken }) {
  const tokenHash = hashToken(refreshToken);
  const existing = await findActiveRefreshTokenByHash(tokenHash);

  // Idempotent: logging out an already-revoked/unknown token is a no-op,
  // not an error - the client's goal (be logged out) is already true.
  if (!existing || existing.revoked_at) {
    return;
  }

  await revokeRefreshToken(pool, existing.id);
}

// --- email verification ----------------------------------------------

export async function verifyEmailService(token) {
  const tokenHash = hashToken(token);
  const record = await findEmailVerificationTokenByHash(tokenHash);

  if (!record || record.consumed_at || record.expires_at < new Date()) {
    throw new AppError(INVALID_OR_EXPIRED_TOKEN, 400);
  }

  await withTransaction(async (client) => {
    await consumeEmailVerificationToken(client, record.id);
    await setEmailVerified(client, record.user_id);
  });
}

/**
 * Requires auth (caller must know their own userId) rather than taking
 * an email in the body, so this can't be used to spam an arbitrary
 * address with verification links.
 */
export async function resendVerificationService(userId) {
  const user = await findUserById(userId);
  if (!user) throw new AppError("User not found", 404);

  if (user.email_verified_at) {
    throw new AppError("Email is already verified", 409);
  }

  const verification = await withTransaction((client) =>
    issueEmailVerificationToken(client, { userId: user.id, email: user.email })
  );

  await enqueueVerificationEmail({
    tokenId: verification.tokenId,
    toEmail: verification.toEmail,
    verifyUrl: verification.verifyUrl,
    expiresAt: verification.expiresAt,
  });
}

// --- forgot / reset password -------------------------------------------

/**
 * Always resolves the same way regardless of whether the email exists -
 * enumeration-safe, same discipline as loginService. Only enqueues an
 * email when there's actually an account to reset.
 */
export async function forgotPasswordService({ email, ip }) {
  const user = await findUserByEmail(email);
  if (!user) return;

  const { tokenId, resetUrl, expiresAt } = await withTransaction(
    async (client) => {
      await invalidatePendingPasswordResetTokens(client, user.id);
      const { raw, hash } = generateOpaqueToken();
      const expiresAt = new Date(Date.now() + PASSWORD_RESET_TTL_MS);
      const token = await createPasswordResetToken(client, {
        userId: user.id,
        tokenHash: hash,
        expiresAt,
        requestedIp: ip,
      });
      return {
        tokenId: token.id,
        resetUrl: `${config.dashboardOrigin}/reset-password/${raw}`,
        expiresAt,
      };
    }
  );

  await enqueuePasswordResetEmail({
    tokenId,
    toEmail: user.email,
    resetUrl,
    expiresAt,
  });
}

/**
 * Resetting the password also revokes every existing refresh token for
 * the user - a password reset is a strong signal the old sessions
 * shouldn't be trusted anymore (lost device, compromised password,
 * etc), same as the reuse-detection kill-switch in refreshService.
 */
export async function resetPasswordService({ token, newPassword }) {
  const tokenHash = hashToken(token);
  const record = await findPasswordResetTokenByHash(tokenHash);

  if (!record || record.consumed_at || record.expires_at < new Date()) {
    throw new AppError(INVALID_OR_EXPIRED_TOKEN, 400);
  }

  const passwordHash = await hashPassword(newPassword);

  await withTransaction(async (client) => {
    await consumePasswordResetToken(client, record.id);
    await updateUserPassword(client, record.user_id, passwordHash);
    await revokeAllRefreshTokensForUser(client, record.user_id);
  });
}

// --- two-factor authentication (email OTP) ------------------------------

/**
 * Step 1 of enabling 2FA: re-verify the current password (standing in
 * for "prove you're still you" before turning on an extra login step),
 * then email a confirmation code. 2FA isn't actually enabled until
 * confirmTwoFactorEnableService succeeds.
 */
export async function initiateTwoFactorEnableService({ userId, password }) {
  const user = await findUserByIdWithPasswordHash(userId);
  if (!user) throw new AppError("User not found", 404);

  const isValid = await verifyPassword(user.password_hash, password);
  if (!isValid) {
    throw new AppError("Incorrect password", 401);
  }

  if (user.two_factor_enabled) {
    throw new AppError("Two-factor authentication is already enabled", 409);
  }

  const challenge = await withTransaction(async (client) => {
    await invalidatePendingTwoFactorChallenges(client, {
      userId: user.id,
      purpose: "enable",
    });
    const { code, hash } = generateOtpCode();
    const expiresAt = new Date(Date.now() + TWO_FACTOR_CODE_TTL_MS);
    const row = await createTwoFactorChallenge(client, {
      userId: user.id,
      codeHash: hash,
      purpose: "enable",
      expiresAt,
    });
    return { row, code, expiresAt };
  });

  await enqueueTwoFactorCodeEmail({
    challengeId: challenge.row.id,
    toEmail: user.email,
    code: challenge.code,
    expiresAt: challenge.expiresAt,
  });

  return { challenge_id: challenge.row.id, expires_at: challenge.expiresAt };
}

export async function confirmTwoFactorEnableService({
  userId,
  challengeId,
  code,
}) {
  const challenge = await findTwoFactorChallengeById(challengeId);

  if (
    !challenge ||
    challenge.user_id !== userId ||
    challenge.purpose !== "enable" ||
    challenge.consumed_at ||
    challenge.expires_at < new Date()
  ) {
    throw new AppError(INVALID_OR_EXPIRED_CODE, 401);
  }

  if (challenge.attempts >= config.auth.twoFactorMaxAttempts) {
    await withTransaction((client) => consumeTwoFactorChallenge(client, challenge.id));
    throw new AppError("Too many attempts - request a new code", 429);
  }

  const isValid = constantTimeEqual(hashToken(code), challenge.code_hash);
  if (!isValid) {
    await withTransaction((client) =>
      incrementTwoFactorAttempts(client, challenge.id)
    );
    throw new AppError(INVALID_OR_EXPIRED_CODE, 401);
  }

  await withTransaction(async (client) => {
    await consumeTwoFactorChallenge(client, challenge.id);
    await setTwoFactorEnabled(client, userId, true);
  });
}

/**
 * Disabling only needs the current password, not a fresh code - it's
 * lowering the account's security bar, and requiring the user to still
 * be able to receive email (which they might be losing access to,
 * hence wanting to disable 2FA) would be self-defeating.
 */
export async function disableTwoFactorService({ userId, password }) {
  const user = await findUserByIdWithPasswordHash(userId);
  if (!user) throw new AppError("User not found", 404);

  const isValid = await verifyPassword(user.password_hash, password);
  if (!isValid) {
    throw new AppError("Incorrect password", 401);
  }

  await withTransaction((client) => setTwoFactorEnabled(client, userId, false));
}
