import { pool } from "../config/db.js";
import { AppError } from "../Error/appError.js";
import logger from "../config/logger/index.js";
import config from "../config/index.js";
import { hashPassword, verifyPassword } from "../utils/password.js";
import { signAccessToken } from "../utils/jwt.js";
import { generateRefreshToken, hashToken } from "../utils/tokens.js";
import {
  createUser,
  findUserByEmail,
  findUserById,
  insertRefreshToken,
  findActiveRefreshTokenByHash,
  rotateRefreshToken,
  revokeRefreshToken,
  revokeAllRefreshTokensForUser,
} from "../repositories/auth.repository.js";
import { createTeam } from "../repositories/team.repository.js";
import { createMembership } from "../repositories/membership.repository.js";
import { SYSTEM_ROLE } from "../repositories/role.repository.js";

const REFRESH_TTL_MS = config.auth.refreshTokenTtlSeconds * 1000;

// A generic, timing-neutral message for anything auth-related that
// shouldn't reveal whether an email/token exists. Keeps signup/login/
// refresh from leaking enumeration signal, matching the discipline used
// on Snip's auth endpoints.
const INVALID_CREDENTIALS = "Invalid email or password";
const INVALID_REFRESH = "Invalid or expired refresh token";

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
    },
  };
}

/**
 * Creates a brand-new team + the signing-up user as its OWNER, then logs
 * them straight in. A user created this way has exactly one membership;
 * they can be invited into further teams later (phase 1.9).
 */
export async function signupService({ teamName, email, password }) {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");

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

    await client.query("COMMIT");

    return {
      ...buildAuthResponse({ accessToken, user }),
      team: { id: team.id, name: team.name, role: "OWNER" },
      refreshToken,
    };
  } catch (err) {
    await client.query("ROLLBACK");
    // Unique violation on email is translated by the central pg error
    // mapper (23505 -> 409), so just rethrow.
    throw err;
  } finally {
    client.release();
  }
}

export async function loginService({ email, password, userAgent, ip }) {
  const user = await findUserByEmail(email);

  // Enumeration-safe: run a verify against a dummy hash even when the
  // user doesn't exist, so response timing doesn't leak existence.
  const passwordHash =
    user?.password_hash ??
    "$argon2id$v=19$m=19456,t=2,p=1$AAAAAAAAAAAAAAAAAAAAAA$AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
  const isValid = await verifyPassword(passwordHash, password);

  if (!user || !isValid) {
    throw new AppError(INVALID_CREDENTIALS, 401);
  }

  const { accessToken, refreshToken, refreshTokenHash } =
    await issueTokenPair({ userId: user.id });

  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    await insertRefreshToken(client, {
      userId: user.id,
      tokenHash: refreshTokenHash,
      expiresAt: new Date(Date.now() + REFRESH_TTL_MS),
      userAgent,
      ip,
    });
    await client.query("COMMIT");
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }

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

  const client = await pool.connect();
  try {
    await client.query("BEGIN");

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

    await client.query("COMMIT");
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }

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
