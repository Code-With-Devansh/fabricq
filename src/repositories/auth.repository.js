import { pool } from "../config/db.js";


export async function createUser(client, { email, passwordHash }) {
  const { rows } = await client.query(
    `INSERT INTO users (email, password_hash)
     VALUES ($1, $2)
     RETURNING id, email, created_at`,
    [email, passwordHash]
  );
  return rows[0];
}

export async function findUserByEmail(email) {
  const { rows } = await pool.query(
    `SELECT id, email, password_hash, email_verified_at, two_factor_enabled, created_at
     FROM users
     WHERE email = $1`,
    [email]
  );
  return rows[0] ?? null;
}

export async function findUserById(userId) {
  const { rows } = await pool.query(
    `SELECT id, email, email_verified_at, two_factor_enabled, created_at
     FROM users
     WHERE id = $1`,
    [userId]
  );
  return rows[0] ?? null;
}

// Only used where a password re-check is actually needed (enabling/
// disabling 2FA) - kept separate from findUserById so password_hash
// doesn't ride along on the general-purpose lookup used throughout the
// request path.
export async function findUserByIdWithPasswordHash(userId) {
  const { rows } = await pool.query(
    `SELECT id, email, password_hash, email_verified_at, two_factor_enabled, created_at
     FROM users
     WHERE id = $1`,
    [userId]
  );
  return rows[0] ?? null;
}

export async function setEmailVerified(client, userId) {
  const { rows } = await client.query(
    `UPDATE users SET email_verified_at = now() WHERE id = $1 RETURNING *`,
    [userId]
  );
  return rows[0] ?? null;
}

export async function updateUserPassword(client, userId, passwordHash) {
  await client.query(`UPDATE users SET password_hash = $2 WHERE id = $1`, [
    userId,
    passwordHash,
  ]);
}

export async function setTwoFactorEnabled(client, userId, enabled) {
  const { rows } = await client.query(
    `UPDATE users SET two_factor_enabled = $2 WHERE id = $1 RETURNING *`,
    [userId, enabled]
  );
  return rows[0] ?? null;
}

// --- email verification --------------------------------------------------

// Mirrors the invitation pattern: invalidate any still-pending token for
// this user before issuing a new one, so an old link in an old email
// can't be used alongside a freshly requested one.
export async function invalidatePendingEmailVerificationTokens(client, userId) {
  await client.query(
    `UPDATE email_verification_tokens
     SET consumed_at = now()
     WHERE user_id = $1 AND consumed_at IS NULL`,
    [userId]
  );
}

export async function createEmailVerificationToken(
  client,
  { userId, tokenHash, expiresAt }
) {
  const { rows } = await client.query(
    `INSERT INTO email_verification_tokens (user_id, token_hash, expires_at)
     VALUES ($1, $2, $3)
     RETURNING *`,
    [userId, tokenHash, expiresAt]
  );
  return rows[0];
}

export async function findEmailVerificationTokenByHash(tokenHash) {
  const { rows } = await pool.query(
    `SELECT * FROM email_verification_tokens WHERE token_hash = $1`,
    [tokenHash]
  );
  return rows[0] ?? null;
}

export async function consumeEmailVerificationToken(client, tokenId) {
  await client.query(
    `UPDATE email_verification_tokens SET consumed_at = now() WHERE id = $1`,
    [tokenId]
  );
}

// --- password reset -------------------------------------------------------

export async function invalidatePendingPasswordResetTokens(client, userId) {
  await client.query(
    `UPDATE password_reset_tokens
     SET consumed_at = now()
     WHERE user_id = $1 AND consumed_at IS NULL`,
    [userId]
  );
}

export async function createPasswordResetToken(
  client,
  { userId, tokenHash, expiresAt, requestedIp = null }
) {
  const { rows } = await client.query(
    `INSERT INTO password_reset_tokens (user_id, token_hash, expires_at, requested_ip)
     VALUES ($1, $2, $3, $4)
     RETURNING *`,
    [userId, tokenHash, expiresAt, requestedIp]
  );
  return rows[0];
}

export async function findPasswordResetTokenByHash(tokenHash) {
  const { rows } = await pool.query(
    `SELECT * FROM password_reset_tokens WHERE token_hash = $1`,
    [tokenHash]
  );
  return rows[0] ?? null;
}

export async function consumePasswordResetToken(client, tokenId) {
  await client.query(
    `UPDATE password_reset_tokens SET consumed_at = now() WHERE id = $1`,
    [tokenId]
  );
}

// --- two-factor (email OTP) ------------------------------------------------

// A fresh challenge supersedes any earlier pending one for the same
// purpose, same reasoning as the token invalidation above - only the
// most recently sent code should be acceptable.
export async function invalidatePendingTwoFactorChallenges(
  client,
  { userId, purpose }
) {
  await client.query(
    `UPDATE two_factor_challenges
     SET consumed_at = now()
     WHERE user_id = $1 AND purpose = $2 AND consumed_at IS NULL`,
    [userId, purpose]
  );
}

export async function createTwoFactorChallenge(
  client,
  { userId, codeHash, purpose, expiresAt }
) {
  const { rows } = await client.query(
    `INSERT INTO two_factor_challenges (user_id, code_hash, purpose, expires_at)
     VALUES ($1, $2, $3, $4)
     RETURNING *`,
    [userId, codeHash, purpose, expiresAt]
  );
  return rows[0];
}

export async function findTwoFactorChallengeById(id) {
  const { rows } = await pool.query(
    `SELECT * FROM two_factor_challenges WHERE id = $1`,
    [id]
  );
  return rows[0] ?? null;
}

export async function incrementTwoFactorAttempts(client, id) {
  const { rows } = await client.query(
    `UPDATE two_factor_challenges SET attempts = attempts + 1 WHERE id = $1 RETURNING *`,
    [id]
  );
  return rows[0] ?? null;
}

export async function consumeTwoFactorChallenge(client, id) {
  await client.query(
    `UPDATE two_factor_challenges SET consumed_at = now() WHERE id = $1`,
    [id]
  );
}

// --- refresh tokens -----------------------------------------------------

export async function insertRefreshToken(
  client,
  { userId, tokenHash, expiresAt, userAgent = null, ip = null }
) {
  const { rows } = await client.query(
    `INSERT INTO refresh_tokens (user_id, token_hash, expires_at, user_agent, ip)
     VALUES ($1, $2, $3, $4, $5)
     RETURNING *`,
    [userId, tokenHash, expiresAt, userAgent, ip]
  );
  return rows[0];
}

export async function findActiveRefreshTokenByHash(tokenHash) {
  const { rows } = await pool.query(
    `SELECT * FROM refresh_tokens WHERE token_hash = $1`,
    [tokenHash]
  );
  return rows[0] ?? null;
}

export async function rotateRefreshToken(
  client,
  { oldTokenId, newTokenId }
) {
  await client.query(
    `UPDATE refresh_tokens
     SET revoked_at = now(), replaced_by = $2
     WHERE id = $1`,
    [oldTokenId, newTokenId]
  );
}

export async function revokeRefreshToken(client, tokenId) {
  await client.query(
    `UPDATE refresh_tokens SET revoked_at = now() WHERE id = $1 AND revoked_at IS NULL`,
    [tokenId]
  );
}

// Used on reuse-detection: a revoked/rotated token being presented again
// means it was likely stolen - kill every active session for the user.
export async function revokeAllRefreshTokensForUser(client, userId) {
  await client.query(
    `UPDATE refresh_tokens SET revoked_at = now() WHERE user_id = $1 AND revoked_at IS NULL`,
    [userId]
  );
}
