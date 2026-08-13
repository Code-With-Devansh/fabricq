import { pool } from "../config/db.js";

// --- accounts ---------------------------------------------------------

export async function createAccount(client, { name }) {
  const { rows } = await client.query(
    `INSERT INTO accounts (name) VALUES ($1) RETURNING *`,
    [name]
  );
  return rows[0];
}

// --- users --------------------------------------------------------------

export async function createUser(
  client,
  { accountId, email, passwordHash, role = "owner" }
) {
  const { rows } = await client.query(
    `INSERT INTO users (account_id, email, password_hash, role)
     VALUES ($1, $2, $3, $4)
     RETURNING id, account_id, email, role, created_at`,
    [accountId, email, passwordHash, role]
  );
  return rows[0];
}

export async function findUserByEmail(email) {
  const { rows } = await pool.query(
    `SELECT id, account_id, email, password_hash, role, created_at
     FROM users
     WHERE email = $1`,
    [email]
  );
  return rows[0] ?? null;
}

export async function findUserById(userId) {
  const { rows } = await pool.query(
    `SELECT id, account_id, email, role, created_at
     FROM users
     WHERE id = $1`,
    [userId]
  );
  return rows[0] ?? null;
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
