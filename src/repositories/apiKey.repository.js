import { pool } from "../config/db.js";

export async function createApiKey(
  client,
  { teamId, name, prefix, hash, scopes, expiresAt, createdBy }
) {
  const { rows } = await client.query(
    `INSERT INTO api_keys (team_id, name, key_prefix, key_hash, scopes, expires_at, created_by)
     VALUES ($1, $2, $3, $4, $5, $6, $7)
     RETURNING id, team_id, name, key_prefix, scopes, expires_at, revoked_at,
               created_at, created_by, last_used_at`,
    [teamId, name, prefix, hash, scopes, expiresAt, createdBy]
  );
  return rows[0];
}

// Candidates by prefix - narrows to (usually) a single row before the
// caller does the real hash comparison. Includes revoked/expired keys so
// the caller can distinguish "wrong key" from "revoked key" if needed.
export async function findByPrefix(prefix) {
  const { rows } = await pool.query(
    `SELECT * FROM api_keys WHERE key_prefix = $1`,
    [prefix]
  );
  return rows;
}

export async function listForTeam(teamId) {
  const { rows } = await pool.query(
    `SELECT id, team_id, name, key_prefix, scopes, expires_at, revoked_at,
            created_at, created_by, last_used_at
     FROM api_keys
     WHERE team_id = $1
     ORDER BY created_at DESC`,
    [teamId]
  );
  return rows;
}

export async function findByIdForTeam(teamId, keyId) {
  const { rows } = await pool.query(
    `SELECT id, team_id, name, key_prefix, scopes, expires_at, revoked_at,
            created_at, created_by, last_used_at
     FROM api_keys
     WHERE team_id = $1 AND id = $2`,
    [teamId, keyId]
  );
  return rows[0] ?? null;
}

export async function revokeApiKey(client, { teamId, keyId }) {
  const { rows } = await client.query(
    `UPDATE api_keys
     SET revoked_at = now()
     WHERE id = $1 AND team_id = $2 AND revoked_at IS NULL
     RETURNING id`,
    [keyId, teamId]
  );
  return rows[0] ?? null;
}

// Fire-and-forget from the caller's perspective - failures here should
// never block or fail the request the key is authenticating.
export async function touchLastUsed(keyId) {
  await pool.query(`UPDATE api_keys SET last_used_at = now() WHERE id = $1`, [
    keyId,
  ]);
}
