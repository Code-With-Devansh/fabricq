import { pool } from "../config/db.js";
import {
  getCachedCandidates,
  setCachedCandidates,
  invalidateApiKeyCache,
} from "../cache/apiKeyCache.js";

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
  // A prior miss/negative-cache entry for this prefix (e.g. a probing
  // request against an unused prefix) could otherwise shadow the new key
  // until the TTL expires.
  await invalidateApiKeyCache(prefix);
  return rows[0];
}

// Candidates by prefix - narrows to (usually) a single row before the
// caller does the real hash comparison. Includes revoked/expired keys so
// the caller can distinguish "wrong key" from "revoked key" if needed.
//
// Cached in Redis for CACHE_TTL_SECONDS keyed by prefix, since this is
// the hot path hit by every authenticated /v1/* request. Revoke/rotate
// explicitly bust the cache below, so the TTL is a safety net rather
// than the primary invalidation mechanism.
export async function findByPrefix(prefix) {
  const cached = await getCachedCandidates(prefix);
  if (cached !== null) return cached;

  const { rows } = await pool.query(
    `SELECT * FROM api_keys WHERE key_prefix = $1`,
    [prefix]
  );
  // Cache the miss too (empty array) - a spray of invalid-prefix requests
  // shouldn't each fall through to Postgres.
  await setCachedCandidates(prefix, rows);
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
     RETURNING id, key_prefix`,
    [keyId, teamId]
  );
  const revoked = rows[0] ?? null;
  if (revoked) {
    // Explicit invalidation so revocation is effective immediately,
    // instead of the caller staying authenticated until the TTL expires.
    await invalidateApiKeyCache(revoked.key_prefix);
  }
  return revoked;
}

// Fire-and-forget from the caller's perspective - failures here should
// never block or fail the request the key is authenticating. Kept for
// callers that need a synchronous, immediate write (e.g. tests, admin
// tooling); the hot request path uses markKeyUsed + flushPendingLastUsed
// instead (see cache/apiKeyCache.js and the last-used flush cron).
export async function touchLastUsed(keyId) {
  await pool.query(`UPDATE api_keys SET last_used_at = now() WHERE id = $1`, [
    keyId,
  ]);
}

// Batched equivalent of touchLastUsed - one statement for many keys,
// each set to its own most-recent timestamp via UNNEST + a value list.
export async function touchLastUsedBatch(entries) {
  // entries: Array<[keyId, timestampMs]>
  if (entries.length === 0) return;
  const ids = entries.map(([keyId]) => keyId);
  const timestamps = entries.map(([, ts]) => new Date(ts));
  await pool.query(
    `UPDATE api_keys AS k
     SET last_used_at = v.last_used_at
     FROM (
       SELECT UNNEST($1::uuid[]) AS id, UNNEST($2::timestamptz[]) AS last_used_at
     ) AS v
     WHERE k.id = v.id`,
    [ids, timestamps]
  );
}
