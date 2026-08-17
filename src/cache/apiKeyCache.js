import redis from "../config/redis.js";
import logger from "../config/logger/index.js";

/**
 * Caches API key candidate rows by prefix so authenticateApiKey doesn't hit
 * Postgres on every request. A prefix can map to more than one row (rare,
 * but possible), so we cache the whole candidate array per prefix.
 *
 * Cache shape per prefix: JSON array of
 *   { id, team_id, key_hash, scopes, revoked_at, expires_at }
 *
 * TTL is short (default 45s) so a revoke/rotate that *isn't* explicitly
 * invalidated (e.g. a bug, or a manual DB update) self-heals quickly.
 * The explicit invalidate() call below is what makes revokes feel instant
 * in the common case - the TTL is just a safety net.
 */

const PREFIX_KEY = (prefix) => `apikey:prefix:${prefix}`;
const CACHE_TTL_SECONDS = 45;

function serializeRow(row) {
  return {
    id: row.id,
    team_id: row.team_id,
    key_hash: row.key_hash,
    scopes: row.scopes,
    revoked_at: row.revoked_at,
    expires_at: row.expires_at,
  };
}

function deserializeRow(row) {
  return {
    ...row,
    revoked_at: row.revoked_at ? new Date(row.revoked_at) : null,
    expires_at: row.expires_at ? new Date(row.expires_at) : null,
  };
}

/**
 * Returns cached candidates for a prefix, or null on a cache miss
 * (including "we don't know" - the caller should fall back to Postgres).
 */
export async function getCachedCandidates(prefix) {
  try {
    const raw = await redis.get(PREFIX_KEY(prefix));
    if (raw === null) return null;
    const parsed = JSON.parse(raw);
    return parsed.map(deserializeRow);
  } catch (err) {
    // Cache errors should never block auth - treat as a miss.
    logger.warn({ err, prefix }, "[apiKeyCache] read failed, falling back to db");
    return null;
  }
}

export async function setCachedCandidates(prefix, rows) {
  try {
    const payload = JSON.stringify(rows.map(serializeRow));
    await redis.set(PREFIX_KEY(prefix), payload, "EX", CACHE_TTL_SECONDS);
  } catch (err) {
    logger.warn({ err, prefix }, "[apiKeyCache] write failed");
  }
}

/**
 * Explicit invalidation on revoke/rotate/create - hook this into the
 * repository layer wherever key_prefix rows change, so revocation is
 * effective immediately instead of waiting out the TTL.
 */
export async function invalidateApiKeyCache(prefix) {
  try {
    await redis.del(PREFIX_KEY(prefix));
  } catch (err) {
    logger.warn({ err, prefix }, "[apiKeyCache] invalidate failed");
  }
}

// ---------------------------------------------------------------------
// Batched last-used tracking
//
// Instead of `UPDATE api_keys SET last_used_at = now()` on every request,
// each request writes a cheap Redis SET (in-memory, O(1)) and a cron
// flushes all pending keyIds to Postgres in one batched statement.
// ---------------------------------------------------------------------

const LAST_USED_ZSET = "apikey:last_used:pending";

export async function markKeyUsed(keyId) {
  try {
    await redis.zadd(LAST_USED_ZSET, Date.now(), keyId);
  } catch (err) {
    // Best-effort, same as the write it replaces - never block the request.
    logger.warn({ err, keyId }, "[apiKeyCache] markKeyUsed failed");
  }
}

/**
 * Atomically pops everything currently pending so concurrent flush runs
 * (or a flush racing new markKeyUsed calls) don't double-process or drop
 * entries. Returns a Map<keyId, timestampMs>.
 */
export async function drainPendingLastUsed() {
  const script = `
    local entries = redis.call('ZRANGE', KEYS[1], 0, -1, 'WITHSCORES')
    if #entries > 0 then
      redis.call('DEL', KEYS[1])
    end
    return entries
  `;
  try {
    const entries = await redis.eval(script, 1, LAST_USED_ZSET);
    const result = new Map();
    for (let i = 0; i < entries.length; i += 2) {
      result.set(entries[i], Number(entries[i + 1]));
    }
    return result;
  } catch (err) {
    logger.error({ err }, "[apiKeyCache] drainPendingLastUsed failed");
    return new Map();
  }
}
