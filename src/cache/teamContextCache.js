import redis from "../config/redis.js";
import logger from "../config/logger/index.js";

/**
 * Caches the resolved (membership + role + permissions) context for a
 * (teamId, userId) pair, so loadTeamContext doesn't hit Postgres on every
 * authenticated dashboard request - this is the JWT-path equivalent of
 * cache/apiKeyCache.js's prefix cache for the API-key path.
 *
 * Cache shape per (teamId, userId): JSON
 *   { membershipId, userId, roleId, roleName, roleIsSystem, permissionKeys }
 * or the literal JSON `null` for a cached "no membership" result (see
 * NOT_FOUND_SENTINEL below).
 *
 * TTL is short (same 45s as the API key cache) so it self-heals even if
 * an invalidation call is ever missed. The explicit invalidate() calls
 * below (role change, member removal, new membership) are what make
 * those changes feel instant in the common case - the TTL is just a
 * safety net.
 */

const TEAM_CONTEXT_KEY = (teamId, userId) => `teamctx:${teamId}:${userId}`;
const CACHE_TTL_SECONDS = 45;

// JSON.stringify(null) is the string "null" - distinguishable from
// Redis returning null for "key doesn't exist" (a real cache miss).
// This lets us negative-cache "this user has no membership on this
// team" the same as a positive result, instead of hitting Postgres on
// every request for a team a user was never on (or got removed from).
const NOT_FOUND_SENTINEL = "null";

/**
 * Returns:
 *   - the cached context object on a cached hit
 *   - null on a cached "not found" (negative cache hit)
 *   - undefined on a true cache miss (caller should query Postgres)
 */
export async function getCachedTeamContext(teamId, userId) {
  try {
    const raw = await redis.get(TEAM_CONTEXT_KEY(teamId, userId));
    if (raw === null) return undefined;
    if (raw === NOT_FOUND_SENTINEL) return null;
    return JSON.parse(raw);
  } catch (err) {
    // Cache errors should never block authorization - treat as a miss.
    logger.warn(
      { err, teamId, userId },
      "[teamContextCache] read failed, falling back to db"
    );
    return undefined;
  }
}

/**
 * Pass a context object to cache a hit, or null to negative-cache "this
 * user has no membership on this team" (e.g. wrong team_id, not yet
 * invited, just removed).
 */
export async function setCachedTeamContext(teamId, userId, ctx) {
  try {
    await redis.set(
      TEAM_CONTEXT_KEY(teamId, userId),
      ctx === null ? NOT_FOUND_SENTINEL : JSON.stringify(ctx),
      "EX",
      CACHE_TTL_SECONDS
    );
  } catch (err) {
    logger.warn({ err, teamId, userId }, "[teamContextCache] write failed");
  }
}

/**
 * Explicit invalidation on role change / membership removal / new
 * membership, so a demoted/removed member's stale permissions - or a
 * newly-added member's cached "not found" - don't linger for up to
 * CACHE_TTL_SECONDS. Hook this into team.service.js wherever a
 * membership is created, its role_id changes, or it's deleted.
 */
export async function invalidateTeamContextCache(teamId, userId) {
  try {
    await redis.del(TEAM_CONTEXT_KEY(teamId, userId));
  } catch (err) {
    logger.warn({ err, teamId, userId }, "[teamContextCache] invalidate failed");
  }
}
