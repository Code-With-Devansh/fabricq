import redis from "../config/redis.js";
import logger from "../config/logger/index.js";

// Write-behind path for job_executions row detail (response body, status,
// timing, redirects). Scheduling-critical fields (attempts/next_run/
// enabled on http_jobs) are NEVER routed through this - they stay on the
// synchronous Postgres path in worker.js/recovery.js, driven off the
// in-memory result object. Only the heavy, non-authoritative-for-scheduling
// execution_id row is deferred here.
export const STREAM_KEY = "fabricq:execution-results";
export const GROUP_NAME = "execution-merger";

// Fast, synchronous, cheap: worker/recovery HSET this right after deciding
// an execution's outcome, BEFORE handing the detail off to the stream.
// This is what recovery.js checks instead of reading job_executions from
// Postgres, so recovery never races the merger's batch flush.
//
// TTL is per-field (HEXPIRE, Redis 7.4+/8), not per-key. A key-level EXPIRE
// here would get pushed forward by every write to any execution's field
// under continuous load, so the hash would never actually expire - the
// same unbounded-growth problem this key exists to avoid. Per-field TTL
// means each executionId's entry expires on its own clock regardless of
// what else is being written to the hash.
//
// Every write (including the non-final "running" write) gets an HEXPIRE so
// nothing is ever unbounded even transiently - e.g. a worker crashing
// between the "running" write and the final write still has a bounded
// entry. `final` writes just mark intent for callers; both paths currently
// carry the same TTL.
const STATUS_HASH_KEY = "fabricq:execution:status";
const STATUS_TTL_SECONDS = 60 * 60;

export async function recordExecutionStatus(executionId, status, { final = false } = {}) {
  try {
    await redis
      .multi()
      .hset(STATUS_HASH_KEY, executionId, status)
      .hexpire(STATUS_HASH_KEY, STATUS_TTL_SECONDS, "FIELDS", 1, executionId)
      .exec();
  } catch (err) {
    logger.warn({ err, executionId, status, final }, "[execution-results] failed to record fast-path status");
  }
}

export async function getExecutionStatus(executionId) {
  try {
    return await redis.hget(STATUS_HASH_KEY, executionId);
  } catch (err) {
    logger.warn({ err, executionId }, "[execution-results] failed to read fast-path status");
    return null;
  }
}

export async function clearExecutionStatus(executionId) {
  try {
    await redis.hdel(STATUS_HASH_KEY, executionId);
  } catch (err) {
    logger.warn({ err, executionId }, "[execution-results] failed to clear fast-path status");
  }
}

// Ensure the consumer group exists. Called by the merger on startup; safe
// to call repeatedly (BUSYGROUP is swallowed). MKSTREAM so the group
// creation also creates the stream if this is a totally fresh deploy.
export async function ensureConsumerGroup() {
  try {
    await redis.xgroup("CREATE", STREAM_KEY, GROUP_NAME, "$", "MKSTREAM");
  } catch (err) {
    if (!String(err.message).includes("BUSYGROUP")) throw err;
  }
}

// One XADD per lifecycle event (queued->running, running->success/failed).
// `*` lets Redis assign the entry ID. Kept as a flat field map (not a
// single JSON blob) so XADD/XREAD stay cheap and debuggable via redis-cli.
export async function pushExecutionEvent(event) {
  const { executionId, createdAt = null, type, payload } = event;
  await redis.xadd(
    STREAM_KEY,
    "*",
    "execution_id",
    executionId,
    // job_executions' created_at, carried along purely so the merger can
    // include it in its batch UPDATE and let Postgres prune to a single
    // partition instead of probing all of them (see migration 024). Not
    // this stream entry's own timestamp - that's the "ts" field below.
    "created_at",
    createdAt === null ? "" : new Date(createdAt).toISOString(),
    "type", // "running" | "completed"
    type,
    "payload",
    JSON.stringify(payload),
    "ts",
    Date.now().toString(),
  );
}