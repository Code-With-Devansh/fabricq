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
// Postgres, so recovery never races the merger's batch flush. TTL keeps
// it from growing unbounded - once the merger has actually flushed to
// Postgres (a few hundred ms later, worst case a few seconds under
// backlog), Postgres is authoritative again and this entry is disposable.
const STATUS_HASH_KEY = "fabricq:execution:status";
const STATUS_TTL_SECONDS = 60 * 60;

export async function recordExecutionStatus(executionId, status) {
  try {
    await redis
      .multi()
      .hset(STATUS_HASH_KEY, executionId, status)
      .expire(STATUS_HASH_KEY, STATUS_TTL_SECONDS)
      .exec();
  } catch (err) {
    logger.warn({ err, executionId, status }, "[execution-results] failed to record fast-path status");
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