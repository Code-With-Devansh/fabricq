import crypto from "crypto";
import redis from "../config/redis.js";
import { pool } from "../config/db.js";
import logger from "../config/logger/index.js";
import { EXECUTION_QUEUE_KEY } from "../scheduler/scheduler.js";
import { HEARTBEAT_SET_KEY, PROCESSING_INDEX_KEY } from "../worker/worker.js";
import {
  completeExecution,
  getExecutionById,
} from "../repositories/execution.repository.js";
import { finalizeJobRun } from "../repositories/httpJob.repository.js";

// Same value the worker uses to decide an execution has gone dark (worker.js
// heartbeats every 10s). Give it a couple of missed beats of slack before
// we call it abandoned.
const HEARTBEAT_TIMEOUT_MS = 30_000;
const PROCESSING_KEY_PATTERN = `${EXECUTION_QUEUE_KEY}:processing:*`;
const LOCK_PREFIX = "recovery:lock:";
const LOCK_TTL_MS = 15_000;

const unlockScript = `
  if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
  else
    return 0
  end
`;

async function acquireLock(executionId) {
  const token = crypto.randomUUID();
  const ok = await redis.set(
    `${LOCK_PREFIX}${executionId}`,
    token,
    "PX",
    LOCK_TTL_MS,
    "NX",
  );
  return ok === "OK" ? token : null;
}

async function releaseLock(executionId, token) {
  await redis.eval(unlockScript, 1, `${LOCK_PREFIX}${executionId}`, token);
}

async function getStaleExecutionIds() {
  const cutoff = Date.now() - HEARTBEAT_TIMEOUT_MS;
  return redis.zrangebyscore(HEARTBEAT_SET_KEY, 0, cutoff);
}

// O(1) point lookup against the global index the worker maintains alongside
// its per-worker processing list. This is the hot path - no SCAN involved.
// listKey is deterministic from workerId, so we reconstruct it rather than
// storing it; job is reconstructed by re-parsing raw for the same reason.
async function lookupProcessingEntry(executionId) {
  const indexed = await redis.hget(PROCESSING_INDEX_KEY, executionId);
  if (!indexed) return null;
  try {
    const { workerId, raw } = JSON.parse(indexed);
    return {
      listKey: `${EXECUTION_QUEUE_KEY}:processing:${workerId}`,
      raw,
      job: JSON.parse(raw),
    };
  } catch (err) {
    logger.error(
      { err, executionId },
      "[recovery] unparseable processing index entry",
    );
    return null;
  }
}

// Cold-path fallback for the narrow window where a worker died between its
// BLMOVE (job safely in its processing list) and the follow-up HSET into
// PROCESSING_INDEX_KEY. Only runs for ids the index didn't know about, which
// should be rare, so the SCAN cost here is acceptable.
async function findInProcessingListsFallback(executionId) {
  let cursor = "0";
  do {
    const [nextCursor, keys] = await redis.scan(
      cursor,
      "MATCH",
      PROCESSING_KEY_PATTERN,
      "COUNT",
      100,
    );
    cursor = nextCursor;

    for (const listKey of keys) {
      const entries = await redis.lrange(listKey, 0, -1);
      for (const raw of entries) {
        let job;
        try {
          job = JSON.parse(raw);
        } catch {
          continue;
        }
        if (job.execution_id === executionId) {
          return { listKey, raw, job };
        }
      }
    }
  } while (cursor !== "0");

  return null;
}

// Requeues a non-recurring job for another attempt after backoff. Mirrors
// the retry branch in worker.js's handleExecution so a crash and a clean
// failure end up in the same place.
async function rescheduleForRetry(client, jobId) {
  await client.query(
    `UPDATE http_jobs
     SET status = 'PENDING',
         attempts = attempts + 1,
         next_run = now() + (backoff_seconds || ' seconds')::interval,
         updated_at = now()
     WHERE job_id = $1`,
    [jobId],
  );
}

// Handles a single stale execution: decide whether the worker actually
// finished before dying (nothing to do but tidy Redis) or genuinely
// abandoned it mid-flight (fail the execution, retry/finalize the job).
async function recoverExecution(executionId) {
  const token = await acquireLock(executionId);
  if (!token) {
    logger.debug(
      { executionId },
      "[recovery] lock held by another recovery run, skipping",
    );
    return;
  }

  try {
    // Re-check freshness now that we hold the lock - the worker may have
    // heartbeated again between our scan and acquiring the lock.
    const score = await redis.zscore(HEARTBEAT_SET_KEY, executionId);
    if (score !== null && Number(score) > Date.now() - HEARTBEAT_TIMEOUT_MS) {
      logger.debug(
        { executionId },
        "[recovery] heartbeat is fresh again, worker is alive",
      );
      return;
    }

    let entry = await lookupProcessingEntry(executionId);
    if (!entry) {
      // Not in the index - either genuinely gone (worker finished and
      // cleaned up right as we checked) or it hit the narrow BLMOVE->HSET
      // crash window. Fall back to the slow scan before giving up.
      entry = await findInProcessingListsFallback(executionId);
    }
    if (!entry) {
      logger.warn(
        { executionId },
        "[recovery] stale heartbeat with no matching processing entry, clearing heartbeat",
      );
      await redis.zrem(HEARTBEAT_SET_KEY, executionId);
      return;
    }

    const execution = await getExecutionById(executionId);
    if (!execution) {
      logger.error(
        { executionId },
        "[recovery] execution not found in postgres, dropping orphaned entry",
      );
      await redis
        .multi()
        .lrem(entry.listKey, 1, entry.raw)
        .zrem(HEARTBEAT_SET_KEY, executionId)
        .hdel(PROCESSING_INDEX_KEY, executionId)
        .exec();
      return;
    }

    if (execution.status === "success" || execution.status === "failed") {
      // Worker completed the HTTP call and updated Postgres, but crashed
      // before it could LREM the processing list / clear the heartbeat.
      // The execution result is already correct - just tidy up Redis.
      logger.info(
        { executionId, status: execution.status },
        "[recovery] execution already finished, cleaning up Redis only",
      );
      await redis
        .multi()
        .lrem(entry.listKey, 1, entry.raw)
        .zrem(HEARTBEAT_SET_KEY, executionId)
        .hdel(PROCESSING_INDEX_KEY, executionId)
        .exec();
      return;
    }

    // Still "running"/"queued" with a dead heartbeat: genuinely abandoned.
    const job = entry.job;
    const client = await pool.connect();
    try {
      await client.query("BEGIN");

      await completeExecution(
        client,
        executionId,
        {
          success: false,
          error: "Worker crashed executing this job.",
        },
        null,
      );

      const isRecurring = job.schedule_type === "CRON";
      const exhaustedRetries = job.attempts >= job.max_attempts;

      if (!isRecurring && !exhaustedRetries) {
        await rescheduleForRetry(client, job.job_id);
      } else {
        await finalizeJobRun(client, job.job_id, {
          success: false,
          isRecurring,
        });
      }

      await client.query("COMMIT");
    } catch (err) {
      await client.query("ROLLBACK");
      logger.error(
        { err, executionId, jobId: job.job_id },
        "[recovery] failed to recover abandoned execution, leaving in place for next cycle",
      );
      return;
    } finally {
      client.release();
    }

    await redis
      .multi()
      .lrem(entry.listKey, 1, entry.raw)
      .zrem(HEARTBEAT_SET_KEY, executionId)
      .hdel(PROCESSING_INDEX_KEY, executionId)
      .exec();

    logger.warn(
      { executionId, jobId: job.job_id, listKey: entry.listKey },
      "[recovery] recovered abandoned execution",
    );
  } finally {
    await releaseLock(executionId, token);
  }
}

export async function runRecoveryCycle() {
  const staleIds = await getStaleExecutionIds();
  if (staleIds.length === 0) return;

  logger.info({ count: staleIds.length }, "[recovery] found stale executions");

  for (const executionId of staleIds) {
    try {
      await recoverExecution(executionId);
    } catch (err) {
      logger.error(
        { err, executionId },
        "[recovery] unexpected error recovering execution",
      );
    }
  }
}
