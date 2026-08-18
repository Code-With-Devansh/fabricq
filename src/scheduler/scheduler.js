import { CronExpressionParser } from "cron-parser";
import { pool } from "../config/db.js";
import logger from "../config/logger/index.js";
import { claimDueJobs, markJobScheduledBatch, releaseJobClaims } from "../repositories/httpJob.repository.js";
import { createExecutionBatch } from "../repositories/execution.repository.js";
import { createOutboxEntryBatch } from "../repositories/outbox.repository.js";
import { publishOutboxEntryNow } from "../outbox/outbox.js";
import redis from "../config/redis.js";

const EXECUTION_QUEUE_KEY = "fabricq:executions";
// Single consumer group shared by every worker process - each worker is
// its own consumer within it (see worker.js's WORKER_ID). Recovery also
// reads/claims from this same group rather than maintaining a parallel
// index, so "who owns this execution" only ever lives in one place: the
// group's pending-entries list (PEL).
export const EXECUTION_QUEUE_GROUP = "fabricq-workers";

// Ensure the consumer group exists. Safe to call repeatedly (BUSYGROUP is
// swallowed) - both worker.js and recovery.js call this on startup, same
// pattern as streams/executionResults.js's ensureConsumerGroup. MKSTREAM
// so this also creates the stream itself on a totally fresh deploy.
export async function ensureExecutionQueueGroup() {
  try {
    await redis.xgroup("CREATE", EXECUTION_QUEUE_KEY, EXECUTION_QUEUE_GROUP, "$", "MKSTREAM");
  } catch (err) {
    if (!String(err.message).includes("BUSYGROUP")) throw err;
  }
}

function computeNextRunEpoch(job) {
  const expr = CronExpressionParser.parse(job.cron_expression, {
    currentDate: new Date(job.next_run),
  });
  return Math.floor(expr.next().getTime() / 1000);
}

export async function pollAndScheduleDueJobs() {
  const client = await pool.connect();
  let claimed = [];
  const toEnqueue = [];

  try {
    await client.query("BEGIN");

    claimed = await claimDueJobs(client);

    if (claimed.length === 0) {
      await client.query("COMMIT");
      client.release();
      return 0;
    }

    logger.info({ count: claimed.length }, "[scheduler] claimed due jobs");

    // Claiming, scheduling, AND the outbox write all happen in the SAME
    // transaction now. That matters specifically for ONCE jobs:
    // markJobScheduledBatch clears next_run as part of this transaction,
    // so if the process crashes anywhere before COMMIT, the whole batch
    // rolls back and every job goes right back to being a normal due job
    // on the next poll - no window where it's claimed (locked_at set) but
    // next_run is still sitting in the past.
    //
    // Unlike the old per-job savepoint loop, this does the scheduling and
    // outbox writes as THREE set-based statements covering the whole
    // batch, not up to 5xN sequential round-trips. That's what keeps the
    // claim's row locks (FOR UPDATE SKIP LOCKED, held since claimDueJobs)
    // from being held for a duration that scales with batch size - a
    // 100-job batch now costs roughly the same number of round-trips as
    // a 10-job one.
    //
    // Because a single set-based INSERT can't skip just the one bad row
    // the way a per-job savepoint could, validation happens BEFORE any
    // write: computeNextRunEpoch is called for every claimed job up
    // front (pure in-memory, no DB round-trip), and jobs that fail it
    // (e.g. malformed cron_expression) are excluded from the batch
    // entirely and have their claim released via releaseJobClaims so
    // they don't get stuck locked forever - they're simply due again,
    // and will fail loudly in the same way on the next poll until fixed.
    const validJobs = [];
    const invalidJobIds = [];

    for (const job of claimed) {
      try {
        const isRecurring = job.schedule_type === "CRON";
        const nextRun = isRecurring ? computeNextRunEpoch(job) : null;
        validJobs.push({ job, isRecurring, nextRun });
      } catch (err) {
        invalidJobIds.push(job.job_id);
        logger.error(
          { err, jobId: job.job_id },
          "[scheduler] failed to compute next run, releasing claim"
        );
      }
    }

    if (invalidJobIds.length > 0) {
      await releaseJobClaims(client, invalidJobIds);
    }

    if (validJobs.length > 0) {
      const attempt = 1;

      const executionRows = await createExecutionBatch(
        client,
        validJobs.map(({ job }) => ({
          jobId: job.job_id,
          attempt,
          scheduledFor: Math.floor(new Date(job.next_run).getTime() / 1000),
        }))
      );
      // Match back by job_id, not array position - UNNEST-based
      // INSERT...SELECT doesn't guarantee it preserves input order.
      const executionIdByJobId = new Map(executionRows.map((r) => [r.job_id, r.execution_id]));

      await markJobScheduledBatch(
        client,
        validJobs.map(({ job, nextRun, isRecurring }) => ({
          jobId: job.job_id,
          nextRun,
          isRecurring,
        }))
      );

      const outboxEntries = validJobs.map(({ job }) => {
        const executionId = executionIdByJobId.get(job.job_id);
        return {
          executionId,
          queueKey: EXECUTION_QUEUE_KEY,
          payload: { ...job, execution_id: executionId, attempt },
        };
      });
      await createOutboxEntryBatch(client, outboxEntries);

      for (const { job } of validJobs) {
        toEnqueue.push({ ...job, execution_id: executionIdByJobId.get(job.job_id) });
      }
    }

    await client.query("COMMIT");
  } catch (err) {
    await client.query("ROLLBACK");
    logger.error({ err }, "[scheduler] failed to claim/schedule due jobs, batch rolled back");
    client.release();
    return 0;
  }

  client.release();

  // The transaction already committed, so every job in toEnqueue is
  // durably scheduled in Postgres AND has a matching execution_outbox
  // row with published_at still NULL. This loop is just the fast path:
  // publish to Redis immediately so executions don't sit around waiting
  // for the relay's next tick. If this fails, or the process crashes
  // right here, nothing is lost - the outbox row is still there, and
  // the outbox relay (src/outbox) will publish it on its next sweep.
  for (const job of toEnqueue) {
    try {
      const published = await publishOutboxEntryNow({
        executionId: job.execution_id,
        queueKey: EXECUTION_QUEUE_KEY,
        payload: job,
      });
      logger.info(
        { jobId: job.job_id, executionId: job.execution_id, published },
        published ? "[scheduler] execution queued" : "[scheduler] execution queued, deferred to outbox relay"
      );
    } catch (err) {
      logger.error(
        { err, jobId: job.job_id, executionId: job.execution_id },
        "[scheduler] fast-path publish failed, execution left for outbox relay"
      );
    }
  }

  return claimed.length;
}

export { EXECUTION_QUEUE_KEY };