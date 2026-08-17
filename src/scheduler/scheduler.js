import { CronExpressionParser } from "cron-parser";
import { pool } from "../config/db.js";
import logger from "../config/logger/index.js";
import { claimDueJobs, markJobScheduled } from "../repositories/httpJob.repository.js";
import { createExecution } from "../repositories/execution.repository.js";
import { createOutboxEntry } from "../repositories/outbox.repository.js";
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
      return;
    }

    logger.info({ count: claimed.length }, "[scheduler] claimed due jobs");

    // Claiming, scheduling, AND the outbox write all happen in the SAME
    // transaction now. That matters specifically for ONCE jobs:
    // markJobScheduled clears next_run as part of this transaction, so if
    // the process crashes anywhere before COMMIT, the whole batch rolls
    // back and the job goes right back to being a normal due job on the
    // next poll - no window where it's claimed (locked_at set) but
    // next_run is still sitting in the past, which was the root cause of
    // the double-schedule race this replaces.
    //
    // The execution_outbox row (see migration 016) is written in the
    // same savepoint as the execution row itself, so the two can never
    // land separately - either both commit or both roll back. That's
    // what closes the "committed but never LPUSHed" gap: the Redis push
    // is no longer the thing that has to survive a crash, the outbox row
    // is, and it's already inside the same transaction as everything
    // else.
    //
    // Each job gets its own SAVEPOINT so one bad job (e.g. malformed
    // cron_expression) can't roll back the entire batch - it rolls back
    // only that job's claim, leaving it due again for the next poll,
    // while every other job in the batch still commits normally.
    for (const job of claimed) {
      const savepoint = `job_${job.job_id.replace(/-/g, "_")}`;
      try {
        await client.query(`SAVEPOINT "${savepoint}"`);
        const execution = await scheduleOne(client, job);
        await client.query(`RELEASE SAVEPOINT "${savepoint}"`);
        toEnqueue.push({ ...job, execution_id: execution.execution_id });
      } catch (err) {
        await client.query(`ROLLBACK TO SAVEPOINT "${savepoint}"`);
        logger.error(
          { err, jobId: job.job_id },
          "[scheduler] failed to schedule job, skipping"
        );
      }
    }

    await client.query("COMMIT");
  } catch (err) {
    await client.query("ROLLBACK");
    logger.error({ err }, "[scheduler] failed to claim/schedule due jobs, batch rolled back");
    client.release();
    return;
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
}

async function scheduleOne(client, job) {
  const isRecurring = job.schedule_type === "CRON";
  const attempt = 1;
  const scheduledForEpoch = Math.floor(new Date(job.next_run).getTime() / 1000);

  const execution = await createExecution(client, {
    jobId: job.job_id,
    attempt,
    scheduledFor: scheduledForEpoch,
  });

  const nextRun = isRecurring ? computeNextRunEpoch(job) : null;
  await markJobScheduled(client, job.job_id, { nextRun, isRecurring });

  await createOutboxEntry(client, {
    executionId: execution.execution_id,
    queueKey: EXECUTION_QUEUE_KEY,
    payload: { ...job, execution_id: execution.execution_id, attempt },
  });

  return execution;
}

export { EXECUTION_QUEUE_KEY };