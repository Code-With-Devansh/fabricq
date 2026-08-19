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

// System-wide ceiling on backfilled executions per job per poll, matching
// the CHECK constraint on http_jobs.max_catchup_per_poll (migration 023).
// A job's own max_catchup_per_poll can only ever tighten this, never
// exceed it.
const SYSTEM_MAX_CATCHUP_PER_POLL = 100;

// Global backpressure valve: total unacked (pending) entries in
// EXECUTION_QUEUE_KEY's consumer group, checked once at the start of each
// poll. Backfilled jobs especially can otherwise ask for hundreds of
// executions in a single poll; this bounds how much a single poll can add
// to Redis regardless of how many jobs (or how overdue) are claimed.
const MAX_QUEUE_DEPTH = Number(process.env.SCHEDULER_MAX_QUEUE_DEPTH) || 10_000;

// Live count of pending (un-XACK'd) entries in the execution queue's
// consumer group. Returns 0 (rather than throwing) if the group doesn't
// exist yet - ensureExecutionQueueGroup runs at worker/recovery startup,
// but the scheduler shouldn't hard-fail a poll just because it raced
// that on a completely fresh deploy.
async function getPendingQueueDepth() {
  try {
    const summary = await redis.xpending(EXECUTION_QUEUE_KEY, EXECUTION_QUEUE_GROUP);
    // ioredis XPENDING summary form: [count, minId, maxId, consumers] or
    // null when there are zero pending entries.
    return summary && summary[0] ? Number(summary[0]) : 0;
  } catch (err) {
    logger.warn({ err }, "[scheduler] failed to read queue depth, assuming 0");
    return 0;
  }
}

// Computes the set of executions a due job should produce this poll, plus
// its next next_run, per the job's backfill policy:
//
//   backfill_on_missed_run = false (default / "skip-ahead"): fire exactly
//   one execution for the currently-due tick (job.next_run itself - that's
//   why it was claimed), then advance next_run to the first tick strictly
//   AFTER now. This is the actual fix for the drift bug: the old code
//   called expr.next() exactly once regardless of how far behind next_run
//   was, so a job whose interval was shorter than the poll interval (or
//   that had been down for a while) only ever crept next_run forward by
//   one tick per poll and NEVER caught up. Looping .next() here until it
//   clears "now" is what stops the drift.
//
//   backfill_on_missed_run = true: create one execution per missed tick
//   from job.next_run up to (and including) the last tick <= now, capped
//   at min(job.max_catchup_per_poll ?? default, SYSTEM_MAX_CATCHUP_PER_POLL).
//   Each execution's scheduled_time is its REAL missed tick, not "now".
//   next_run advances to exactly (last included tick).next() - NOT synced
//   to wall-clock - so if the cap was hit and ticks remain between it and
//   now, next_run still correctly reads as "due" and the job is reclaimed
//   next poll to keep draining, in strict next_run order (see
//   claimDueJobs' ORDER BY next_run) rather than losing its place.
function computeJobSchedule(job, nowEpoch) {
  const isRecurring = job.schedule_type === "CRON";
  if (!isRecurring) {
    // ONCE jobs: unchanged, single execution, next_run cleared afterward.
    return {
      isRecurring: false,
      ticks: [Math.floor(new Date(job.next_run).getTime() / 1000)],
      nextRunEpoch: null,
    };
  }

  const expr = CronExpressionParser.parse(job.cron_expression, {
    currentDate: new Date(job.next_run),
  });
  const firstDueTick = Math.floor(new Date(job.next_run).getTime() / 1000);

  if (!job.backfill_on_missed_run) {
    let nextRunEpoch;
    do {
      nextRunEpoch = Math.floor(expr.next().getTime() / 1000);
    } while (nextRunEpoch <= nowEpoch);
    return { isRecurring: true, ticks: [firstDueTick], nextRunEpoch };
  }

  const cap = Math.min(job.max_catchup_per_poll ?? SYSTEM_MAX_CATCHUP_PER_POLL, SYSTEM_MAX_CATCHUP_PER_POLL);
  const ticks = [firstDueTick];
  let lastTick = firstDueTick;
  while (ticks.length < cap) {
    const tick = Math.floor(expr.next().getTime() / 1000);
    if (tick > nowEpoch) break;
    ticks.push(tick);
    lastTick = tick;
  }
  // Next tick after the last one we actually included - whether or not
  // it's still <= now. If it's still due, the job stays claimable and
  // resumes catching up from exactly where it left off on the next poll.
  const tail = CronExpressionParser.parse(job.cron_expression, {
    currentDate: new Date(lastTick * 1000),
  });
  const nextRunEpoch = Math.floor(tail.next().getTime() / 1000);

  return { isRecurring: true, ticks, nextRunEpoch };
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

    const nowEpoch = Math.floor(Date.now() / 1000);
    let remainingBudget = MAX_QUEUE_DEPTH - (await getPendingQueueDepth());
    if (remainingBudget < 0) remainingBudget = 0;

    // Claiming, scheduling, AND the outbox write all happen in the SAME
    // transaction now. That matters specifically for ONCE jobs:
    // markJobScheduledBatch clears next_run as part of this transaction,
    // so if the process crashes anywhere before COMMIT, the whole batch
    // rolls back and every job goes right back to being a normal due job
    // on the next poll - no window where it's claimed (locked_at set) but
    // next_run is still sitting in the past.
    //
    // Because a single set-based INSERT can't skip just the one bad row
    // the way a per-job savepoint could, validation happens BEFORE any
    // write: computeJobSchedule is called for every claimed job up front
    // (pure in-memory, no DB round-trip). Jobs are then processed in
    // claim order (ORDER BY next_run - most overdue first, see
    // claimDueJobs) against the remaining queue-depth budget: a job whose
    // ticks don't fit in what's left has its claim released untouched -
    // whole job, no partial catchup - and is deferred to the next poll,
    // same as every job after it in the batch. Jobs that fail schedule
    // computation entirely (e.g. malformed cron_expression) are excluded
    // the same way.
    const validJobs = [];
    const invalidJobIds = [];
    const budgetStarvedJobIds = [];
    let budgetExhausted = false;

    for (const job of claimed) {
      if (budgetExhausted) {
        budgetStarvedJobIds.push(job.job_id);
        continue;
      }

      let schedule;
      try {
        schedule = computeJobSchedule(job, nowEpoch);
      } catch (err) {
        invalidJobIds.push(job.job_id);
        logger.error(
          { err, jobId: job.job_id },
          "[scheduler] failed to compute next run, releasing claim"
        );
        continue;
      }

      if (schedule.ticks.length > remainingBudget) {
        budgetStarvedJobIds.push(job.job_id);
        budgetExhausted = true;
        continue;
      }

      remainingBudget -= schedule.ticks.length;
      validJobs.push({ job, ...schedule });
    }

    if (invalidJobIds.length > 0) {
      await releaseJobClaims(client, invalidJobIds, "invalid_schedule");
    }
    if (budgetStarvedJobIds.length > 0) {
      logger.warn(
        { count: budgetStarvedJobIds.length, jobIds: budgetStarvedJobIds },
        "[scheduler] releasing claims - execution queue depth budget exhausted for this poll"
      );
      await releaseJobClaims(client, budgetStarvedJobIds, "queue_depth_budget");
    }

    if (validJobs.length > 0) {
      const attempt = 1;

      const executionEntries = validJobs.flatMap(({ job, ticks }) =>
        ticks.map((scheduledFor) => ({ jobId: job.job_id, attempt, scheduledFor }))
      );
      const executionRows = await createExecutionBatch(client, executionEntries);

      // Match back on (job_id, scheduled_for), not job_id alone - a job
      // can now own multiple rows in this batch (backfill), all with the
      // same attempt=1, so job_id alone is no longer unique.
      const executionIdByKey = new Map(
        executionRows.map((r) => [`${r.job_id}:${r.scheduled_for}`, { executionId: r.execution_id, createdAt: r.created_at }])
      );

      await markJobScheduledBatch(
        client,
        validJobs.map(({ job, nextRunEpoch, isRecurring }) => ({
          jobId: job.job_id,
          nextRun: nextRunEpoch,
          isRecurring,
        }))
      );

      const outboxEntries = [];
      for (const { job, ticks } of validJobs) {
        for (const scheduledFor of ticks) {
          const { executionId, createdAt } = executionIdByKey.get(`${job.job_id}:${scheduledFor}`);
          outboxEntries.push({
            executionId,
            executionCreatedAt: createdAt,
            queueKey: EXECUTION_QUEUE_KEY,
            payload: { ...job, execution_id: executionId, created_at: createdAt, attempt, scheduled_for: scheduledFor },
          });
          toEnqueue.push({ ...job, execution_id: executionId, created_at: createdAt, scheduled_for: scheduledFor });
        }
      }
      await createOutboxEntryBatch(client, outboxEntries);
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