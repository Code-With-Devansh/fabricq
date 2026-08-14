import { CronExpressionParser } from "cron-parser";
import { pool } from "../config/db.js";
import redis from "../config/redis.js";
import logger from "../config/logger/index.js";
import { claimDueJobs, markJobScheduled } from "../repositories/httpJob.repository.js";
import { createExecution } from "../repositories/execution.repository.js";

const EXECUTION_QUEUE_KEY = "fabricq:executions";

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

    // Claiming and scheduling happen in the SAME transaction now - not two
    // separate commits like before. That matters specifically for ONCE
    // jobs: markJobScheduled clears next_run as part of this transaction,
    // so if the process crashes anywhere before COMMIT, the whole batch
    // rolls back and the job goes right back to being a normal due job on
    // the next poll - no window where it's claimed (locked_at set) but
    // next_run is still sitting in the past, which was the root cause of
    // the double-schedule race this replaces.
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

  // Redis push happens after the transaction commits. If the process dies
  // in this specific gap, the affected jobs are fully scheduled in
  // Postgres (execution row created, next_run cleared/advanced) but never
  // reach a worker - a pre-existing gap that equally affects CRON jobs
  // today, not something this change introduces or widens. Worth a
  // dedicated fix later (e.g. a recovery sweep for "queued but never
  // heartbeated" executions) but out of scope here.
  for (const job of toEnqueue) {
    try {
      await redis.lpush(EXECUTION_QUEUE_KEY, JSON.stringify(job));
      logger.info(
        { jobId: job.job_id, executionId: job.execution_id },
        "[scheduler] execution queued"
      );
    } catch (err) {
      logger.error(
        { err, jobId: job.job_id, executionId: job.execution_id },
        "[scheduler] failed to push scheduled execution to redis"
      );
    }
  }
}

async function scheduleOne(client, job) {
  const isRecurring = job.schedule_type === "CRON";
  const attempt = job.attempts + 1;
  const scheduledForEpoch = Math.floor(new Date(job.next_run).getTime() / 1000);

  const execution = await createExecution(client, {
    jobId: job.job_id,
    attempt,
    scheduledFor: scheduledForEpoch,
  });

  const nextRun = isRecurring ? computeNextRunEpoch(job) : null;
  await markJobScheduled(client, job.job_id, { nextRun, isRecurring });

  return execution;
}

export { EXECUTION_QUEUE_KEY };