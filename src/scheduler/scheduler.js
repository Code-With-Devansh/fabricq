import { CronExpressionParser } from "cron-parser";
import { pool } from "../config/db.js";
import redis from "../config/redis.js";
import logger from "../config/logger/index.js";
import { claimDueJobs, markJobScheduled } from "../repositories/job.repository.js";
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

  try {
    await client.query("BEGIN");
    claimed = await claimDueJobs(client);
    await client.query("COMMIT");
  } catch (err) {
    await client.query("ROLLBACK");
    logger.error({ err }, "[scheduler] failed to claim due jobs");
    client.release();
    return;
  }

  if (claimed.length === 0) {
    client.release();
    return;
  }

  logger.info({ count: claimed.length }, "[scheduler] claimed due jobs");

  for (const job of claimed) {
    await scheduleOne(client, job).catch((err) => {
      logger.error({ err, jobId: job.job_id }, "[scheduler] failed to schedule job");
    });
  }

  client.release();
}

async function scheduleOne(client, job) {
  const isRecurring = job.schedule_type === "CRON";
  const attempt = job.attempts + 1;
  const scheduledForEpoch = Math.floor(new Date(job.next_run).getTime() / 1000);

  await client.query("BEGIN");
  try {
    const execution = await createExecution(client, {
      jobId: job.job_id,
      attempt,
      scheduledFor: scheduledForEpoch,
    });

    const nextRun = isRecurring ? computeNextRunEpoch(job) : null;
    await markJobScheduled(client, job.job_id, { nextRun, isRecurring });

    await client.query("COMMIT");
    await redis.lpush(EXECUTION_QUEUE_KEY, execution.execution_id);

    logger.info(
      { jobId: job.job_id, executionId: execution.execution_id },
      "[scheduler] execution queued"
    );
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  }
}

export { EXECUTION_QUEUE_KEY };