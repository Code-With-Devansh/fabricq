import redis from "../config/redis.js";
import { pool } from "../config/db.js";
import logger from "../config/logger/index.js";
import { findJobById } from "../repositories/httpJob.repository.js";
import { computeDelaySeconds } from "./backoff.js";

// Worker/recovery LPUSH onto this when a ONCE job fails but has retries
// left. This process is the only thing that ever turns a NULL next_run
// back into a real timestamp for such jobs - once it does, the existing
// scheduler poll (next_run <= now()) picks it up exactly like any other
// due job. No second queue/zset needed.
export const RETRY_INTAKE_KEY = "fabricq:retry:intake";

// LPUSH isn't durable the way the Postgres claim pattern is - if this
// process is down when a push happens, that job would sit at
// next_run = NULL forever with nothing watching it. This reconciliation
// query is the fallback: anything that's been sitting unscheduled for a
// while gets picked up and given a next_run on the next sweep, same as if
// its intake message had arrived normally.
const RECONCILE_STALE_INTERVAL = "1 minute";

async function scheduleRetry(jobId, attempt) {
  const job = await findJobById(jobId);
  if (!job) {
    logger.warn({ jobId }, "[retry] job no longer exists, dropping intake entry");
    return;
  }
  if (!job.enabled) {
    logger.debug({ jobId }, "[retry] job disabled, skipping retry scheduling");
    return;
  }
  // Nothing to do if it already has a next_run - either another intake
  // message beat us to it, or the reconciliation sweep already handled it.
  if (job.next_run !== null) {
    return;
  }

  const delaySeconds = computeDelaySeconds(job, attempt ?? job.attempts);

  await pool.query(
    `UPDATE http_jobs
     SET next_run = now() + ($1 || ' seconds')::interval
     WHERE job_id = $2 AND next_run IS NULL`,
    [delaySeconds, jobId],
  );

  logger.info(
    { jobId, attempt, strategy: job.retry_strategy, delaySeconds },
    "[retry] scheduled next attempt",
  );
}

export async function runReconciliationSweep() {
  const { rows } = await pool.query(
    `SELECT job_id, attempts
     FROM http_jobs
     WHERE enabled
       AND schedule_type = 'ONCE'
       AND next_run IS NULL
       AND attempts > 0
       AND attempts < max_attempts
       AND updated_at < now() - interval '${RECONCILE_STALE_INTERVAL}'`,
  );

  if (rows.length === 0) return;

  logger.warn(
    { count: rows.length },
    "[retry] reconciliation found jobs stuck without next_run, catching up",
  );

  for (const row of rows) {
    try {
      await scheduleRetry(row.job_id, row.attempts);
    } catch (err) {
      logger.error({ err, jobId: row.job_id }, "[retry] reconciliation failed for job");
    }
  }
}

let shuttingDown = false;

export async function runIntakeLoop() {
  logger.info("[retry] intake loop started, waiting for failed jobs");
  while (!shuttingDown) {
    const item = await redis.brpop(RETRY_INTAKE_KEY, 5);
    if (!item) continue; // timed out, nothing to do
    if (shuttingDown) {
      logger.warn("[retry] popped an intake entry after shutdown was requested, finishing it anyway");
    }

    let payload;
    try {
      payload = JSON.parse(item[1]);
    } catch (err) {
      logger.error({ err, raw: item[1] }, "[retry] unparseable intake entry, dropping");
      continue;
    }

    try {
      await scheduleRetry(payload.jobId, payload.attempt);
    } catch (err) {
      logger.error({ err, payload }, "[retry] failed to schedule retry, will be caught by reconciliation sweep");
    }
  }
  logger.info("[retry] intake loop exited");
}

export function requestShutdown() {
  shuttingDown = true;
}
