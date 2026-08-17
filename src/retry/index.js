import cron from "node-cron";
import logger from "../config/logger/index.js";
import { runRetrySweep } from "./retry.js";

// No more Redis intake to listen for - retry state lives entirely on
// job_executions (see migration 020), so this process just polls for due
// retry_wait rows. 5s cadence keeps IMMEDIATE/short-backoff retries feeling
// responsive without hammering Postgres; claimDueRetries' partial index
// keeps each poll cheap even when idle.
let sweepRunning = false;
let sweepTask = null;
let shuttingDown = false;

export function startRetryScheduler() {
  sweepTask = cron.schedule("*/5 * * * * *", async () => {
    if (shuttingDown) return;
    if (sweepRunning) {
      logger.warn("[retry] previous sweep still running, skipping this tick");
      return;
    }
    sweepRunning = true;
    try {
      await runRetrySweep();
    } catch (err) {
      logger.error({ err }, "[retry] sweep failed");
    } finally {
      sweepRunning = false;
    }
  });

  logger.info("[retry] started: polling for due retries every 5s");
}

export async function stopRetryScheduler() {
  shuttingDown = true;
  if (sweepTask) sweepTask.stop();

  while (sweepRunning) {
    await new Promise((r) => setTimeout(r, 100));
  }
  logger.info("[retry] stopped, no in-flight work remaining");
}
