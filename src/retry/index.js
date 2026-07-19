import cron from "node-cron";
import logger from "../config/logger/index.js";
import {
  runIntakeLoop,
  runReconciliationSweep,
  requestShutdown,
} from "./retry.js";

let intakeLoopPromise = null;
let sweepRunning = false;
let sweepTask = null;
let shuttingDown = false;

export function startRetryScheduler() {
  intakeLoopPromise = runIntakeLoop();

  sweepTask = cron.schedule("*/30 * * * * *", async () => {
    if (shuttingDown) return;
    if (sweepRunning) {
      logger.warn("[retry] previous reconciliation sweep still running, skipping this tick");
      return;
    }
    sweepRunning = true;
    try {
      await runReconciliationSweep();
    } catch (err) {
      logger.error({ err }, "[retry] reconciliation sweep failed");
    } finally {
      sweepRunning = false;
    }
  });

  logger.info("[retry] started: intake loop live, reconciliation sweep every 30s");
}

export async function stopRetryScheduler() {
  shuttingDown = true;
  requestShutdown();
  if (sweepTask) sweepTask.stop();

  if (intakeLoopPromise) {
    await intakeLoopPromise.catch(() => {});
  }
  while (sweepRunning) {
    await new Promise((r) => setTimeout(r, 100));
  }
  logger.info("[retry] stopped, no in-flight work remaining");
}
