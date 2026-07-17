import cron from "node-cron";
import logger from "../config/logger/index.js";
import { runRecoveryCycle } from "./recovery.js";

let running = false;
let task = null;
let shuttingDown = false;

export function startRecovery() {
  task = cron.schedule("*/15 * * * * *", async () => {
    if (shuttingDown) return;
    if (running) {
      logger.warn("[recovery] previous cycle still running, skipping this tick");
      return;
    }
    running = true;
    try {
      await runRecoveryCycle();
    } catch (err) {
      logger.error({ err }, "[recovery] cycle failed");
    } finally {
      running = false;
    }
  });

  logger.info("[recovery] started, sweeping for abandoned executions every 15s");
}

export async function stopRecovery() {
  shuttingDown = true;
  if (task) task.stop();
  while (running) {
    await new Promise((r) => setTimeout(r, 100));
  }
  logger.info("[recovery] stopped, no in-flight cycle remaining");
}
