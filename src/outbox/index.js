import cron from "node-cron";
import logger from "../config/logger/index.js";
import { publishPendingExecutions } from "./outbox.js";

let running = false;
let task = null;
let shuttingDown = false;

// Every 5s. The scheduler's fast-path push handles the common case
// within the same tick it schedules, so this doesn't need to be
// sub-second - it just needs to be short enough that a crash between
// commit and Redis push is invisible in practice.
export function startOutboxRelay() {
  task = cron.schedule("*/5 * * * * *", async () => {
    if (shuttingDown) return;
    if (running) {
      logger.warn("[outbox] previous sweep still running, skipping this tick");
      return;
    }
    running = true;
    try {
      await publishPendingExecutions();
    } catch (err) {
      logger.error({ err }, "[outbox] sweep tick failed");
    } finally {
      running = false;
    }
  });

  logger.info("[outbox] relay started, sweeping every 5s");
}

export async function stopOutboxRelay() {
  shuttingDown = true;
  if (task) task.stop();
  while (running) {
    await new Promise((r) => setTimeout(r, 100));
  }
  logger.info("[outbox] relay stopped, no in-flight sweep remaining");
}
