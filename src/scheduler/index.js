import cron from "node-cron";
import logger from "../config/logger/index.js";
import { pollAndScheduleDueJobs } from "./scheduler.js";

let running = false;
let task = null;
let shuttingDown = false;

export function startScheduler() {
  task = cron.schedule("*/10 * * * * *", async () => {
    if (shuttingDown) return;
    if (running) {
      logger.warn("[scheduler] previous poll still running, skipping this tick");
      return;
    }
    running = true;
    try {
      await pollAndScheduleDueJobs();
    } catch (err) {
      logger.error({ err }, "[scheduler] poll cycle failed");
    } finally {
      running = false;
    }
  });

  logger.info("[scheduler] started, polling every 10s");
}

export async function stopScheduler() {
  shuttingDown = true;
  if (task) task.stop();
  while (running) {
    await new Promise((r) => setTimeout(r, 100));
  }
  logger.info("[scheduler] stopped, no in-flight poll cycle remaining");
}