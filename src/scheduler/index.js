import cron from "node-cron";
import logger from "../config/logger/index.js";
import { pollAndScheduleDueJobs } from "./scheduler.js";

let running = false;

export function startScheduler() {
  cron.schedule("*/10 * * * * *", async () => {
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