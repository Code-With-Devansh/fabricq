import "dotenv/config";
import logger from "./config/logger/index.js";
import { startScheduler, stopScheduler } from "./scheduler/index.js";
import { pool } from "./config/db.js";
import redis from "./config/redis.js";

const FORCE_EXIT_MS = 15_000;

let shuttingDown = false;

async function gracefulShutdown(signal) {
  if (shuttingDown) return;
  shuttingDown = true;
  logger.warn({ signal }, "[scheduler-process] graceful shutdown started");

  const forceTimer = setTimeout(() => {
    logger.error("[scheduler-process] forced shutdown after timeout");
    process.exit(1);
  }, FORCE_EXIT_MS);
  forceTimer.unref?.();

  try {
    // Stops new ticks and waits for an in-flight poll cycle (claim -> queue
    // jobs) to finish, so we never kill it mid-transaction.
    await stopScheduler();

    await Promise.all([pool.end(), redis.quit()]);
    logger.info("[scheduler-process] shutdown complete");
    clearTimeout(forceTimer);
    process.exit(0);
  } catch (err) {
    logger.error({ err }, "[scheduler-process] shutdown failed");
    process.exit(1);
  }
}

process.on("SIGTERM", () => gracefulShutdown("SIGTERM"));
process.on("SIGINT", () => gracefulShutdown("SIGINT"));

process.on("unhandledRejection", (reason) => {
  logger.fatal({ reason }, "[scheduler-process] unhandled rejection");
  gracefulShutdown("UNHANDLED_REJECTION");
});
process.on("uncaughtException", (err) => {
  logger.fatal({ err }, "[scheduler-process] uncaught exception");
  gracefulShutdown("UNCAUGHT_EXCEPTION");
});

startScheduler();