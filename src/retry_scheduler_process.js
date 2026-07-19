import "dotenv/config";
import logger from "./config/logger/index.js";
import { startRetryScheduler, stopRetryScheduler } from "./retry/index.js";
import { pool } from "./config/db.js";
import redis from "./config/redis.js";

const FORCE_EXIT_MS = 15_000;

let shuttingDown = false;

async function gracefulShutdown(signal) {
  if (shuttingDown) return;
  shuttingDown = true;
  logger.warn({ signal }, "[retry-process] graceful shutdown started");

  const forceTimer = setTimeout(() => {
    logger.error("[retry-process] forced shutdown after timeout");
    process.exit(1);
  }, FORCE_EXIT_MS);
  forceTimer.unref?.();

  try {
    // Stops the intake loop and the reconciliation cron, waiting for
    // whichever is in-flight to finish before we tear down connections.
    await stopRetryScheduler();

    await Promise.all([pool.end(), redis.quit()]);
    logger.info("[retry-process] shutdown complete");
    clearTimeout(forceTimer);
    process.exit(0);
  } catch (err) {
    logger.error({ err }, "[retry-process] shutdown failed");
    process.exit(1);
  }
}

process.on("SIGTERM", () => gracefulShutdown("SIGTERM"));
process.on("SIGINT", () => gracefulShutdown("SIGINT"));

process.on("unhandledRejection", (reason) => {
  logger.fatal({ reason }, "[retry-process] unhandled rejection");
  gracefulShutdown("UNHANDLED_REJECTION");
});
process.on("uncaughtException", (err) => {
  logger.fatal({ err }, "[retry-process] uncaught exception");
  gracefulShutdown("UNCAUGHT_EXCEPTION");
});

startRetryScheduler();
