import "dotenv/config";
import logger from "./config/logger/index.js";
import { startMerger, stopMerger } from "./merger/merger.js";
import { pool } from "./config/db.js";
import redis from "./config/redis.js";

const FORCE_EXIT_MS = 15_000;

let shuttingDown = false;

async function gracefulShutdown(signal) {
  if (shuttingDown) return;
  shuttingDown = true;
  logger.warn({ signal }, "[merger-process] graceful shutdown started");

  const forceTimer = setTimeout(() => {
    logger.error("[merger-process] forced shutdown after timeout");
    process.exit(1);
  }, FORCE_EXIT_MS);
  forceTimer.unref?.();

  try {
    // Stops pulling new stream entries and lets an in-flight batch flush
    // finish (or fail cleanly and stay pending for the next merger to claim).
    await stopMerger();

    await Promise.all([pool.end(), redis.quit()]);
    logger.info("[merger-process] shutdown complete");
    clearTimeout(forceTimer);
    process.exit(0);
  } catch (err) {
    logger.error({ err }, "[merger-process] shutdown failed");
    process.exit(1);
  }
}

process.on("SIGTERM", () => gracefulShutdown("SIGTERM"));
process.on("SIGINT", () => gracefulShutdown("SIGINT"));

process.on("unhandledRejection", (reason) => {
  logger.fatal({ reason }, "[merger-process] unhandled rejection");
  gracefulShutdown("UNHANDLED_REJECTION");
});
process.on("uncaughtException", (err) => {
  logger.fatal({ err }, "[merger-process] uncaught exception");
  gracefulShutdown("UNCAUGHT_EXCEPTION");
});

startMerger();
