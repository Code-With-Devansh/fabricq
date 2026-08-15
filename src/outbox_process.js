import "dotenv/config";
import logger from "./config/logger/index.js";
import { startOutboxRelay, stopOutboxRelay } from "./outbox/index.js";
import { pool } from "./config/db.js";
import redis from "./config/redis.js";

const FORCE_EXIT_MS = 15_000;

let shuttingDown = false;

async function gracefulShutdown(signal) {
  if (shuttingDown) return;
  shuttingDown = true;
  logger.warn({ signal }, "[outbox-process] graceful shutdown started");

  const forceTimer = setTimeout(() => {
    logger.error("[outbox-process] forced shutdown after timeout");
    process.exit(1);
  }, FORCE_EXIT_MS);
  forceTimer.unref?.();

  try {
    // Waits for an in-flight sweep (claim -> publish -> mark) to finish
    // before tearing down connections, so we never kill it mid-batch.
    await stopOutboxRelay();

    await Promise.all([pool.end(), redis.quit()]);
    logger.info("[outbox-process] shutdown complete");
    clearTimeout(forceTimer);
    process.exit(0);
  } catch (err) {
    logger.error({ err }, "[outbox-process] shutdown failed");
    process.exit(1);
  }
}

process.on("SIGTERM", () => gracefulShutdown("SIGTERM"));
process.on("SIGINT", () => gracefulShutdown("SIGINT"));

process.on("unhandledRejection", (reason) => {
  logger.fatal({ reason }, "[outbox-process] unhandled rejection");
  gracefulShutdown("UNHANDLED_REJECTION");
});
process.on("uncaughtException", (err) => {
  logger.fatal({ err }, "[outbox-process] uncaught exception");
  gracefulShutdown("UNCAUGHT_EXCEPTION");
});

startOutboxRelay();
