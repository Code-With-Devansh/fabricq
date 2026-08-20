import "dotenv/config";
import logger from "./config/logger/index.js";
import { startRetention, stopRetention } from "./retention/index.js";
import { pool } from "./config/db.js";

const FORCE_EXIT_MS = 30_000; // batched deletes can take longer to wind down than a single poll cycle

let shuttingDown = false;

async function gracefulShutdown(signal) {
  if (shuttingDown) return;
  shuttingDown = true;
  logger.warn({ signal }, "[retention-process] graceful shutdown started");

  const forceTimer = setTimeout(() => {
    logger.error("[retention-process] forced shutdown after timeout");
    process.exit(1);
  }, FORCE_EXIT_MS);
  forceTimer.unref?.();

  try {
    await stopRetention();
    await pool.end();
    logger.info("[retention-process] shutdown complete");
    clearTimeout(forceTimer);
    process.exit(0);
  } catch (err) {
    logger.error({ err }, "[retention-process] shutdown failed");
    process.exit(1);
  }
}

process.on("SIGTERM", () => gracefulShutdown("SIGTERM"));
process.on("SIGINT", () => gracefulShutdown("SIGINT"));

process.on("unhandledRejection", (reason) => {
  logger.fatal({ reason }, "[retention-process] unhandled rejection");
  gracefulShutdown("UNHANDLED_REJECTION");
});
process.on("uncaughtException", (err) => {
  logger.fatal({ err }, "[retention-process] uncaught exception");
  gracefulShutdown("UNCAUGHT_EXCEPTION");
});

startRetention();
