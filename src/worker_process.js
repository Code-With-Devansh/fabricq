import "dotenv/config";
import logger from "./config/logger/index.js";
import { startWorker, stopWorker } from "./worker/worker.js";
import { pool } from "./config/db.js";
import redis from "./config/redis.js";

// Longest an in-flight job can legitimately take is bounded by the
// timeout_ms validator (max 120s). Give it real headroom over that before
// giving up and force-exiting, so a well-behaved shutdown almost never
// hits this path - it's a last resort, not the expected exit route.
const FORCE_EXIT_MS = 150_000;

let shuttingDown = false;
let workerLoop;

async function gracefulShutdown(signal) {
  if (shuttingDown) return;
  shuttingDown = true;
  logger.warn({ signal }, "[worker-process] graceful shutdown started");

  const forceTimer = setTimeout(() => {
    logger.error("[worker-process] forced shutdown after timeout");
    process.exit(1);
  }, FORCE_EXIT_MS);
  forceTimer.unref?.();

  try {
    // Stops the loop from pulling new jobs and waits for whatever is
    // currently running to finish (or hit its own AbortController timeout).
    await stopWorker();
    // Let the main loop's promise settle too, in case it's mid-iteration.
    await workerLoop;

    await Promise.all([pool.end(), redis.quit()]);
    logger.info("[worker-process] shutdown complete");
    clearTimeout(forceTimer);
    process.exit(0);
  } catch (err) {
    logger.error({ err }, "[worker-process] shutdown failed");
    process.exit(1);
  }
}

process.on("SIGTERM", () => gracefulShutdown("SIGTERM"));
process.on("SIGINT", () => gracefulShutdown("SIGINT"));

process.on("unhandledRejection", (reason) => {
  logger.fatal({ reason }, "[worker-process] unhandled rejection");
  gracefulShutdown("UNHANDLED_REJECTION");
});
process.on("uncaughtException", (err) => {
  logger.fatal({ err }, "[worker-process] uncaught exception");
  gracefulShutdown("UNCAUGHT_EXCEPTION");
});

workerLoop = startWorker();