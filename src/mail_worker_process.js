import "dotenv/config";
import logger from "./config/logger/index.js";
import config from "./config/index.js";
import { startMailWorker } from "./mail/worker.js";

if (!config.mail.resendApiKey) {
  logger.fatal("[mail-process] RESEND_API_KEY is not set");
  process.exit(1);
}

const FORCE_EXIT_MS = 15_000;

let shuttingDown = false;
let worker;

async function gracefulShutdown(signal) {
  if (shuttingDown) return;
  shuttingDown = true;
  logger.warn({ signal }, "[mail-process] graceful shutdown started");

  const forceTimer = setTimeout(() => {
    logger.error("[mail-process] forced shutdown after timeout");
    process.exit(1);
  }, FORCE_EXIT_MS);
  forceTimer.unref?.();

  try {
    // Worker#close waits for any in-flight job to finish before
    // disconnecting, same as the other processes waiting on their
    // current unit of work.
    await worker?.close();
    logger.info("[mail-process] shutdown complete");
    clearTimeout(forceTimer);
    process.exit(0);
  } catch (err) {
    logger.error({ err }, "[mail-process] shutdown failed");
    process.exit(1);
  }
}

process.on("SIGTERM", () => gracefulShutdown("SIGTERM"));
process.on("SIGINT", () => gracefulShutdown("SIGINT"));

process.on("unhandledRejection", (reason) => {
  logger.fatal({ reason }, "[mail-process] unhandled rejection");
  gracefulShutdown("UNHANDLED_REJECTION");
});
process.on("uncaughtException", (err) => {
  logger.fatal({ err }, "[mail-process] uncaught exception");
  gracefulShutdown("UNCAUGHT_EXCEPTION");
});

logger.info("[mail-process] started, waiting for email jobs");
worker = startMailWorker();
