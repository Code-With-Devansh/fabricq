import "dotenv/config";
import logger from "./config/logger/index.js";
import { startWorker } from "./worker/worker.js";

process.on("unhandledRejection", (reason) => {
  logger.fatal({ reason }, "[worker-process] unhandled rejection");
  process.exit(1);
});
process.on("uncaughtException", (err) => {
  logger.fatal({ err }, "[worker-process] uncaught exception");
  process.exit(1);
});

startWorker();