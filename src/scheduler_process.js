import "dotenv/config";
import logger from "./config/logger/index.js";
import { startScheduler } from "./scheduler/index.js";

process.on("unhandledRejection", (reason) => {
  logger.fatal({ reason }, "[scheduler-process] unhandled rejection");
  process.exit(1);
});
process.on("uncaughtException", (err) => {
  logger.fatal({ err }, "[scheduler-process] uncaught exception");
  process.exit(1);
});

startScheduler();