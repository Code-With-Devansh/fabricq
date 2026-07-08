import "dotenv/config";
import redis from "./config/redis.js";
import logger from "./config/logger/index.js";
import { setShuttingDown } from "./state/shutdown.js";
import config from "./config/index.js";

let server;
let isShuttingDown = false;

async function gracefulShutdown(signal) {
  if (isShuttingDown) return;
  isShuttingDown = true;
  const timeout = setTimeout(() => {
    logger.error("Forced shutdown after timeout");
    process.exit(1);
  }, 10000);

  logger.warn({ signal }, "Graceful shutdown started");

  try {
    setShuttingDown();
    await new Promise((resolve, reject) => {
      server.close((err) => {
        if (err) return reject(err);
        resolve();
      });
    });
    logger.info("HTTP server closed");

    await Promise.all([redis.quit()]);
    logger.info("MongoDB & Redis connection closed");

    logger.info("Shutdown complete");
    clearTimeout(timeout);
  } catch (err) {
    logger.error({ err }, "Shutdown failed");
    process.exit(1);
  }
}

async function startServer() {
  try {
    const { default: app } = await import("./app.js");
    const PORT = config.app.port || 5000;

    server = app.listen(PORT, () => {
      logger.info({ port: PORT }, "Server started");
    });

    process.on("SIGTERM", () => gracefulShutdown("SIGTERM"));
    process.on("SIGINT", () => gracefulShutdown("SIGINT"));
    process.on("unhandledRejection", (reason) => {
      logger.fatal({ reason }, "Unhandled Rejection");
      gracefulShutdown("UNHANDLED_REJECTION");
    });

    process.on("uncaughtException", (err) => {
      logger.fatal({ err }, "Uncaught Exception");
      gracefulShutdown("UNCAUGHT_EXCEPTION");
    });
  } catch (err) {
    logger.fatal({ err }, "Failed to start server");
    process.exit(1);
  }
}
startServer();
