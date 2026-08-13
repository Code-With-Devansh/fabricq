import config from "./index.js";
import logger from "./logger/index.js";

if (!config.auth.jwtSecret) {
  logger.fatal("[auth] JWT_ACCESS_SECRET is not set");
  process.exit(1);
}

if (config.auth.jwtSecret.length < 32) {
  logger.fatal(
    "[auth] JWT_ACCESS_SECRET is too short (min 32 chars) - use a high-entropy random value"
  );
  process.exit(1);
}
