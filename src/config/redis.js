import IORedis from "ioredis";
import logger from "./logger/index.js";
import config from "./index.js";
if (!config.redis.url) {
  logger.fatal("[redis] REDIS_URL is not set");
  process.exit(1);
}
const options = {
  retryStrategy: (times) => {
    if (times > 10) {
      logger.fatal("[redis] max reconnection attempts reached");
      process.exit(1);
    }
    return Math.min(times * 100, 3000);
  },
  connectTimeout: 10000,
  maxRetriesPerRequest: null,
};

const client = new IORedis(config.redis.url, options);

client.on("connect", () => logger.info("[redis] connected"));
client.on("reconnecting", () => logger.warn("[redis] reconnecting..."));
client.on("error", (err) => logger.error({ err }, "[redis] client error"));

export default client;
