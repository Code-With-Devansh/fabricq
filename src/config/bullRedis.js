import IORedis from "ioredis";
import logger from "./logger/index.js";
import config from "./index.js";

if (!config.redis.url) {
  logger.fatal("[redis] REDIS_URL is not set");
  process.exit(1);
}

/**
 * BullMQ needs its own dedicated ioredis connection(s) - it issues
 * blocking commands and manages its own retry/reconnect semantics, so
 * it can't safely share a client with the plain redis usage in
 * config/redis.js (lists, ZSETs, pub/sub for the scheduler/worker/retry
 * processes). Call this once per Queue/Worker/QueueEvents instance.
 *
 * maxRetriesPerRequest MUST be null - BullMQ requires this so its
 * blocking calls aren't interrupted by ioredis's own retry logic.
 */
export function createBullConnection(name) {
  const client = new IORedis(config.redis.url, {
    maxRetriesPerRequest: null,
    enableReadyCheck: true,
    retryStrategy: (times) => {
      if (times > 10) {
        logger.fatal({ name }, "[redis:bullmq] max reconnection attempts reached");
        process.exit(1);
      }
      return Math.min(times * 100, 3000);
    },
  });

  client.on("connect", () => logger.info({ name }, "[redis:bullmq] connected"));
  client.on("reconnecting", () => logger.warn({ name }, "[redis:bullmq] reconnecting..."));
  client.on("error", (err) => logger.error({ err, name }, "[redis:bullmq] client error"));

  return client;
}
