export default {
  app: {
    env: process.env.NODE_ENV,
    port: Number(process.env.PORT),
  },
  redis: {
    url: process.env.REDIS_URL,
  },
  logging: {
    level: process.env.LOG_LEVEL,
  },
  postgres: {
    url: process.env.DATABASE_URL,
    max: Number(process.env.DATABASE_POOL_MAX) || 10,
    idleTimeoutMillis: Number(process.env.DATABASE_IDLE_TIMEOUT_MS) || 30_000,
    connectionTimeoutMillis:
      Number(process.env.DATABASE_CONNECT_TIMEOUT_MS) || 10_000,
  },
};
