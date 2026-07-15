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
  },
};
