export default {
  app: {
    env: process.env.NODE_ENV,
    port: Number(process.env.PORT)
  },
  redis:{
    port:Number(process.env.REDIS_PORT),
    host:process.env.REDIS_HOST
  },
   logging: {
    level: process.env.LOG_LEVEL,
  },
  postgres:{
    url:process.env.DATABASE_URL
  }
};
