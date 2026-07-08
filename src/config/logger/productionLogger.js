import pino from "pino";
import config from "../index.js";

const logger = pino({
  level: config.logging.level || "info",
  timestamp: pino.stdTimeFunctions.isoTime,
  base: {
    service: "api",
    env: config.app.env,
  },
  formatters: {
    level: (label) => ({ level: label }),
  },
  serializers: {
    req: pino.stdSerializers.req,
    res: pino.stdSerializers.res,
    err: pino.stdSerializers.err,
  },
});

export default logger;