import pino from "pino";
import config from "../index.js";

const logger = pino(
  {
    level: config.logging.level || "debug",
    timestamp: pino.stdTimeFunctions.isoTime,
    redact: {
      paths: ["req.headers.authorization", "*.password", "*.token"],
      censor: "[REDACTED]",
    },
  },
  pino.transport({targets:[
    {
      target: "pino-pretty",
      options: {
        colorize: true,
      },
    },
  ]}),
);

export default logger;
