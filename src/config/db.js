import { Pool } from "pg";
import config from "./index.js";

if (!config.postgres.url) {
  logger.fatal("[postgres] DATABASE_URL is not set");
  process.exit(1);
}
export const pool = new Pool({
  connectionString: config.postgres.url,
  max: config.postgres.max,
  idleTimeoutMillis: config.postgres.idleTimeoutMillis,
  connectionTimeoutMillis: config.postgres.connectionTimeoutMillis,
});

pool.on("error", (err) => {
  logger.error({ err }, "[postgres] idle client error");
});