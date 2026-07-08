import { Pool } from "pg";
import config from './index.js'
export const pool = new Pool({
  connectionString: config.postgres.url,
});