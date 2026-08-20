import logger from "../config/logger/index.js";
import { pool } from "../config/db.js";

// How many months of job_executions partitions to keep. Must match what
// migration 024's drop_old_job_executions_partitions default assumes if
// called with no argument - passed explicitly here instead so this stays
// the single source of truth and doesn't quietly drift from the SQL
// function's own default.
const RETENTION_MONTHS = Number(process.env.JOB_EXECUTIONS_RETENTION_MONTHS) || 3;

// Create this many months ahead of the current month, every run. Cheap
// and idempotent (CREATE TABLE IF NOT EXISTS under the hood) - the buffer
// just means a slow/delayed retention run, or one that misses a cycle,
// never results in "no partition exists yet for a date that's already
// arrived" and rows falling into the DEFAULT partition.
const MONTHS_AHEAD_BUFFER = 2;

export async function ensureFuturePartitions() {
  const created = [];
  const now = new Date();
  for (let i = 0; i <= MONTHS_AHEAD_BUFFER; i++) {
    const monthStart = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth() + i, 1));
    const monthStartStr = monthStart.toISOString().slice(0, 10); // YYYY-MM-DD

    // create_job_executions_partition doesn't tell us whether it actually
    // created anything (IF NOT EXISTS under the hood) - check first so
    // the log line below is meaningful instead of claiming to "create" a
    // partition that already existed every single run.
    const partName = `job_executions_${monthStart.getUTCFullYear()}_${String(monthStart.getUTCMonth() + 1).padStart(2, "0")}`;
    const { rows: existsRows } = await pool.query(
      `SELECT 1 FROM pg_class WHERE relname = $1`,
      [partName]
    );
    if (existsRows.length > 0) continue;

    await pool.query("SELECT create_job_executions_partition($1::date)", [monthStartStr]);
    created.push(partName);
  }

  if (created.length > 0) {
    logger.info({ created }, "[retention] created job_executions partitions");
  }
  return created;
}

export async function dropExpiredPartitions() {
  const { rows } = await pool.query(
    "SELECT dropped_partition FROM drop_old_job_executions_partitions($1)",
    [RETENTION_MONTHS]
  );
  const dropped = rows.map((r) => r.dropped_partition);

  if (dropped.length > 0) {
    logger.info({ dropped, retentionMonths: RETENTION_MONTHS }, "[retention] dropped expired job_executions partitions");
  }
  return dropped;
}

export async function runPartitionMaintenance() {
  const created = await ensureFuturePartitions();
  const dropped = await dropExpiredPartitions();
  return { created, dropped };
}
