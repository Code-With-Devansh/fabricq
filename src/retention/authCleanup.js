import logger from "../config/logger/index.js";
import { pool } from "../config/db.js";

// All five token/invite tables use expires_at as the sole staleness
// anchor. Once a row is this far past its own expires_at, it's unusable
// regardless of whether it was ever consumed/revoked/accepted - a
// consumed/revoked/accepted row's resolution event virtually always
// happens before expires_at anyway, so deriving staleness from
// expires_at alone covers every resolution state without needing a
// separate index or predicate per table.
const AUTH_TOKEN_RETENTION_DAYS = Number(process.env.AUTH_TOKEN_RETENTION_DAYS) || 7;

// execution_outbox rows are only useful until the relay publishes them -
// once published, nothing reads them again. Short retention (hours, not
// days) since there's no audit requirement on outbox specifically -
// job_executions is the actual audit trail.
const OUTBOX_PUBLISHED_RETENTION_HOURS = Number(process.env.OUTBOX_PUBLISHED_RETENTION_HOURS) || 24;

// Delete in batches rather than one giant DELETE, so a table with a large
// backlog (e.g. retention running for the first time against months of
// accumulated rows) doesn't hold a single long-running transaction/lock
// or blow out WAL in one shot. Small pause between batches gives
// concurrent traffic (logins hitting refresh_tokens, etc.) room to
// interleave instead of queueing behind a cleanup job.
const BATCH_SIZE = Number(process.env.RETENTION_BATCH_SIZE) || 5_000;
const BATCH_PAUSE_MS = Number(process.env.RETENTION_BATCH_PAUSE_MS) || 250;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// ctid-based batching: DELETE doesn't support LIMIT directly, so select a
// batch of ctids first (using the expires_at index), then delete exactly
// those rows. Cheaper than a correlated subquery per row and avoids
// re-scanning rows a previous batch already handled.
async function deleteBatchedByExpiry(table, retentionDays) {
  let totalDeleted = 0;
  for (;;) {
    const { rowCount } = await pool.query(
      `DELETE FROM ${table}
       WHERE ctid IN (
         SELECT ctid FROM ${table}
         WHERE expires_at < now() - ($1 || ' days')::interval
         LIMIT $2
       )`,
      [retentionDays, BATCH_SIZE]
    );
    totalDeleted += rowCount;
    if (rowCount < BATCH_SIZE) break;
    await sleep(BATCH_PAUSE_MS);
  }
  return totalDeleted;
}

async function deleteBatchedOutboxPublished(retentionHours) {
  let totalDeleted = 0;
  for (;;) {
    const { rowCount } = await pool.query(
      `DELETE FROM execution_outbox
       WHERE ctid IN (
         SELECT ctid FROM execution_outbox
         WHERE published_at IS NOT NULL
           AND published_at < now() - ($1 || ' hours')::interval
         LIMIT $2
       )`,
      [retentionHours, BATCH_SIZE]
    );
    totalDeleted += rowCount;
    if (rowCount < BATCH_SIZE) break;
    await sleep(BATCH_PAUSE_MS);
  }
  return totalDeleted;
}

// Table names are hardcoded literals below, never interpolated from
// anything outside this file - safe despite the string-built DELETE in
// deleteBatchedByExpiry above.
const AUTH_TABLES = [
  "refresh_tokens",
  "team_invitations",
  "email_verification_tokens",
  "password_reset_tokens",
  "two_factor_challenges",
];

export async function runAuthTableCleanup() {
  const results = {};
  for (const table of AUTH_TABLES) {
    try {
      results[table] = await deleteBatchedByExpiry(table, AUTH_TOKEN_RETENTION_DAYS);
    } catch (err) {
      // One table failing shouldn't block the others - log and continue,
      // this run's summary will show 0/undefined for it and the next
      // scheduled run picks it back up.
      logger.error({ err, table }, "[retention] auth table cleanup failed");
      results[table] = null;
    }
  }

  logger.info({ deleted: results, retentionDays: AUTH_TOKEN_RETENTION_DAYS }, "[retention] auth table cleanup complete");
  return results;
}

export async function runOutboxCleanup() {
  let deleted = null;
  try {
    deleted = await deleteBatchedOutboxPublished(OUTBOX_PUBLISHED_RETENTION_HOURS);
  } catch (err) {
    logger.error({ err }, "[retention] execution_outbox cleanup failed");
  }

  logger.info({ deleted, retentionHours: OUTBOX_PUBLISHED_RETENTION_HOURS }, "[retention] execution_outbox cleanup complete");
  return deleted;
}
