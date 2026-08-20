import cron from "node-cron";
import logger from "../config/logger/index.js";
import { pool } from "../config/db.js";
import { runPartitionMaintenance } from "./partitionMaintenance.js";
import { runAuthTableCleanup, runOutboxCleanup } from "./authCleanup.js";

// Same pattern as scheduler/index.js's ADVISORY_LOCK_KEY - a distinct key
// so retention and the scheduler can each hold their own lock
// independently (no reason a second retention instance backing off
// should have anything to do with scheduler leadership, or vice versa).
const ADVISORY_LOCK_KEY = "fabricq-retention";

// Partition maintenance is cheap and idempotent even when it's a no-op
// (CREATE TABLE IF NOT EXISTS, DROP only what's actually past retention),
// so daily is plenty - there's no benefit to running it more often, and
// every run does touch the partition catalog so there's a small reason
// not to run it constantly either.
const PARTITION_MAINTENANCE_CRON = process.env.RETENTION_PARTITION_CRON || "0 3 * * *"; // 03:00 daily

// Auth-table and outbox cleanup deal with much higher row churn (every
// login rotates a refresh_tokens row, every schedule tick writes an
// outbox row) - hourly keeps each run's batch small instead of letting a
// day's backlog accumulate into one large batched run.
const CLEANUP_CRON = process.env.RETENTION_CLEANUP_CRON || "0 * * * *"; // hourly

let partitionTask = null;
let cleanupTask = null;
let partitionRunning = false;
let cleanupRunning = false;
let shuttingDown = false;
let lockClient = null;

async function acquireSingleWriterLock() {
  const client = await pool.connect();
  const { rows } = await client.query("SELECT pg_try_advisory_lock(hashtext($1)) AS acquired", [
    ADVISORY_LOCK_KEY,
  ]);
  if (!rows[0].acquired) {
    client.release();
    return null;
  }
  // Not released back to the pool - tied to this session for as long as
  // the lock is held, same as scheduler/index.js.
  return client;
}

async function runPartitionMaintenanceTick() {
  if (shuttingDown || partitionRunning) return;
  partitionRunning = true;
  try {
    await runPartitionMaintenance();
  } catch (err) {
    logger.error({ err }, "[retention] partition maintenance tick failed");
  } finally {
    partitionRunning = false;
  }
}

async function runCleanupTick() {
  if (shuttingDown || cleanupRunning) return;
  cleanupRunning = true;
  try {
    // Independent tables, independent failure domains (each already
    // catches its own errors internally) - run concurrently rather than
    // making outbox cleanup wait behind five sequential auth-table sweeps.
    await Promise.all([runAuthTableCleanup(), runOutboxCleanup()]);
  } catch (err) {
    logger.error({ err }, "[retention] cleanup tick failed");
  } finally {
    cleanupRunning = false;
  }
}

export async function startRetention() {
  lockClient = await acquireSingleWriterLock();
  if (!lockClient) {
    logger.warn(
      "[retention] another instance already holds the retention advisory lock, not starting"
    );
    return;
  }

  shuttingDown = false;

  partitionTask = cron.schedule(PARTITION_MAINTENANCE_CRON, runPartitionMaintenanceTick);
  cleanupTask = cron.schedule(CLEANUP_CRON, runCleanupTick);

  // Run once immediately on startup rather than waiting for the first
  // cron tick - otherwise a freshly deployed instance sits doing nothing
  // for up to a full cycle, and if it's the first-ever run in a new
  // environment, upcoming partitions might not exist yet when the
  // scheduler needs them.
  await runPartitionMaintenanceTick();
  await runCleanupTick();

  logger.info(
    { partitionCron: PARTITION_MAINTENANCE_CRON, cleanupCron: CLEANUP_CRON },
    "[retention] started"
  );
}

export async function stopRetention() {
  shuttingDown = true;
  if (partitionTask) partitionTask.stop();
  if (cleanupTask) cleanupTask.stop();

  // Let any in-flight tick finish rather than killing it mid-batch.
  while (partitionRunning || cleanupRunning) {
    await new Promise((r) => setTimeout(r, 100));
  }

  if (lockClient) {
    try {
      await lockClient.query("SELECT pg_advisory_unlock(hashtext($1))", [ADVISORY_LOCK_KEY]);
    } catch (err) {
      logger.error({ err }, "[retention] failed to release advisory lock");
    } finally {
      lockClient.release();
      lockClient = null;
    }
  }
  logger.info("[retention] stopped");
}
