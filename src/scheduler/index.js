import logger from "../config/logger/index.js";
import { pool } from "../config/db.js";
import { pollAndScheduleDueJobs } from "./scheduler.js";
import { DEFAULT_CLAIM_LIMIT } from "../repositories/httpJob.repository.js";

// Adaptive polling (fix #1): under light load this behaves exactly like
// the old fixed 10s cron - MAX_POLL_MS is the same steady-state cadence.
// Under heavy load, a full batch (claimed.length >= batch size) is a
// direct backpressure signal that there's more work waiting right now, so
// the next tick fires almost immediately instead of waiting out the rest
// of a 10s window. This is what removes the "10 jobs/sec no matter what"
// ceiling - throughput under backlog is now bounded by (batch size /
// MIN_POLL_MS), not by a fixed tick rate.
const MIN_POLL_MS = Number(process.env.SCHEDULER_MIN_POLL_MS) || 200;
const MAX_POLL_MS = Number(process.env.SCHEDULER_MAX_POLL_MS) || 10_000;

// Session-level advisory lock key (fix #4). Concurrent scheduler
// instances are already safe (FOR UPDATE SKIP LOCKED), but they don't
// help throughput - they just contend on the same due-job window and
// split the same batch. This turns "only run one" from an unenforced
// assumption into something a second instance actually detects and
// backs off from at startup, rather than silently running and quietly
// halving everyone's batches.
const ADVISORY_LOCK_KEY = "fabricq-scheduler";

let running = false;
let shuttingDown = false;
let loopPromise = null;
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
  // Deliberately NOT releasing this client back to the pool - the lock is
  // tied to this session for as long as we hold it open. It's released
  // (and the lock dropped) explicitly in stopScheduler, or automatically
  // by Postgres if the connection dies.
  return client;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function loop() {
  while (!shuttingDown) {
    running = true;
    let claimedCount = 0;
    try {
      claimedCount = (await pollAndScheduleDueJobs()) ?? 0;
    } catch (err) {
      logger.error({ err }, "[scheduler] poll cycle failed");
    } finally {
      running = false;
    }

    if (shuttingDown) break;

    const delay = claimedCount >= DEFAULT_CLAIM_LIMIT ? MIN_POLL_MS : MAX_POLL_MS;
    await sleep(delay);
  }
}

export async function startScheduler() {
  lockClient = await acquireSingleWriterLock();
  if (!lockClient) {
    logger.warn(
      "[scheduler] another instance already holds the scheduler advisory lock, not starting poll loop"
    );
    return;
  }

  shuttingDown = false;
  loopPromise = loop();
  logger.info(
    { minPollMs: MIN_POLL_MS, maxPollMs: MAX_POLL_MS },
    "[scheduler] started with adaptive polling"
  );
}

export async function stopScheduler() {
  shuttingDown = true;
  if (loopPromise) {
    await loopPromise;
  }
  while (running) {
    await new Promise((r) => setTimeout(r, 100));
  }
  if (lockClient) {
    try {
      await lockClient.query("SELECT pg_advisory_unlock(hashtext($1))", [ADVISORY_LOCK_KEY]);
    } catch (err) {
      logger.error({ err }, "[scheduler] failed to release advisory lock");
    } finally {
      lockClient.release();
      lockClient = null;
    }
  }
  logger.info("[scheduler] stopped, no in-flight poll cycle remaining");
}