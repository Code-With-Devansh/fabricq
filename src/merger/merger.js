import redis from "../config/redis.js";
import { pool } from "../config/db.js";
import logger from "../config/logger/index.js";
import {
  STREAM_KEY,
  GROUP_NAME,
  ensureConsumerGroup,
  clearExecutionStatus,
} from "../streams/executionResults.js";
import {
  markExecutionsRunningBatch,
  completeExecutionsBatch,
} from "../repositories/execution.repository.js";

const CONSUMER_ID = `merger:${process.pid}`;
const BATCH_SIZE = 500; // max entries pulled per XREADGROUP
const BLOCK_MS = 200; // how long to wait for entries before flushing whatever we have
const CLAIM_IDLE_MS = 30_000; // an entry pending this long belongs to a dead consumer
const CLAIM_INTERVAL_MS = 15_000;

let shuttingDown = false;
let loopPromise = null;

function parseEntry([id, fields]) {
  const obj = {};
  for (let i = 0; i < fields.length; i += 2) obj[fields[i]] = fields[i + 1];
  let payload = {};
  try {
    payload = JSON.parse(obj.payload ?? "{}");
  } catch {
    // malformed payload - still ack it below so it doesn't block the group forever
  }
  return { id, executionId: obj.execution_id, type: obj.type, payload };
}

// Collapse a batch to the LATEST event per execution_id for each event
// type. If a worker emitted both "running" and "completed" for the same
// execution within one flush window (fast HTTP job), only "completed"
// matters - applying "running" after it would incorrectly reset status.
function reduceBatch(entries) {
  const runningByExec = new Map();
  const completedByExec = new Map();

  for (const entry of entries) {
    if (entry.type === "running") {
      runningByExec.set(entry.executionId, entry);
    } else if (entry.type === "completed") {
      completedByExec.set(entry.executionId, entry);
      // a completed event supersedes any running event for the same id
      runningByExec.delete(entry.executionId);
    }
  }

  return { runningByExec, completedByExec };
}

async function flush(entries) {
  if (entries.length === 0) return;

  const { runningByExec, completedByExec } = reduceBatch(entries);

  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    if (runningByExec.size > 0) {
      await markExecutionsRunningBatch(client, [...runningByExec.keys()]);
    }
    if (completedByExec.size > 0) {
      const rows = [...completedByExec.values()].map((e) => ({
        executionId: e.executionId,
        ...e.payload,
      }));
      await completeExecutionsBatch(client, rows);
    }

    await client.query("COMMIT");
  } catch (err) {
    await client.query("ROLLBACK");
    logger.error({ err, count: entries.length }, "[merger] batch flush failed, entries left pending for retry");
    throw err; // don't XACK - let it get redelivered / claimed
  } finally {
    client.release();
  }

  if (completedByExec.size > 0) {
    await Promise.all(
      [...completedByExec.keys()].map((executionId) =>
        clearExecutionStatus(executionId).catch((err) =>
          logger.warn({ err, executionId }, "[merger] failed to clear execution status entry")
        )
      )
    );
  }

  await redis.xack(STREAM_KEY, GROUP_NAME, ...entries.map((e) => e.id));
}

// Picks up entries claimed by a consumer that died mid-flush (crash between
// XREADGROUP and XACK). Same idea as recovery.js's heartbeat sweep, just
// for the merger's own consumer group instead of worker processing lists.
async function claimStaleEntries() {
  try {
    const [, claimed] = await redis.xautoclaim(
      STREAM_KEY,
      GROUP_NAME,
      CONSUMER_ID,
      CLAIM_IDLE_MS,
      "0",
      "COUNT",
      BATCH_SIZE
    );
    if (claimed.length === 0) return;
    logger.warn({ count: claimed.length }, "[merger] reclaimed stale pending entries");
    await flush(claimed.map(parseEntry));
  } catch (err) {
    logger.error({ err }, "[merger] failed to claim stale entries");
  }
}

async function runLoop() {
  await ensureConsumerGroup();

  let lastClaimCheck = Date.now();

  while (!shuttingDown) {
    if (Date.now() - lastClaimCheck > CLAIM_INTERVAL_MS) {
      await claimStaleEntries();
      lastClaimCheck = Date.now();
    }

    let result;
    try {
      result = await redis.xreadgroup(
        "GROUP",
        GROUP_NAME,
        CONSUMER_ID,
        "COUNT",
        BATCH_SIZE,
        "BLOCK",
        BLOCK_MS,
        "STREAMS",
        STREAM_KEY,
        ">"
      );
    } catch (err) {
      logger.error({ err }, "[merger] xreadgroup failed, backing off");
      await new Promise((r) => setTimeout(r, 1000));
      continue;
    }

    if (!result) continue; // block timed out, nothing new

    const [[, rawEntries]] = result;
    const entries = rawEntries.map(parseEntry);

    try {
      await flush(entries);
    } catch {
      // already logged in flush(); loop continues, entries stay pending
      // for the next claimStaleEntries() pass.
    }
  }
}

export function startMerger() {
  loopPromise = runLoop().catch((err) => {
    logger.fatal({ err }, "[merger] loop crashed");
    process.exit(1);
  });
  logger.info({ consumerId: CONSUMER_ID }, "[merger] started");
}

export async function stopMerger() {
  shuttingDown = true;
  if (loopPromise) await loopPromise;
  logger.info("[merger] stopped");
}