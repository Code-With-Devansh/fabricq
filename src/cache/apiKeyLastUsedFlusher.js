import cron from "node-cron";
import logger from "../config/logger/index.js";
import { drainPendingLastUsed } from "./apiKeyCache.js";
import { touchLastUsedBatch } from "../repositories/apiKey.repository.js";

let flushTask = null;
let flushRunning = false;
let shuttingDown = false;

async function flushOnce() {
  const pending = await drainPendingLastUsed();
  if (pending.size === 0) return;

  try {
    await touchLastUsedBatch([...pending.entries()]);
  } catch (err) {
    // The batch write failed after we already drained the ZSET, so these
    // timestamps are lost for this cycle - acceptable, since
    // last_used_at is a best-effort/informational field, not correctness-
    // critical. Logged for visibility.
    logger.error({ err, count: pending.size }, "[apiKeyLastUsedFlusher] batch write failed");
  }
}

// Every 30s: turns up to N requests/sec of `UPDATE ... last_used_at`
// writes into a single batched statement every 30s.
export function startApiKeyLastUsedFlusher() {
  flushTask = cron.schedule("*/30 * * * * *", async () => {
    if (shuttingDown) return;
    if (flushRunning) {
      logger.warn("[apiKeyLastUsedFlusher] previous flush still running, skipping this tick");
      return;
    }
    flushRunning = true;
    try {
      await flushOnce();
    } finally {
      flushRunning = false;
    }
  });

  logger.info("[apiKeyLastUsedFlusher] started: flushing every 30s");
}

export async function stopApiKeyLastUsedFlusher() {
  shuttingDown = true;
  if (flushTask) flushTask.stop();

  // Final flush so we don't lose up to 30s of last_used_at data on deploy.
  try {
    await flushOnce();
  } catch (err) {
    logger.error({ err }, "[apiKeyLastUsedFlusher] final flush failed");
  }
}
