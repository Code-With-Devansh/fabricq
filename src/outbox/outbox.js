import redis from "../config/redis.js";
import { pool } from "../config/db.js";
import logger from "../config/logger/index.js";
import {
  claimUnpublishedOutboxBatch,
  markOutboxPublishedBatch,
  markOutboxPublished,
  bumpOutboxAttempts,
} from "../repositories/outbox.repository.js";

// Dedup set TTL - long enough to cover any realistic gap between the
// scheduler's fast-path push and the relay sweep catching the same row,
// short enough not to leak memory. A published execution_id staying in
// the set for an hour after the fact is harmless; the entry is only ever
// consulted while the row could still be unpublished.
const DEDUPE_TTL_SECONDS = 60 * 60;
const PUBLISHED_SET_PREFIX = "fabricq:outbox:published:";

// Cap on stream length - approximate trim (~) so it doesn't have to
// examine every entry on every XADD. Generous enough to comfortably
// outlive any realistic backlog between workers/recovery and the
// consumer group's oldest unacked entry.
const STREAM_MAXLEN = 100_000;

// SADD + XADD atomically in one round trip so there's no window where
// two callers (the scheduler's immediate push and a relay sweep racing
// it) could both see "not yet marked published" and both XADD - one of
// them would win the SADD, the other's XADD gets skipped entirely.
const PUBLISH_SCRIPT = `
local dedupeKey = KEYS[1]
local queueKey = KEYS[2]
local executionId = ARGV[1]
local payload = ARGV[2]
local ttl = tonumber(ARGV[3])
local maxlen = ARGV[4]

local added = redis.call("SET", dedupeKey, "1", "NX", "EX", ttl)
if added then
  redis.call("XADD", queueKey, "MAXLEN", "~", maxlen, "*", "payload", payload)
  return 1
end
return 0
`;

// Returns true if this call actually pushed (or a prior call already
// did and this one is a harmless no-op) - i.e. "the caller can safely
// mark this row published". Returns false only on a Redis-level failure,
// in which case the row is left unpublished for the next sweep to retry.
//
// dedupeKey defaults to the execution_id, which is correct for a normal
// first-time schedule (one execution_id is only ever published once).
// Retries reuse the SAME execution_id (see migration 020 - a retry
// mutates the execution row in place rather than creating a new one), so
// the retry scheduler passes `${executionId}:${attempt}` instead - a
// fresh dedupe key per attempt. Without that, the second, third, etc.
// retry of the same execution would hit the still-live dedupe entry from
// attempt 1 and silently never get pushed.
async function publishOne({ executionId, queueKey, payload, dedupeKey = executionId }) {
  try {
    await redis.eval(
      PUBLISH_SCRIPT,
      2,
      `${PUBLISHED_SET_PREFIX}${dedupeKey}`,
      queueKey,
      executionId,
      JSON.stringify(payload),
      DEDUPE_TTL_SECONDS,
      STREAM_MAXLEN
    );
    return true;
  } catch (err) {
    logger.error({ err, executionId }, "[outbox] publish failed, will retry on next sweep");
    return false;
  }
}

// Called by the scheduler right after its transaction commits. Best
// effort, low latency - if it fails or the process dies here too,
// nothing is lost, because the row is already durably in
// execution_outbox with published_at still NULL and the relay sweep
// (publishPendingExecutions) will pick it up.
export async function publishOutboxEntryNow({ executionId, queueKey, payload, dedupeKey }) {
  const ok = await publishOne({ executionId, queueKey, payload, dedupeKey });
  if (!ok) return false;

  return markOutboxPublished(executionId);
}

// The durability backstop. Runs on a short interval (see outbox/index.js)
// and catches anything the fast path missed - crashed before Redis push,
// Redis was down, process died mid-request, etc. staleAfter gives the
// fast path a head start so the sweep isn't racing every single row.
export async function publishPendingExecutions({ limit = 100, staleAfter = "3 seconds" } = {}) {
  const client = await pool.connect();
  let claimed = [];

  try {
    await client.query("BEGIN");
    claimed = await claimUnpublishedOutboxBatch(client, { limit, staleAfter });
    // Row locks from FOR UPDATE SKIP LOCKED are held until COMMIT, which
    // is what actually prevents a second concurrent sweep from claiming
    // the same rows - so the transaction has to stay open across the
    // Redis calls below, not just the SELECT.
    if (claimed.length === 0) {
      await client.query("COMMIT");
      return;
    }

    const published = [];
    const failed = [];
    for (const row of claimed) {
      const ok = await publishOne({
        executionId: row.execution_id,
        queueKey: row.queue_key,
        payload: row.payload,
      });
      (ok ? published : failed).push(row.execution_id);
    }

    if (published.length > 0) {
      await markOutboxPublishedBatch(client, published);
      logger.warn(
        { count: published.length },
        "[outbox] relay published executions the fast path missed"
      );
    }
    if (failed.length > 0) {
      await bumpOutboxAttempts(client, failed);
      logger.error({ count: failed.length }, "[outbox] relay failed to publish, will retry");
    }

    await client.query("COMMIT");
  } catch (err) {
    await client.query("ROLLBACK");
    logger.error({ err }, "[outbox] sweep failed, batch left unpublished for next tick");
  } finally {
    client.release();
  }
}