import { pool } from "../config/db.js";
import logger from "../config/logger/index.js";
import { publishOutboxEntryNow } from "./outbox.js";

// Shared by the scheduler poll (scheduler.js) and the retry sweep
// (retry.js) - both claim rows under SKIP LOCKED, do a same-transaction
// outbox write, commit, then fast-publish to Redis as a backstop-covered
// low-latency path. This owns the transaction plumbing; each caller keeps
// its own claim/build logic in `workFn`.
//
// `workFn(client)` runs inside BEGIN/COMMIT and returns whatever the
// caller wants back (e.g. items to fast-publish, plus any extra bookkeeping
// like a claimed-count). On any error the transaction is rolled back and
// `emptyValue` is returned instead - nothing was left claimed-only-in-Postgres
// with no outbox row, since the write and the claim share one transaction.
export async function runInClaimTransaction(label, workFn, emptyValue) {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const result = await workFn(client);
    await client.query("COMMIT");
    return result;
  } catch (err) {
    await client.query("ROLLBACK");
    logger.error({ err }, `[${label}] failed to claim/write, batch rolled back`);
    return emptyValue;
  } finally {
    client.release();
  }
}

// The transaction already committed, so every item here is durably
// recorded in Postgres with a matching execution_outbox row (published_at
// still NULL). This loop is just the fast path: publish to Redis
// immediately so executions/retries don't sit around waiting for the
// relay's next tick. If this fails, or the process crashes right here,
// nothing is lost - the outbox row is still there and the outbox relay
// will publish it on its next sweep.
//
// `toPublishArgs(item)` maps a claimed item to publishOutboxEntryNow's
// input. `describe(item)` returns the fields to log alongside the
// outcome. `itemLabel`/`verb` reproduce each caller's existing wording
// ("execution queued" / "retry requeued") rather than forcing one generic
// phrasing on both.
export async function publishClaimedBatch(label, items, toPublishArgs, describe, { itemLabel, verb }) {
  for (const item of items) {
    try {
      const published = await publishOutboxEntryNow(toPublishArgs(item));
      logger.info(
        { ...describe(item), published },
        published
          ? `[${label}] ${itemLabel} ${verb}`
          : `[${label}] ${itemLabel} ${verb}, deferred to outbox relay`
      );
    } catch (err) {
      logger.error(
        { err, ...describe(item) },
        `[${label}] fast-path publish failed, ${itemLabel} left for outbox relay`
      );
    }
  }
}