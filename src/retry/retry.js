import { pool } from "../config/db.js";
import logger from "../config/logger/index.js";
import { EXECUTION_QUEUE_KEY } from "../scheduler/scheduler.js";
import { claimDueRetries } from "../repositories/execution.repository.js";
import { upsertRetryOutboxEntry } from "../repositories/outbox.repository.js";
import { publishOutboxEntryNow } from "../outbox/outbox.js";

// Reconstructs the same shape worker.js's handleExecution expects (the
// http_jobs fields it needs to actually re-run the call, plus
// execution_id/attempt) from a claimDueRetries row.
function buildRetryPayload(row) {
  return {
    job_id: row.hj_job_id,
    execution_id: row.execution_id,
    created_at: row.created_at,
    attempt: row.attempt,
    schedule_type: row.schedule_type,
    method: row.method,
    url: row.url,
    body: row.body,
    headers: row.headers,
    query_params: row.query_params,
    body_type: row.body_type,
    auth_type: row.auth_type,
    auth_config: row.auth_config,
    redirect_mode: row.redirect_mode,
    redirect_policy: row.redirect_policy,
    timeout_ms: row.timeout_ms,
    max_attempts: row.max_attempts,
    backoff_seconds: row.backoff_seconds,
    retry_strategy: row.retry_strategy,
    retry_multiplier: row.retry_multiplier,
    retry_max_seconds: row.retry_max_seconds,
    honor_retry_after: row.honor_retry_after,
  };
}

// Retry state lives entirely on job_executions now (see migration 020) -
// there's no separate intake signal to listen for. This process just
// polls for rows sitting in retry_wait whose retry_at has passed, exactly
// the same SKIP LOCKED claim pattern as the scheduler's claimDueJobs, and
// republishes each one through the normal outbox path so the worker picks
// it up with no retry-specific handling of its own.
export async function runRetrySweep() {
  const client = await pool.connect();
  let claimed = [];

  try {
    await client.query("BEGIN");
    claimed = await claimDueRetries(client);

    if (claimed.length === 0) {
      await client.query("COMMIT");
      client.release();
      return;
    }

    logger.info({ count: claimed.length }, "[retry] claimed due retries");

    // upsertRetryOutboxEntry writes inside this SAME transaction, so a
    // crash between here and COMMIT rolls the claim back too - the row
    // just goes back to retry_wait and gets picked up on the next sweep,
    // never "claimed but never republished".
    for (const row of claimed) {
      await upsertRetryOutboxEntry(client, {
        executionId: row.execution_id,
        executionCreatedAt: row.created_at,
        queueKey: EXECUTION_QUEUE_KEY,
        payload: buildRetryPayload(row),
      });
    }

    await client.query("COMMIT");
  } catch (err) {
    await client.query("ROLLBACK");
    logger.error({ err }, "[retry] failed to claim/requeue due retries, batch rolled back");
    client.release();
    return;
  }

  client.release();

  // Same fast-path-then-relay-backstop shape as the scheduler: publish
  // immediately for low latency, but if this fails or the process dies
  // right here, the outbox row is already durable and the relay sweep
  // will publish it on its next tick - nothing is lost.
  for (const row of claimed) {
    try {
      // Per-attempt dedupe key (see outbox.js) - this execution_id was
      // already published once for its previous attempt, so reusing the
      // bare execution_id as the dedupe key would make this republish a
      // silent no-op.
      const published = await publishOutboxEntryNow({
        executionId: row.execution_id,
        queueKey: EXECUTION_QUEUE_KEY,
        dedupeKey: `${row.execution_id}:${row.attempt}`,
        payload: buildRetryPayload(row),
      });
      logger.info(
        { executionId: row.execution_id, attempt: row.attempt, published },
        published ? "[retry] retry requeued" : "[retry] retry requeued, deferred to outbox relay",
      );
    } catch (err) {
      logger.error(
        { err, executionId: row.execution_id },
        "[retry] fast-path publish failed, retry left for outbox relay",
      );
    }
  }
}