import { pool } from "../config/db.js";

// Written inside the SAME transaction/savepoint as the execution row it
// belongs to - see migration 016 for why that matters. `client` must be
// the transaction client the caller is already inside, never the pool.
export async function createOutboxEntry(client, { executionId, queueKey, payload }) {
  await client.query(
    `INSERT INTO execution_outbox (execution_id, queue_key, payload)
     VALUES ($1, $2, $3::jsonb)`,
    [executionId, queueKey, JSON.stringify(payload)]
  );
}

// Best-effort fast path: called right after the scheduling transaction
// commits, so most executions get published within the same tick instead
// of waiting for the relay sweep. Only marks published if it's still
// unpublished, so a concurrent relay sweep can't double-count it.
export async function markOutboxPublished(executionId) {
  const { rows } = await pool.query(
    `UPDATE execution_outbox
     SET published_at = now()
     WHERE execution_id = $1 AND published_at IS NULL
     RETURNING execution_id`,
    [executionId]
  );
  return rows.length > 0;
}

// FOR UPDATE SKIP LOCKED lets more than one relay instance run
// concurrently without stepping on each other - each row is claimed by
// exactly one relay at a time, and a relay that's mid-batch never blocks
// another from picking up different rows.
export async function claimUnpublishedOutboxBatch(client, { limit = 100, staleAfter = "5 seconds" } = {}) {
  const { rows } = await client.query(
    `SELECT execution_id, queue_key, payload, publish_attempts
     FROM execution_outbox
     WHERE published_at IS NULL
       AND created_at < now() - $2::interval
     ORDER BY created_at
     LIMIT $1
     FOR UPDATE SKIP LOCKED`,
    [limit, staleAfter]
  );
  return rows;
}

export async function markOutboxPublishedBatch(client, executionIds) {
  if (executionIds.length === 0) return;
  await client.query(
    `UPDATE execution_outbox
     SET published_at = now()
     WHERE execution_id = ANY($1::uuid[])`,
    [executionIds]
  );
}

export async function bumpOutboxAttempts(client, executionIds) {
  if (executionIds.length === 0) return;
  await client.query(
    `UPDATE execution_outbox
     SET publish_attempts = publish_attempts + 1
     WHERE execution_id = ANY($1::uuid[])`,
    [executionIds]
  );
}
