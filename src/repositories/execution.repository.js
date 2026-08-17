import { pool } from "../config/db.js";

export async function createExecution(client, { jobId, attempt, scheduledFor }) {
  const { rows } = await client.query(
    `INSERT INTO job_executions (job_id, attempt, status, scheduled_time)
     VALUES ($1, $2, 'queued', to_timestamp($3::bigint))
     RETURNING *`,
    [jobId, attempt, scheduledFor]
  );
  return rows[0];
}

export async function markExecutionRunning(executionId) {
  const { rows } = await pool.query(
    `UPDATE job_executions
     SET status = 'running', started_at = now()
     WHERE execution_id = $1
     RETURNING *`,
    [executionId]
  );
  return rows[0];
}

export async function completeExecution(
  client,
  executionId,
  {
    success,
    responseStatus = null,
    responseBody = null,
    error = null,
    redirectOccurred = false,
    redirectCount = 0,
    redirects = [],
  },
  WORKER_ID
) {
  const { rows } = await client.query(
    `UPDATE job_executions
     SET status = $2,
         finished_at = now(),
         response_status = $3,
         response = $4,
         error = $5,
         worker_id = $6,
         redirect_occurred = $7,
         redirect_count = $8,
         redirects = $9::jsonb
     WHERE execution_id = $1
     RETURNING *`,
    [
      executionId,
      success ? "success" : "failed",
      responseStatus,
      responseBody === null ? null : JSON.stringify({ body: responseBody }),
      error === null ? null : JSON.stringify({ message: error }),
      WORKER_ID,
      redirectOccurred,
      redirectCount,
      JSON.stringify(redirects),
    ]
  );
  return rows[0];
}


// --- batched write-behind path (used only by the merger process) ---------
// These replace per-execution markExecutionRunning/completeExecution calls
// with a single multi-row statement per flush. Order within each array
// must line up positionally - callers build these from the same list.

export async function markExecutionsRunningBatch(client, executionIds) {
  if (executionIds.length === 0) return;
  await client.query(
    `UPDATE job_executions je
     SET status = 'running', started_at = now()
     FROM (SELECT unnest($1::uuid[]) AS execution_id) x
     WHERE je.execution_id = x.execution_id
       AND je.status = 'queued'`, // don't clobber a later 'completed' event that flushed first
    [executionIds]
  );
}

export async function completeExecutionsBatch(client, rows) {
  if (rows.length === 0) return;

  const executionIds = [];
  const statuses = [];
  const responseStatuses = [];
  const responses = [];
  const errors = [];
  const workerIds = [];
  const redirectOccurreds = [];
  const redirectCounts = [];
  const redirectsArr = [];

  for (const r of rows) {
    executionIds.push(r.executionId);
    statuses.push(r.success ? "success" : "failed");
    responseStatuses.push(r.responseStatus ?? null);
    responses.push(
      r.responseBody === null || r.responseBody === undefined
        ? null
        : JSON.stringify({ body: r.responseBody })
    );
    errors.push(r.error === null || r.error === undefined ? null : JSON.stringify({ message: r.error }));
    workerIds.push(r.workerId ?? null);
    redirectOccurreds.push(r.redirectOccurred ?? false);
    redirectCounts.push(r.redirectCount ?? 0);
    redirectsArr.push(JSON.stringify(r.redirects ?? []));
  }

  await client.query(
    `UPDATE job_executions je
     SET status = x.status,
         finished_at = now(),
         response_status = x.response_status,
         response = x.response::jsonb,
         error = x.error::jsonb,
         worker_id = x.worker_id,
         redirect_occurred = x.redirect_occurred,
         redirect_count = x.redirect_count,
         redirects = x.redirects::jsonb
     FROM (
       SELECT * FROM unnest(
         $1::uuid[], $2::execution_status[], $3::int[], $4::text[],
         $5::text[], $6::text[], $7::boolean[], $8::int[], $9::text[]
       ) AS t(execution_id, status, response_status, response, error,
              worker_id, redirect_occurred, redirect_count, redirects)
     ) x
     WHERE je.execution_id = x.execution_id`,
    [
      executionIds,
      statuses,
      responseStatuses,
      responses,
      errors,
      workerIds,
      redirectOccurreds,
      redirectCounts,
      redirectsArr,
    ]
  );
}

export async function getExecutionById(executionId) {
  const { rows } = await pool.query(
    `SELECT * FROM job_executions WHERE execution_id = $1`,
    [executionId]
  );
  return rows[0] ?? null;
}

// --- retry state (owned entirely by job_executions - see migration 020) --

// Mutates the SAME execution row in place rather than creating a new one:
// one logical execution, several tries. attempt is bumped here (not by the
// caller) so it's atomic with the status/retry_at flip - no window where a
// concurrent reader sees retry_wait with a stale attempt number.
export async function markExecutionRetryWait(client, executionId, { retryAt }) {
  const { rows } = await client.query(
    `UPDATE job_executions
     SET status = 'retry_wait',
         retry_at = $2,
         attempt = attempt + 1
     WHERE execution_id = $1
     RETURNING *`,
    [executionId, retryAt]
  );
  return rows[0] ?? null;
}

// Claimed rows flip straight to 'queued' in the same statement - from here
// they're indistinguishable from a freshly-scheduled execution, so the
// worker's normal pickup/running/complete path handles them with no
// retry-specific branching. Joins http_jobs for the request config needed
// to actually re-run the call (url, method, headers, retry policy, ...).
export async function claimDueRetries(client, limit = 100) {
  const { rows } = await client.query(
    `WITH due AS (
       SELECT execution_id
       FROM job_executions
       WHERE status = 'retry_wait'
         AND retry_at <= now()
       ORDER BY retry_at
       LIMIT $1
       FOR UPDATE SKIP LOCKED
     )
     UPDATE job_executions je
     SET status = 'queued', retry_at = NULL
     FROM due, http_jobs hj
     WHERE je.execution_id = due.execution_id
       AND hj.job_id = je.job_id
     RETURNING je.*, hj.method, hj.url, hj.body, hj.headers, hj.query_params,
               hj.body_type, hj.auth_type, hj.auth_config, hj.redirect_mode,
               hj.redirect_policy, hj.timeout_ms, hj.schedule_type,
               hj.max_attempts, hj.backoff_seconds, hj.retry_strategy,
               hj.retry_multiplier, hj.retry_max_seconds, hj.enabled,
               hj.job_id AS hj_job_id`,
    [limit]
  );
  return rows;
}

// Joins through http_jobs to enforce team ownership - an execution has
// no team_id of its own (see migration 015 notes), so authorization has
// to go through the job it belongs to.
export async function getExecutionByIdForTeam(teamId, executionId) {
  const { rows } = await pool.query(
    `SELECT je.* FROM job_executions je
     JOIN http_jobs hj ON hj.job_id = je.job_id
     WHERE je.execution_id = $1 AND hj.team_id = $2`,
    [executionId, teamId]
  );
  return rows[0] ?? null;
}

export async function getExecutionHistory(jobId, { limit = 50, offset = 0 } = {}) {
  const { rows } = await pool.query(
    `SELECT * FROM job_executions WHERE job_id = $1 ORDER BY created_at DESC LIMIT $2 OFFSET $3`,
    [jobId, limit, offset]
  );
  const { rows: countRows } = await pool.query(
    `SELECT COUNT(*)::int AS count FROM job_executions WHERE job_id = $1`,
    [jobId]
  );
  return { executions: rows, total: countRows[0].count };
}