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
  executionId,
  { success, responseStatus = null, responseBody = null, error = null }
) {
  const { rows } = await pool.query(
    `UPDATE job_executions
     SET status = $2,
         finished_at = now(),
         response_status = $3,
         response = $4,
         error = $5
     WHERE execution_id = $1
     RETURNING *`,
    [
      executionId,
      success ? "success" : "failed",
      responseStatus,
      responseBody === null ? null : JSON.stringify({ body: responseBody }),
      error === null ? null : JSON.stringify({ message: error }),
    ]
  );
  return rows[0];
}

export async function getExecutionWithJob(executionId) {
  const { rows } = await pool.query(
    `SELECT
       e.execution_id, e.job_id, e.attempt, e.status AS execution_status,
       e.scheduled_time,
       j.method, j.url, j.body AS payload, j.headers, j.max_attempts,
       j.backoff_seconds, j.schedule_type
     FROM job_executions e
     JOIN http_jobs j ON j.job_id = e.job_id
     WHERE e.execution_id = $1`,
    [executionId]
  );
  return rows[0] ?? null;
}

export async function getExecutionById(executionId) {
  const { rows } = await pool.query(
    `SELECT * FROM job_executions WHERE execution_id = $1`,
    [executionId]
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