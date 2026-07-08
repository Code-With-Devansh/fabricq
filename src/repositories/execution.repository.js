import { pool } from "../config/db.js";

export async function createExecution(client, { jobId, attempt, scheduledFor }) {
  const { rows } = await client.query(
    `INSERT INTO executions (job_id, attempt, status, scheduled_for)
     VALUES ($1, $2, 'PENDING', to_timestamp($3::bigint))
     RETURNING *`,
    [jobId, attempt, scheduledFor]
  );
  return rows[0];
}

export async function markExecutionRunning(executionId) {
  const { rows } = await pool.query(
    `UPDATE executions
     SET status = 'RUNNING', started_at = now()
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
    `UPDATE executions
     SET status = $2,
         finished_at = now(),
         response_status = $3,
         response_body = $4,
         error = $5
     WHERE execution_id = $1
     RETURNING *`,
    [executionId, success ? "SUCCESS" : "FAILED", responseStatus, responseBody, error]
  );
  return rows[0];
}

export async function getExecutionWithJob(executionId) {
  const { rows } = await pool.query(
    `SELECT
       e.execution_id, e.job_id, e.attempt, e.status AS execution_status,
       e.scheduled_for,
       j.method, j.url, j.payload, j.headers, j.max_attempts,
       j.backoff_seconds, j.schedule_type
     FROM executions e
     JOIN http_jobs j ON j.job_id = e.job_id
     WHERE e.execution_id = $1`,
    [executionId]
  );
  return rows[0] ?? null;
}

export async function getExecutionHistory(jobId, limit = 50) {
  const { rows } = await pool.query(
    `SELECT * FROM executions WHERE job_id = $1 ORDER BY created_at DESC LIMIT $2`,
    [jobId, limit]
  );
  return rows;
}