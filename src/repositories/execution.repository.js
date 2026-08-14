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


export async function getExecutionById(executionId) {
  const { rows } = await pool.query(
    `SELECT * FROM job_executions WHERE execution_id = $1`,
    [executionId]
  );
  return rows[0] ?? null;
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