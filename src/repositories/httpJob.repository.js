import { pool } from "../config/db.js";

export async function createJob(job) {
  const query = `
    INSERT INTO http_jobs (
      method,
      url,
      body,
      headers,
      schedule_type,
      run_at,
      cron_expression,
      enabled,
      max_attempts,
      attempts,
      backoff_seconds,
      next_run,
      query_params,
      body_type,
      auth_type,
      auth_config,
      redirect_mode,
      timeout_ms
    )
    VALUES (
      $1,
      $2, 
      $3, 
      $4, 
      $5,
      CASE WHEN $6::bigint IS NULL THEN NULL ELSE to_timestamp($6::bigint) END,
      $7, 
      $8,  
      $9, 
      $10, 
      $11,
      to_timestamp($12::bigint),
      $13,
      $14,
      $15,
      $16,
      $17,
      $18
 )
    RETURNING *;
  `;

  const values = [
    job.method,
    job.url,
    job.body ?? {},
    job.headers ?? {},
    job.schedule_type,
    job.run_at ?? null,
    job.cron_expression ?? null,
    job.enabled ?? true,
    job.max_attempts,
    job.attempts,
    job.backoff_seconds,
    job.next_run,
    job.query_params ?? {},
    job.body_type ?? "json",
    job.auth_type ?? "NONE",
    job.auth_config ?? {},
    job.redirect_mode ?? "follow",
    job.timeout_ms ?? 30000,
  ];

  const { rows } = await pool.query(query, values);
  return rows[0];
}

export async function claimDueJobs(client, limit = 100) {
  const { rows } = await client.query(
    `
    UPDATE http_jobs
    SET locked_at = now()
    WHERE job_id IN (
      SELECT job_id
      FROM http_jobs
      WHERE enabled
        AND next_run IS NOT NULL
        AND next_run <= now()
        AND (locked_at IS NULL OR locked_at < now() - interval '1 minute')
      ORDER BY next_run
      LIMIT $1
      FOR UPDATE SKIP LOCKED
    )
    RETURNING *;
    `,
    [limit]
  );
  return rows;
}
 
export async function markJobScheduled(client, jobId, { nextRun, isRecurring }) {
  if (isRecurring) {
    await client.query(
      `UPDATE http_jobs
       SET next_run = to_timestamp($2::bigint), locked_at = NULL, updated_at = now()
       WHERE job_id = $1`,
      [jobId, nextRun]
    );
  }
}
 
export async function finalizeJobRun(client, jobId, { isRecurring }) {
  if (isRecurring) {
    await client.query(
      `UPDATE http_jobs SET attempts = attempts + 1, updated_at = now() WHERE job_id = $1`,
      [jobId]
    );
  } else {
    await client.query(
      `UPDATE http_jobs
       SET attempts = attempts + 1, next_run = NULL, locked_at = NULL, updated_at = now()
       WHERE job_id = $1`,
      [jobId]
    );
  }
}
 
export async function getJobById(client, jobId) {
  const { rows } = await client.query(
    `SELECT * FROM http_jobs WHERE job_id = $1`,
    [jobId]
  );
  return rows[0] ?? null;
}

export async function findJobById(jobId) {
  const { rows } = await pool.query(
    `SELECT * FROM http_jobs WHERE job_id = $1`,
    [jobId]
  );
  return rows[0] ?? null;
}

const OUTCOME_STATUSES = new Set(["COMPLETED", "FAILED"]);

export async function listJobs({ status, enabled, scheduleType, limit, offset }) {
  const conditions = [];
  const values = [];

  if (status && OUTCOME_STATUSES.has(status)) {
    values.push(status === "COMPLETED" ? "success" : "failed");
    conditions.push(`
      (
        SELECT je.status FROM job_executions je
        WHERE je.job_id = http_jobs.job_id
        ORDER BY je.created_at DESC
        LIMIT 1
      ) = $${values.length}
    `);
  }
  if (typeof enabled === "boolean") {
    values.push(enabled);
    conditions.push(`enabled = $${values.length}`);
  }
  if (scheduleType) {
    values.push(scheduleType);
    conditions.push(`schedule_type = $${values.length}`);
  }

  const where = conditions.length ? `WHERE ${conditions.join(" AND ")}` : "";

  values.push(limit);
  const limitIdx = values.length;
  values.push(offset);
  const offsetIdx = values.length;

  const { rows } = await pool.query(
    `SELECT * FROM http_jobs ${where}
     ORDER BY created_at DESC
     LIMIT $${limitIdx} OFFSET $${offsetIdx}`,
    values
  );

  const { rows: countRows } = await pool.query(
    `SELECT COUNT(*)::int AS count FROM http_jobs ${where}`,
    values.slice(0, conditions.length)
  );

  return { jobs: rows, total: countRows[0].count };
}

export async function updateJob(jobId, fields) {
  const allowedColumns = [
    "method",
    "url",
    "body",
    "headers",
    "max_attempts",
    "backoff_seconds",
    "enabled",
    "next_run",
    "run_at",
    "cron_expression",
    "query_params",
    "body_type",
    "auth_type",
    "auth_config",
    "redirect_mode",
    "timeout_ms",
  ];

  const sets = [];
  const values = [];

  for (const [key, value] of Object.entries(fields)) {
    if (!allowedColumns.includes(key)) continue;
    values.push(value);
    sets.push(`${key} = $${values.length}`);
  }

  if (sets.length === 0) return findJobById(jobId);

  values.push(jobId);
  const { rows } = await pool.query(
    `UPDATE http_jobs
     SET ${sets.join(", ")}, updated_at = now()
     WHERE job_id = $${values.length}
     RETURNING *`,
    values
  );
  return rows[0] ?? null;
}

export async function deleteJob(jobId) {
  const { rows } = await pool.query(
    `DELETE FROM http_jobs WHERE job_id = $1 RETURNING job_id`,
    [jobId]
  );
  return rows[0] ?? null;
}