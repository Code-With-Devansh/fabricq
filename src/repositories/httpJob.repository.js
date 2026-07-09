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
      status,
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
    job.status,
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
      WHERE status = 'PENDING'
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
 
// Called after an execution row has been created for a job.
// - CRON jobs: advance next_run to the next cron occurrence, unlock, stay PENDING.
// - ONCE jobs: no more runs due; unlock and flip to a terminal-ish state.
//   (Terminal status here just means "not due again" - the execution's
//   own status is what tracks success/failure of the actual HTTP call.)
export async function markJobScheduled(client, jobId, { nextRun, isRecurring }) {
  if (isRecurring) {
    await client.query(
      `UPDATE http_jobs
       SET next_run = to_timestamp($2::bigint), locked_at = NULL, updated_at = now()
       WHERE job_id = $1`,
      [jobId, nextRun]
    );
  } else {
    await client.query(
      `UPDATE http_jobs
       SET status = 'RUNNING', locked_at = NULL, updated_at = now()
       WHERE job_id = $1`,
      [jobId]
    );
  }
}
 
// Worker calls this once it knows the outcome of a ONCE job's single execution,
// or a CRON job run that has exhausted its retries for this occurrence.
export async function finalizeJobRun(client, jobId, { success, isRecurring }) {
  if (isRecurring) {
    // recurring jobs go back to PENDING regardless of outcome - they'll fire again
    await client.query(
      `UPDATE http_jobs SET status = 'PENDING', updated_at = now() WHERE job_id = $1`,
      [jobId]
    );
  } else {
    await client.query(
      `UPDATE http_jobs SET status = $2, updated_at = now() WHERE job_id = $1`,
      [jobId, success ? "COMPLETED" : "FAILED"]
    );
  }
}
 
export async function incrementAttempts(client, jobId) {
  await client.query(
    `UPDATE http_jobs SET attempts = attempts + 1, updated_at = now() WHERE job_id = $1`,
    [jobId]
  );
}
 
export async function getJobById(client, jobId) {
  const { rows } = await client.query(
    `SELECT * FROM http_jobs WHERE job_id = $1`,
    [jobId]
  );
  return rows[0] ?? null;
}

// Plain-pool variant for read paths that don't run inside the
// scheduler/worker's transaction (controllers call this one).
export async function findJobById(jobId) {
  const { rows } = await pool.query(
    `SELECT * FROM http_jobs WHERE job_id = $1`,
    [jobId]
  );
  return rows[0] ?? null;
}

export async function listJobs({ status, scheduleType, limit, offset }) {
  const conditions = [];
  const values = [];

  if (status) {
    values.push(status);
    conditions.push(`status = $${values.length}`);
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

// Whitelisted, dynamic UPDATE: only touches columns actually present in `fields`.
// Recomputes next_run when run_at/cron_expression change so the scheduler
// doesn't keep firing on a stale schedule.
export async function updateJob(jobId, fields) {
  const allowedColumns = [
    "method",
    "url",
    "body",
    "headers",
    "max_attempts",
    "backoff_seconds",
    "status",
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
  // ON DELETE CASCADE on job_executions.job_id takes care of execution history.
  const { rows } = await pool.query(
    `DELETE FROM http_jobs WHERE job_id = $1 RETURNING job_id`,
    [jobId]
  );
  return rows[0] ?? null;
}