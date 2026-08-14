import { pool } from "../config/db.js";

export async function createJob(job) {
  const query = `
    INSERT INTO http_jobs (
      team_id,
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
      timeout_ms,
      retry_strategy,
      retry_multiplier,
      retry_max_seconds,
      redirect_policy
    )
    VALUES (
      $1,
      $2,
      $3,
      $4,
      $5,
      $6,
      CASE WHEN $7::bigint IS NULL THEN NULL ELSE to_timestamp($7::bigint) END,
      $8,
      $9,
      $10,
      $11,
      $12,
      to_timestamp($13::bigint),
      $14,
      $15,
      $16,
      $17,
      $18,
      $19,
      $20,
      $21,
      $22,
      $23::jsonb
 )
    RETURNING *;
  `;

  const values = [
    job.team_id,
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
    job.retry_strategy ?? "FIXED",
    job.retry_multiplier ?? 2,
    job.retry_max_seconds ?? 3600,
    JSON.stringify(
      job.redirect_policy ?? {
        maxRedirects: 10,
        allowCrossOrigin: false,
        allowHttpDowngrade: false,
      }
    ),
  ];

  const { rows } = await pool.query(query, values);
  return rows[0];
}

// --- background infra (scheduler/worker/recovery) -----------------------
// These operate across ALL teams' jobs by design - they're internal claim/
// execution machinery, not user-facing, so they intentionally have no
// team_id filtering. Team scoping is purely an API/authorization concern,
// applied only in the team-scoped functions below.

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

// Clears next_run so the job can't match claimDueJobs' `next_run <= now()`
// filter again while it's mid-flight (queued -> worker -> HTTP call ->
// finalize). This is what makes claiming and scheduling atomic in the
// same transaction actually matter: a ONCE job that hasn't finished yet
// is structurally unreachable by the next poll, regardless of how long
// execution takes or how stale locked_at looks - it's not depending on a
// timing threshold anymore. See scheduler.js for the full rationale.
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
       SET next_run = NULL, locked_at = NULL, updated_at = now()
       WHERE job_id = $1`,
      [jobId]
    );
  }
}

// ONCE job failed but has retries left: worker/recovery no longer compute
// the backoff delay themselves. This just records the attempt and leaves
// next_run NULL - the retry scheduler is the only thing that turns it back
// into a real timestamp (see src/retry/retry.js), based on the job's
// configurable retry_strategy.
export async function markJobFailedAwaitingRetry(client, jobId) {
  await client.query(
    `UPDATE http_jobs
     SET attempts = attempts + 1,
         next_run = NULL,
         locked_at = NULL,
         updated_at = now()
     WHERE job_id = $1`,
    [jobId],
  );
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
       SET attempts = attempts + 1,
           enabled = false, updated_at = now()
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

// --- team-scoped (API-facing) --------------------------------------------
// Every function below takes teamId and filters on it, so a caller can
// never read/modify/delete a job belonging to a different team even if
// they guess a valid job_id.

export async function findJobByIdForTeam(teamId, jobId) {
  const { rows } = await pool.query(
    `SELECT * FROM http_jobs WHERE team_id = $1 AND job_id = $2`,
    [teamId, jobId]
  );
  return rows[0] ?? null;
}

const OUTCOME_STATUSES = new Set(["COMPLETED", "FAILED"]);

export async function listJobsForTeam({
  teamId,
  status,
  enabled,
  scheduleType,
  limit,
  offset,
}) {
  const conditions = ["team_id = $1"];
  const values = [teamId];

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

  const where = `WHERE ${conditions.join(" AND ")}`;

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

export async function updateJobForTeam(teamId, jobId, fields) {
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
    "retry_strategy",
    "retry_multiplier",
    "retry_max_seconds",
    // redirect_policy intentionally excluded from this list - it's jsonb
    // and needs a merge (||), not a plain overwrite, so it's handled
    // separately below.
  ];

  const sets = [];
  const values = [];

  for (const [key, value] of Object.entries(fields)) {
    if (!allowedColumns.includes(key)) continue;
    values.push(value);
    sets.push(`${key} = $${values.length}`);
  }

  if (fields.redirect_policy !== undefined) {
    // Shallow merge: keys present in fields.redirect_policy overwrite,
    // keys absent are left as-is. A caller sending { maxRedirects: 5 }
    // alone must not blow away allowCrossOrigin/allowHttpDowngrade.
    values.push(JSON.stringify(fields.redirect_policy));
    sets.push(`redirect_policy = redirect_policy || $${values.length}::jsonb`);
  }

  if (sets.length === 0) return findJobByIdForTeam(teamId, jobId);

  values.push(teamId);
  const teamIdx = values.length;
  values.push(jobId);
  const jobIdx = values.length;

  const { rows } = await pool.query(
    `UPDATE http_jobs
     SET ${sets.join(", ")}, updated_at = now()
     WHERE team_id = $${teamIdx} AND job_id = $${jobIdx}
     RETURNING *`,
    values
  );
  return rows[0] ?? null;
}

export async function deleteJobForTeam(teamId, jobId) {
  const { rows } = await pool.query(
    `DELETE FROM http_jobs WHERE team_id = $1 AND job_id = $2 RETURNING job_id`,
    [teamId, jobId]
  );
  return rows[0] ?? null;
}
