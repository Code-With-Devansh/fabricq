import { pool } from "../config/db.js";
import { AppError } from "../Error/appError.js";

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
      honor_retry_after,
      redirect_policy,
      backfill_on_missed_run,
      max_catchup_per_poll
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
      to_timestamp($12::bigint),
      $13,
      $14,
      $15,
      $16,
      $17,
      $18,
      $19,
      $20,
      $21,
      $22,
      $23::jsonb,
      $24,
      $25
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
    // No ?? fallback here: the create schema already defaults this to 3600
    // when omitted, so by this point it's either that default or an
    // explicit null (uncapped) from the caller - coalescing would silently
    // turn "explicitly uncapped" back into 3600.
    job.retry_max_seconds,
    job.honor_retry_after ?? false,
    JSON.stringify(
      job.redirect_policy ?? {
        maxRedirects: 10,
        allowCrossOrigin: false,
        allowHttpDowngrade: false,
      }
    ),
    job.backfill_on_missed_run ?? false,
    job.max_catchup_per_poll ?? null,
  ];

  const { rows } = await pool.query(query, values);
  return rows[0];
}

// --- background infra (scheduler/worker/recovery) -----------------------
// These operate across ALL teams' jobs by design - they're internal claim/
// execution machinery, not user-facing, so they intentionally have no
// team_id filtering. Team scoping is purely an API/authorization concern,
// applied only in the team-scoped functions below.

// Configurable via SCHEDULER_BATCH_SIZE so the batch size can be tuned
// independently of a code deploy once the claim->work path is fast
// (batched writes below keep lock-hold time roughly flat as this grows).
export const DEFAULT_CLAIM_LIMIT = Number(process.env.SCHEDULER_BATCH_SIZE) || 100;

export async function claimDueJobs(client, limit = DEFAULT_CLAIM_LIMIT) {
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
        AND locked_at IS NULL
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
// Batched version of markJobScheduled for the scheduler's per-tick claim
// batch - see execution.repository.js's createExecutionBatch for the
// round-trip rationale. `entries` is
// [{ jobId, nextRun (epoch seconds or null), isRecurring }, ...].
// nextRun must be null for ONCE jobs (isRecurring: false); the CASE keys
// off isRecurring rather than trusting nextRun's nullness alone, so a
// caller bug can't accidentally leave a CRON job's next_run cleared.
export async function markJobScheduledBatch(client, entries) {
  if (entries.length === 0) return;
  const jobIds = entries.map((e) => e.jobId);
  const nextRuns = entries.map((e) => e.nextRun);
  const isRecurringFlags = entries.map((e) => e.isRecurring);

  await client.query(
    `UPDATE http_jobs AS h
     SET next_run = CASE WHEN d.is_recurring THEN to_timestamp(d.next_run::bigint) ELSE NULL END,
         locked_at = NULL,
         updated_at = now()
     FROM UNNEST($1::uuid[], $2::bigint[], $3::bool[]) AS d(job_id, next_run, is_recurring)
     WHERE h.job_id = d.job_id`,
    [jobIds, nextRuns, isRecurringFlags]
  );
}

// Releases the claim (locked_at) on jobs that were claimed by claimDueJobs
// but excluded from this tick's batch. Two distinct reasons land here:
//   - "invalid_schedule": a malformed cron_expression that fails
//     computeNextRunEpoch before any write happens.
//   - "queue_depth_budget": the job's required executions (1 for
//     skip-ahead, up to max_catchup_per_poll for backfill) didn't fit in
//     this poll's remaining EXECUTION_QUEUE_KEY budget - not a bad job,
//     just backpressure. See scheduler.js.
// Without this, a released-but-never-retried job stays claimed forever:
// claimDueJobs' WHERE clause filters on locked_at IS NULL, and nothing
// else ever clears it, so the job silently drops out of scheduling until
// someone notices. next_run is left untouched (invalid_schedule case) or
// deliberately unadvanced (queue_depth_budget case) so the job is still
// "due" and gets reclaimed on the very next poll rather than disappearing.
// `reason` is accepted purely for structured logging by the caller - it's
// not persisted, since locked_at has no room for it and there's no
// standing need to query "why was this released" after the fact.
export async function releaseJobClaims(client, jobIds, reason = "unspecified") {
  if (jobIds.length === 0) return;
  await client.query(
    `UPDATE http_jobs
     SET locked_at = NULL
     WHERE job_id = ANY($1::uuid[])`,
    [jobIds]
  );
  return { jobIds, reason };
}

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

// ONCE job's execution has fully resolved (succeeded, or failed with no
// retries left) - it never runs again, so disable it. CRON jobs need no
// equivalent call: next_run is a pure schedule cursor that already
// advanced independently at claim time (see scheduler.js), and doesn't
// care how this execution's retries turned out. Retry state itself lives
// entirely on job_executions now (see migration 020) - http_jobs has
// nothing left to record when an execution resolves.
export async function disableJob(client, jobId) {
  await client.query(
    `UPDATE http_jobs SET enabled = false, updated_at = now() WHERE job_id = $1`,
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

// Whitelist of sortable columns - sortBy is interpolated directly into the
// ORDER BY clause (Postgres doesn't allow column names as bind params), so
// this must stay a closed set validated against, never passed through raw.
const SORTABLE_COLUMNS = new Set(["created_at", "updated_at", "next_run"]);

export async function listJobsForTeam({
  teamId,
  status,
  enabled,
  scheduleType,
  limit,
  offset,
  sortBy = "created_at",
  sortDir = "desc",
}) {
  if (!SORTABLE_COLUMNS.has(sortBy)) {
    throw new AppError(`Invalid sort column: ${sortBy}`, 400);
  }
  const direction = sortDir === "asc" ? "ASC" : "DESC";
  const conditions = ["team_id = $1"];
  const values = [teamId];

  if (status && OUTCOME_STATUSES.has(status)) {
    // "failed" covers every terminal failure status: the legacy plain
    // 'failed' value on pre-migration-022 rows, plus the two causes new
    // rows are written with (see classifyFailure.js / migration 022).
    const matchingStatuses =
      status === "COMPLETED"
        ? ["success"]
        : ["failed", "failed_permanent", "failed_max_retries"];
    values.push(matchingStatuses);
    conditions.push(`
      (
        SELECT je.status FROM job_executions je
        WHERE je.job_id = http_jobs.job_id
        ORDER BY je.created_at DESC
        LIMIT 1
      ) = ANY($${values.length}::execution_status[])
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

  // Tie-break on job_id so rows with an identical sortBy value (e.g. many
  // jobs sharing a next_run tick) still come back in a stable order across
  // pages instead of shuffling between requests.
  const { rows } = await pool.query(
    `SELECT * FROM http_jobs ${where}
     ORDER BY ${sortBy} ${direction}, job_id ${direction}
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
    "honor_retry_after",
    "backfill_on_missed_run",
    "max_catchup_per_poll",
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