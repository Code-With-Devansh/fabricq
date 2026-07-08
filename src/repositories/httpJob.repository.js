import { pool } from "../config/db.js";

export async function createJob(job) {
  const query = `
    INSERT INTO http_jobs (
      method,
      url,
      payload,
      headers,
      schedule_type,
      run_at,
      cron_expression,
      status,
      max_attempts,
      attempts,
      backoff_seconds,
      next_run
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
      to_timestamp($12::bigint)
 )
    RETURNING *;
  `;

  const values = [
    job.method,
    job.url,
    job.payload ?? {},
    job.headers ?? {},
    job.schedule_type,
    job.run_at ?? null,
    job.cron_expression ?? null,
    job.status,
    job.max_attempts,
    job.attempts,
    job.backoff_seconds,
    job.next_run,
  ];

  const { rows } = await pool.query(query, values);
  return rows[0];
}