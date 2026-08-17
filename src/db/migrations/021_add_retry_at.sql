-- Retry state moves entirely onto job_executions.
--
-- http_jobs.next_run is a pure schedule cursor.
-- job_executions owns retry state for one logical execution.

ALTER TABLE job_executions
    ADD COLUMN retry_at TIMESTAMPTZ;

CREATE INDEX idx_job_executions_retry_wait
ON job_executions (retry_at)
WHERE status = 'retry_wait';

ALTER TABLE http_jobs
    DROP CONSTRAINT valid_attempts;

ALTER TABLE http_jobs
    DROP COLUMN attempts;