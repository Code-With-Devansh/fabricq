-- Retry state moves entirely onto job_executions.
-- Add the new execution state first so it is committed before being used.

ALTER TYPE execution_status
ADD VALUE IF NOT EXISTS 'retry_wait';