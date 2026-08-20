-- Fixes: "insert or update on table execution_outbox violates foreign key
-- constraint execution_outbox_execution_fkey"
--
-- Root cause: job_executions.created_at is TIMESTAMPTZ with no precision
-- limit, so Postgres stores it with microsecond precision (e.g.
-- 12:40:09.425678+00). node-postgres returns timestamptz columns as JS
-- Date objects, which only hold millisecond precision - so the moment
-- `RETURNING created_at` crosses into JS, it silently truncates to
-- 12:40:09.425+00. That truncated value is what scheduler.js/retry.js
-- then send back down for the execution_outbox insert and for the
-- created_at used in WHERE clauses - it no longer matches the
-- microsecond-precision value actually stored in job_executions, so the
-- FK (and, more subtly, the pruning WHERE clauses on markExecutionRunning
-- etc.) silently mismatch.
--
-- First attempt at this fix tried ALTER COLUMN created_at TYPE
-- TIMESTAMPTZ(3), but created_at is the partition key for job_executions
-- and Postgres refuses to change the type of a partition key column at
-- all (independent of whether the new type is compatible). So instead:
-- truncate to millisecond precision AT WRITE TIME, via the column
-- default and a same-truncation on the outbox side. The column itself
-- stays plain TIMESTAMPTZ, but nothing ever writes it a value with more
-- precision than a JS Date can hold, so there's nothing to lose on the
-- round trip.

BEGIN;

ALTER TABLE job_executions
    ALTER COLUMN created_at SET DEFAULT date_trunc('milliseconds', clock_timestamp());

-- Backfill existing rows so old data doesn't retain microsecond precision
-- either - this is an UPDATE of values, not a type change, so it's fine
-- on a partition key column (a row may move partition if truncation ever
-- pushed it across a boundary, which millisecond truncation never does).
UPDATE job_executions
SET created_at = date_trunc('milliseconds', created_at);

UPDATE execution_outbox
SET execution_created_at = date_trunc('milliseconds', execution_created_at);

COMMIT;