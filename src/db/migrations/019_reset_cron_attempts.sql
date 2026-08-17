-- CRON jobs' `attempts` counter was previously incremented on every failed
-- run with nothing ever resetting it, since retry/backoff exhaustion logic
-- only applies to ONCE jobs (see retry.js, worker.js, recovery.js). It's
-- never read for CRON control flow - the per-run attempt count already
-- lives on job_executions.attempt. This reset is purely cosmetic cleanup
-- of the stale accumulated value; httpJob.repository.js no longer writes
-- to `attempts` for recurring jobs at all, so this isn't required for the
-- `valid_attempts` check constraint violation to stop recurring.
UPDATE http_jobs
SET attempts = 0
WHERE schedule_type = 'CRON'
  AND attempts != 0;
