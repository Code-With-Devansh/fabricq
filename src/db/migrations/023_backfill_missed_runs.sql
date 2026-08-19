-- Fixes the cron-drift bug: when a CRON job's interval is shorter than
-- the scheduler's poll interval (or the scheduler falls behind/was down),
-- the old computeNextRunEpoch only ever advanced next_run by a single
-- tick per poll, no matter how many ticks were actually due. That meant
-- next_run permanently fell further behind "now" every poll cycle and
-- the job was silently downsampled to ~1 execution per poll forever.
--
-- Fix is a per-job policy, defaulting to the safe/non-surprising choice:
--
--   backfill_on_missed_run = false (default): "skip-ahead". Fire exactly
--   one execution for the currently-due tick, then advance next_run past
--   "now" (not just one .next() step) so it stops drifting. Sub-poll-
--   interval crons get downsampled to the poll cadence, but predictably,
--   without drift and without silently missing scheduling for a bad
--   reason.
--
--   backfill_on_missed_run = true: create one execution per missed tick
--   between the old next_run and now, each with its own real
--   scheduled_time (not "now"), capped per poll by max_catchup_per_poll.
--   This is opt-in because backfilling re-runs real side-effecting HTTP
--   calls - fine for a heartbeat ping, dangerous for "charge this card"
--   after an hour of downtime.
--
-- max_catchup_per_poll is nullable (NULL = use the system default,
-- currently 100) and hard-capped at 100 regardless of what a job
-- requests, so a single overdue job can never demand an unbounded burst
-- in one poll - see scheduler.js's queue-depth budget for the other half
-- of that protection.
ALTER TABLE http_jobs
    ADD COLUMN backfill_on_missed_run BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN max_catchup_per_poll INTEGER,
    ADD CONSTRAINT valid_max_catchup_per_poll CHECK (
        max_catchup_per_poll IS NULL
        OR max_catchup_per_poll BETWEEN 1 AND 100
    );