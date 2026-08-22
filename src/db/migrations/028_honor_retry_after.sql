-- retry_max_seconds becomes nullable: NULL means "no ceiling" (applies to
-- every retry_strategy, not just Retry-After honoring below), replacing the
-- previous convention of a very large number as a stand-in for uncapped.
ALTER TABLE http_jobs
    ALTER COLUMN retry_max_seconds DROP NOT NULL,
    ALTER COLUMN retry_max_seconds DROP DEFAULT;

-- valid_retry_max_seconds (>= 0) already passes NULL through un-checked, so
-- no constraint change needed there.

-- Per-job opt-in to honoring an upstream Retry-After header on retryable
-- failures, overriding the configured retry_strategy's computed delay for
-- that one attempt. Defaults false to preserve current behavior.
ALTER TABLE http_jobs
    ADD COLUMN honor_retry_after BOOLEAN NOT NULL DEFAULT false;