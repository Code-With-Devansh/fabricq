CREATE TYPE retry_strategy AS ENUM (
    'IMMEDIATE',
    'FIXED',
    'LINEAR',
    'EXPONENTIAL',
    'EXPONENTIAL_JITTER',
    'FIBONACCI'
);

ALTER TABLE http_jobs
    ADD COLUMN retry_strategy retry_strategy NOT NULL DEFAULT 'FIXED',
    ADD COLUMN retry_multiplier NUMERIC NOT NULL DEFAULT 2,
    ADD COLUMN retry_max_seconds INTEGER NOT NULL DEFAULT 3600,
    ADD CONSTRAINT valid_retry_multiplier CHECK (retry_multiplier > 0),
    ADD CONSTRAINT valid_retry_max_seconds CHECK (retry_max_seconds >= 0);

-- backoff_seconds becomes the "base delay" input to whichever retry_strategy
-- is selected (ignored entirely for IMMEDIATE).
