CREATE TYPE http_method AS ENUM (
    'GET',
    'POST',
    'PUT',
    'PATCH',
    'DELETE'
);

CREATE TYPE schedule_type AS ENUM (
    'ONCE',
    'CRON'
);

CREATE TYPE job_status AS ENUM (
    'PENDING',
    'RUNNING',
    'RETRYING',
    'COMPLETED',
    'FAILED'
);

CREATE TABLE http_jobs (
    job_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    method http_method NOT NULL,
    url TEXT NOT NULL,

    body JSONB,
    headers JSONB,

    schedule_type schedule_type NOT NULL,

    run_at TIMESTAMPTZ,
    cron_expression TEXT,

    -- Execution state
    status job_status NOT NULL DEFAULT 'PENDING',

    -- Retry policy
    max_attempts INTEGER NOT NULL DEFAULT 3,
    attempts INTEGER NOT NULL DEFAULT 0,
    backoff_seconds INTEGER NOT NULL DEFAULT 60,

    -- Scheduler
    next_run TIMESTAMPTZ NOT NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT valid_schedule CHECK (
        (schedule_type = 'ONCE' AND run_at IS NOT NULL AND cron_expression IS NULL)
        OR
        (schedule_type = 'CRON' AND cron_expression IS NOT NULL AND run_at IS NULL)
    ),

    CONSTRAINT valid_attempts CHECK (
        attempts >= 0
        AND max_attempts > 0
        AND attempts <= max_attempts
    ),

    CONSTRAINT valid_backoff CHECK (
        backoff_seconds >= 0
    )
);