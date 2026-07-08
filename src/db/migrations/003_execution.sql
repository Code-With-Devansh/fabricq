CREATE TYPE execution_status AS ENUM (
    'queued',
    'running',
    'success',
    'failed'
);

CREATE TABLE job_executions (
    execution_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    job_id UUID NOT NULL
        REFERENCES http_jobs(job_id)
        ON DELETE CASCADE,

    status execution_status NOT NULL DEFAULT 'queued',

    scheduled_time TIMESTAMPTZ NOT NULL,
    locked_at TIMESTAMPTZ,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,

    worker_id TEXT,
    attempt INTEGER NOT NULL DEFAULT 1,

    response_status INTEGER,
    response JSONB,
    error JSONB,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_job_executions_job
ON job_executions(job_id);

CREATE INDEX idx_job_executions_status
ON job_executions(status);

CREATE INDEX idx_http_jobs_due
ON http_jobs(next_run)
WHERE status = 'PENDING';

CREATE INDEX idx_job_executions_scheduled
ON job_executions(scheduled_time);

CREATE INDEX idx_job_executions_job_created
ON job_executions(job_id, created_at DESC);