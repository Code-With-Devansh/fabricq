ALTER TABLE job_executions
    ADD COLUMN redirect_occurred BOOLEAN NOT NULL DEFAULT false,
    ADD COLUMN redirect_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN redirects JSONB NOT NULL DEFAULT '[]'::jsonb;

ALTER TABLE job_executions
    ADD CONSTRAINT valid_redirect_count CHECK (redirect_count >= 0);