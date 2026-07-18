
ALTER TABLE http_jobs DROP COLUMN status;
DROP TYPE job_status;

ALTER TABLE http_jobs ADD COLUMN enabled BOOLEAN NOT NULL DEFAULT true;

CREATE INDEX idx_http_jobs_next_run ON http_jobs (next_run) WHERE enabled;
