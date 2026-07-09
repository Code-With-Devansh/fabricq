ALTER TABLE http_jobs
    ADD COLUMN locked_at TIMESTAMPTZ; -- set when scheduler claims it for this cycle
 