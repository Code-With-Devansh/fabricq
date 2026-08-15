-- Transactional outbox for handing scheduled executions off to Redis.
--
-- Problem this fixes: pollAndScheduleDueJobs() used to COMMIT the
-- Postgres side (execution row + next_run) and THEN LPUSH to Redis.
-- If the process died in that gap, the execution existed in Postgres
-- as fully scheduled but no worker would ever see it - nothing was
-- watching for "queued execution that never got heartbeated".
--
-- Fix: the LPUSH payload is written to this table in the SAME
-- transaction (same savepoint) as the execution row and the next_run
-- update. That write can never be "half done" - either the execution,
-- the next_run change, and the outbox row all land together, or none
-- of them do. A separate relay (src/outbox) publishes unpublished rows
-- to Redis and marks them published_at. If the app process dies before
-- ever attempting the Redis push, the row is just sitting there with
-- published_at IS NULL and the relay picks it up on its next sweep -
-- there's no gap left for a job to fall into.
CREATE TABLE execution_outbox (
    execution_id UUID PRIMARY KEY
        REFERENCES job_executions(execution_id)
        ON DELETE CASCADE,

    queue_key TEXT NOT NULL,
    payload JSONB NOT NULL,

    published_at TIMESTAMPTZ,
    publish_attempts INTEGER NOT NULL DEFAULT 0,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Partial index keeps the relay's "find unpublished rows" scan cheap
-- forever, regardless of how many published rows pile up over time.
CREATE INDEX idx_execution_outbox_unpublished
ON execution_outbox(created_at)
WHERE published_at IS NULL;
