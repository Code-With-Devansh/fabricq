-- Partition job_executions by created_at (monthly) to fix unbounded table
-- growth. Retention policy: 3 months, enforced by DROPping whole old
-- partitions.
--
-- Design notes / trade-offs accepted here:
--
-- 1. PK becomes (execution_id, created_at). Postgres partitioning has no
--    global index - every partition keeps its own local indexes - so any
--    unique constraint at the table level MUST include the partition key.
--    execution_id alone can no longer be the sole PK. It's still globally
--    unique in practice (uuidv7), just not enforced as a standalone unique
--    constraint by Postgres anymore.
--
-- 2. Point-lookup/update queries that only have execution_id (no
--    created_at) will hit every partition's local index instead of
--    pruning to one. Fine at "a few dozen partitions" scale (3-month
--    retention = ~3-4 partitions live at a time), but the app-side fix is
--    to thread created_at through the hot paths (worker/merger/retry)
--    now that it's cheap to carry - see execution.repository.js changes
--    that accompany this migration.
--
-- 3. execution_outbox.execution_id -> job_executions(execution_id) FK
--    breaks for the same reason (FK target must be unique on exactly the
--    referenced columns). Outbox gains execution_created_at and the FK
--    becomes composite. Insert call sites need to pass it through
--    (createExecutionBatch/createExecution already RETURNING created_at,
--    so this is just plumbing, not a new query).

BEGIN;

-- ---------------------------------------------------------------------
-- 1. New partitioned table, same shape as the current job_executions.
-- ---------------------------------------------------------------------
CREATE TABLE job_executions_new (
    execution_id UUID NOT NULL DEFAULT uuidv7(),

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

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    redirect_occurred BOOLEAN NOT NULL DEFAULT false,
    redirect_count INTEGER NOT NULL DEFAULT 0,
    redirects JSONB NOT NULL DEFAULT '[]'::jsonb,

    retry_at TIMESTAMPTZ,

    CONSTRAINT valid_redirect_count CHECK (redirect_count >= 0),
    PRIMARY KEY (execution_id, created_at)
) PARTITION BY RANGE (created_at);

-- The original table still owns the canonical index names - free them up
-- before creating the new ones. job_executions_old is kept around after
-- the swap below purely for verification/rollback, so its indexes don't
-- need to be usable; just out of the way.
ALTER INDEX idx_job_executions_job RENAME TO idx_job_executions_job_legacy;
ALTER INDEX idx_job_executions_status RENAME TO idx_job_executions_status_legacy;
ALTER INDEX idx_job_executions_scheduled RENAME TO idx_job_executions_scheduled_legacy;
ALTER INDEX idx_job_executions_job_created RENAME TO idx_job_executions_job_created_legacy;
ALTER INDEX idx_job_executions_retry_wait RENAME TO idx_job_executions_retry_wait_legacy;
ALTER INDEX job_executions_pkey RENAME TO job_executions_pkey_legacy;

-- Indexes on the partitioned parent auto-create on every partition
-- (existing + future), so this list only needs to be maintained once.
CREATE INDEX idx_job_executions_job ON job_executions_new (job_id);
CREATE INDEX idx_job_executions_status ON job_executions_new (status);
CREATE INDEX idx_job_executions_scheduled ON job_executions_new (scheduled_time);
CREATE INDEX idx_job_executions_job_created ON job_executions_new (job_id, created_at DESC);
CREATE INDEX idx_job_executions_retry_wait ON job_executions_new (retry_at)
    WHERE status = 'retry_wait';

-- ---------------------------------------------------------------------
-- 2. Partition maintenance helpers. Used both for initial backfill below
--    and by the periodic maintenance job we'll wire up later (create next
--    month's partition ahead of time, drop partitions past retention).
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION create_job_executions_partition(month_start DATE)
RETURNS void AS $$
DECLARE
    part_name TEXT := 'job_executions_' || to_char(month_start, 'YYYY_MM');
    month_end DATE := (month_start + INTERVAL '1 month')::date;
BEGIN
    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS %I PARTITION OF job_executions_new
         FOR VALUES FROM (%L) TO (%L)',
        part_name, month_start, month_end
    );
END;
$$ LANGUAGE plpgsql;

-- Drops any monthly partition whose entire range is older than
-- retention_months. Safe to call repeatedly; no-op if nothing is due.
CREATE OR REPLACE FUNCTION drop_old_job_executions_partitions(retention_months INT DEFAULT 3)
RETURNS TABLE(dropped_partition TEXT) AS $$
DECLARE
    cutoff DATE := date_trunc('month', now())::date - (retention_months || ' months')::interval;
    part RECORD;
BEGIN
    FOR part IN
        SELECT c.relname
        FROM pg_inherits i
        JOIN pg_class c ON c.oid = i.inhrelid
        JOIN pg_class p ON p.oid = i.inhparent
        WHERE p.relname = 'job_executions'
          AND c.relname ~ '^job_executions_\d{4}_\d{2}$'
          AND to_date(substring(c.relname FROM '\d{4}_\d{2}$'), 'YYYY_MM') < cutoff
    LOOP
        EXECUTE format('ALTER TABLE job_executions DETACH PARTITION %I', part.relname);
        EXECUTE format('DROP TABLE %I', part.relname);
        dropped_partition := part.relname;
        RETURN NEXT;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- ---------------------------------------------------------------------
-- 3. Create partitions covering existing data plus a safety buffer.
--    Default partition catches anything outside the explicit range
--    (clock skew, backfilled rows, whatever) so inserts never fail
--    with "no partition found" - it should stay empty in steady state.
-- ---------------------------------------------------------------------
DO $$
DECLARE
    earliest DATE;
    m DATE;
BEGIN
    SELECT date_trunc('month', COALESCE(MIN(created_at), now()))::date
    INTO earliest
    FROM job_executions;

    m := earliest;
    WHILE m <= (date_trunc('month', now()) + INTERVAL '2 months')::date LOOP
        PERFORM create_job_executions_partition(m);
        m := (m + INTERVAL '1 month')::date;
    END LOOP;
END $$;

CREATE TABLE job_executions_default PARTITION OF job_executions_new DEFAULT;

-- ---------------------------------------------------------------------
-- 4. Copy existing data across, then swap names.
-- ---------------------------------------------------------------------
INSERT INTO job_executions_new
SELECT * FROM job_executions;

ALTER TABLE job_executions RENAME TO job_executions_old;
ALTER TABLE job_executions_new RENAME TO job_executions;

-- Rename the constituent partitions' auto-generated index names is not
-- necessary - they were created against job_executions_new and just move
-- with the table rename. job_executions_old is kept around deliberately
-- (not dropped) so this migration is easy to verify/rollback; drop it in
-- a follow-up migration once the swap is confirmed healthy in prod.

-- ---------------------------------------------------------------------
-- 5. execution_outbox: composite FK to match the new partitioned PK.
-- ---------------------------------------------------------------------
ALTER TABLE execution_outbox
    ADD COLUMN execution_created_at TIMESTAMPTZ;

UPDATE execution_outbox eo
SET execution_created_at = je.created_at
FROM job_executions je
WHERE je.execution_id = eo.execution_id;

ALTER TABLE execution_outbox
    ALTER COLUMN execution_created_at SET NOT NULL;

ALTER TABLE execution_outbox
    DROP CONSTRAINT execution_outbox_execution_id_fkey;

ALTER TABLE execution_outbox
    ADD CONSTRAINT execution_outbox_execution_fkey
    FOREIGN KEY (execution_id, execution_created_at)
    REFERENCES job_executions (execution_id, created_at)
    ON DELETE CASCADE;

COMMIT;