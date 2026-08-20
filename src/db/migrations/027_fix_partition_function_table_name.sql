-- Fixes: "relation job_executions_new does not exist" when calling
-- create_job_executions_partition() for any month after migration 024
-- itself ran.
--
-- Root cause: migration 024 defined create_job_executions_partition()
-- while the partitioned table was still named job_executions_new (before
-- the RENAME TO job_executions at the end of that migration). The
-- function body hardcoded that transient name. It worked during
-- migration 024 because the rename hadn't happened yet at the point the
-- function was called for the initial backfill - but every call after
-- migration 024 completed (i.e. every call the retention job actually
-- makes) fails, because job_executions_new no longer exists.
--
-- CREATE OR REPLACE with the corrected body - same signature, same
-- callers, no migration of existing partitions needed since this only
-- changes how future ones get created.

CREATE OR REPLACE FUNCTION create_job_executions_partition(month_start DATE)
RETURNS void AS $$
DECLARE
    part_name TEXT := 'job_executions_' || to_char(month_start, 'YYYY_MM');
    month_end DATE := (month_start + INTERVAL '1 month')::date;
BEGIN
    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS %I PARTITION OF job_executions
         FOR VALUES FROM (%L) TO (%L)',
        part_name, month_start, month_end
    );
END;
$$ LANGUAGE plpgsql;
