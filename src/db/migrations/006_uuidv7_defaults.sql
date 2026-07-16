-- Switch primary key generation from UUIDv4 (gen_random_uuid()) to UUIDv7
-- (uuidv7(), native to Postgres 18+). v7 embeds a millisecond timestamp
-- prefix so new rows insert in roughly ascending PK order instead of
-- landing at random points in the btree - less index bloat/page-splitting
-- on job_executions in particular, which gets a new row per attempt.
--
-- Existing rows keep their v4 ids - this only changes what new inserts get.
-- No backfill: mixing v4 and v7 ids in the same column is safe, since both
-- are just UUID values: the ordering benefit only applies going forward.

ALTER TABLE http_jobs
    ALTER COLUMN job_id SET DEFAULT uuidv7();

ALTER TABLE job_executions
    ALTER COLUMN execution_id SET DEFAULT uuidv7();