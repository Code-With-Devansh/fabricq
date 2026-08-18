-- Splits the old generic 'failed' terminal state into two causes:
-- 'failed_permanent'   - classifyFailure() determined retrying is pointless
--                         (deterministic 4xx, SSRF block, redirect policy
--                         violation, etc.) - see retry/classifyFailure.js
-- 'failed_max_retries' - the failure was retryable but attempts ran out
--                         (worker.js), or an execution was abandoned by
--                         recovery.js (poison-pill delivery count exceeded,
--                         or genuinely abandoned with no attempts left)
--
-- Existing rows already written as 'failed' are left as-is; new code no
-- longer writes that value but it stays valid in the enum for old rows.
ALTER TYPE execution_status
ADD VALUE IF NOT EXISTS 'failed_permanent';

ALTER TYPE execution_status
ADD VALUE IF NOT EXISTS 'failed_max_retries';