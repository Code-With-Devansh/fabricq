import { computeDelaySeconds } from "./backoff.js";
import { markExecutionRetryWait } from "../repositories/execution.repository.js";

// Shared by worker.js (clean failure) and recovery.js (crash/abandonment) -
// both need the exact same "fail this attempt, schedule the next one"
// transition, and both call it inside their own transaction so it commits
// atomically with everything else that attempt's failure implies.
//
// `attempt` is the attempt number that just failed (1-indexed). The next
// attempt will be attempt+1, which is what the backoff delay is computed
// for and what markExecutionRetryWait bumps the row to.
export async function scheduleExecutionRetry(client, { executionId, createdAt = null, job, attempt, retryAfterSeconds = null }) {
  const delaySeconds = computeDelaySeconds(job, attempt + 1, retryAfterSeconds);
  const retryAt = new Date(Date.now() + delaySeconds * 1000);
  return markExecutionRetryWait(client, executionId, createdAt, { retryAt });
}