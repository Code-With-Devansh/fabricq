import redis from "../config/redis.js";
import { pool } from "../config/db.js";
import logger from "../config/logger/index.js";
import { EXECUTION_QUEUE_KEY } from "../scheduler/scheduler.js";
import {
  getExecutionWithJob,
  markExecutionRunning,
  completeExecution,
} from "../repositories/execution.repository.js";
import {
  incrementAttempts,
  finalizeJobRun,
  getJobById,
} from "../repositories/httpJob.repository.js";

const HTTP_TIMEOUT_MS = 30_000;

async function executeHttpJob(execution) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), HTTP_TIMEOUT_MS);


  try {
    const hasBody = !["GET", "DELETE"].includes(execution.method);
  
  
  
  
  
    const res = await fetch(execution.url, {
      method: execution.method,
      headers: {
        ...(hasBody && { "Content-Type": "application/json" }),
        ...(execution.headers ?? {}),
      },
      body: hasBody ? JSON.stringify(execution.body ?? {}) : undefined,
      signal: controller.signal,
    });

    const responseBody = await res.text();
  
  
    return {
      success: res.ok,
      responseStatus: res.status,
      responseBody: responseBody.slice(0, 10_000), // don't let a huge body bloat the row
      error: res.ok ? null : `HTTP ${res.status}`,
    };
  } catch (err) {
  
  
    return {
      success: false,
      responseStatus: null,
      responseBody: null,
      error:
        err instanceof Error
          ? err.name === "AbortError"
            ? "Request timed out"
            : err.message
          : String(err),
    };
  } finally {
    clearTimeout(timeout);
  }
}

async function handleExecution(executionId) {
  const execution = await getExecutionWithJob(executionId);
  if (!execution) {
    logger.warn({ executionId }, "[worker] execution not found, skipping");
    return;
  }

  await markExecutionRunning(executionId);

  const result = await executeHttpJob(execution);

  await completeExecution(executionId, result);

  const client = await pool.connect();
  try {
  
    await client.query("BEGIN");
  
    await incrementAttempts(client, execution.job_id);
  

    const job = await getJobById(client, execution.job_id);
  
  
    const isRecurring = execution.schedule_type === "CRON";
    const exhaustedRetries = job.attempts >= job.max_attempts;
  
  

    if (result.success) {
      await finalizeJobRun(client, execution.job_id, {
        success: true,
        isRecurring,
      });
    } else if (!isRecurring && !exhaustedRetries) {
      // ONCE job that failed but has retries left: reschedule after backoff.
    
      await client.query(
        `UPDATE http_jobs
         SET status = 'PENDING',
             next_run = now() + (backoff_seconds || ' seconds')::interval,
             updated_at = now()
         WHERE job_id = $1`,
        [execution.job_id],
      );
    } else {
    
      await finalizeJobRun(client, execution.job_id, {
        success: false,
        isRecurring,
      });
    }

    await client.query("COMMIT");
  } catch (err) {
    await client.query("ROLLBACK");
    logger.error(
      { err, executionId },
      "[worker] failed to update job after execution",
    );
  } finally {
    client.release();
  }

  logger.info(
    { executionId, jobId: execution.job_id, success: result.success },
    "[worker] execution finished",
  );
}

export async function startWorker() {
  logger.info("[worker] started, waiting for executions");
  // Simple blocking-pop loop. BRPOP blocks the connection until an item
  // arrives or timeout elapses - cheap on Redis, no busy-polling.
  for (;;) {
    try {
      const result = await redis.brpop(EXECUTION_QUEUE_KEY, 5); // 5s timeout, then loop again
      if (!result) continue; // timed out, nothing to do
      const [, executionId] = result;
      await handleExecution(executionId);
    } catch (err) {
      logger.error({ err }, "[worker] loop error, retrying in 1s");
      await new Promise((r) => setTimeout(r, 1000));
    }
  }
}
