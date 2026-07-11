import redis from "../config/redis.js";
import { pool } from "../config/db.js";
import logger from "../config/logger/index.js";
import os from "os";
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

const DEFAULT_HTTP_TIMEOUT_MS = 30_000;
const HEARTBEAT_SET_KEY = "execution:heartbeats";

function buildUrl(execution) {
  const url = new URL(execution.url);
  for (const [key, value] of Object.entries(execution.query_params ?? {})) {
    url.searchParams.set(key, value);
  }

  if (execution.auth_type === "API_KEY") {
    const { key, value, in: location = "header" } = execution.auth_config ?? {};
    if (location === "query" && key) {
      url.searchParams.set(key, value);
    }
  }

  return url.toString();
}

function buildAuthHeaders(execution) {
  const { auth_type, auth_config = {} } = execution;

  switch (auth_type) {
    case "BEARER":
      return { Authorization: `Bearer ${auth_config.token}` };
    case "BASIC": {
      const encoded = Buffer.from(
        `${auth_config.username}:${auth_config.password}`,
      ).toString("base64");
      return { Authorization: `Basic ${encoded}` };
    }
    case "API_KEY":
      if ((auth_config.in ?? "header") === "header" && auth_config.key) {
        return { [auth_config.key]: auth_config.value };
      }
      return {};
    case "NONE":
    default:
      return {};
  }
}

function buildRequestBody(execution) {
  const body = execution.body ?? {};
  if (execution.body_type === "form") {
    const form = new URLSearchParams();
    for (const [key, value] of Object.entries(body)) {
      form.append(key, value == null ? "" : String(value));
    }
    return {
      contentTypeHeader: {
        "Content-Type": "application/x-www-form-urlencoded",
      },
      payload: form.toString(),
    };
  }

  // default: json
  return {
    contentTypeHeader: { "Content-Type": "application/json" },
    payload: JSON.stringify(body),
  };
}

async function executeHttpJob(execution) {
  const controller = new AbortController();
  const timeoutMs = execution.timeout_ms ?? DEFAULT_HTTP_TIMEOUT_MS;
  const timeout = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const hasBody = !["GET", "DELETE"].includes(execution.method);
    const { contentTypeHeader, payload } = hasBody
      ? buildRequestBody(execution)
      : { contentTypeHeader: {}, payload: undefined };

    const res = await fetch(buildUrl(execution), {
      method: execution.method,
      headers: {
        ...contentTypeHeader,
        ...buildAuthHeaders(execution),
        ...(execution.headers ?? {}),
      },
      body: payload,
      redirect: execution.redirect_mode ?? "follow",
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
    // fetch() throws a TypeError with "unsafe redirect" language when
    // redirect_mode is "error" and a redirect is encountered - surface
    // that distinctly rather than lumping it into a generic message.
    const isRedirectError =
      execution.redirect_mode === "error" &&
      err instanceof TypeError &&
      /redirect/i.test(err.message);

    return {
      success: false,
      responseStatus: null,
      responseBody: null,
      error: isRedirectError
        ? "Redirect encountered with redirect_mode=error"
        : err instanceof Error
          ? err.name === "AbortError"
            ? "Request timed out"
            : err.message
          : String(err),
    };
  } finally {
    clearTimeout(timeout);
  }
}

async function handleExecution(job) {
  await markExecutionRunning(job.execution_id);

  const result = await executeHttpJob(job);

  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    await completeExecution(client, job.execution_id, result);

    // await incrementAttempts(client, job.job_id);

    const isRecurring = job.schedule_type === "CRON";
    const exhaustedRetries = job.attempts >= job.max_attempts;

    if (result.success) {
      await finalizeJobRun(client, job.job_id, {
        success: true,
        isRecurring,
      });
    } else if (!isRecurring && !exhaustedRetries) {
      // ONCE job that failed but has retries left: reschedule after backoff.

      await client.query(
        `UPDATE http_jobs
         SET status = 'PENDING',
             attempts = attempts + 1,
             next_run = now() + (backoff_seconds || ' seconds')::interval,
             updated_at = now()
         WHERE job_id = $1`,
        [job.job_id],
      );
    } else {
      await finalizeJobRun(client, job.job_id, {
        success: false,
        isRecurring,
      });
    }

    await client.query("COMMIT");
  } catch (err) {
    await client.query("ROLLBACK");
    logger.error(
      { err, executionId: job.execution_id },
      "[worker] failed to update job after execution",
    );
  } finally {
    client.release();
  }

  logger.info(
    {
      executionId: job.execution_id,
      jobId: job.job_id,
      success: result.success,
    },
    "[worker] execution finished",
  );
}

async function sendHeartBeat(executionId) {
  try {
    await redis.zadd(HEARTBEAT_SET_KEY, Date.now(), executionId);
  } catch (err) {
    logger.warn({ err, executionId }, "Failed to update heartbeat");
  }
}
async function clearHeartBeats(executionId) {
  await redis.zrem(HEARTBEAT_SET_KEY, executionId);
}

export async function startWorker() {
  logger.info("[worker] started, waiting for executions");
  const WORKER_ID = `${os.hostname()}:${process.pid}`;
  // Simple blocking-pop loop. BRPOP blocks the connection until an item
  // arrives or timeout elapses - cheap on Redis, no busy-polling.
  for (;;) {
    const result = await redis.brpop(EXECUTION_QUEUE_KEY, 5); // 5s timeout, then loop again
    if (!result) continue; // timed out, nothing to do
    const [, data] = result;
    const job = JSON.parse(data);
    await sendHeartBeat(job.execution_id);
    const heartbeat = setInterval(() => {
      void sendHeartBeat(job.execution_id);
    }, 10_000);
    try {
      await handleExecution(job);
    } catch (err) {
      logger.error({ err }, "[worker] loop error, retrying in 1s");
      await new Promise((r) => setTimeout(r, 1000));
    } finally {
      clearInterval(heartbeat);
      await clearHeartBeats(job.execution_id);
    }
  }
}
