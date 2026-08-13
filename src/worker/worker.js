import redis from "../config/redis.js";
import { pool } from "../config/db.js";
import logger from "../config/logger/index.js";
import os from "os";
import { EXECUTION_QUEUE_KEY } from "../scheduler/scheduler.js";
import {
  markExecutionRunning,
  completeExecution,
} from "../repositories/execution.repository.js";
import {
  finalizeJobRun,
  getJobById,
  markJobFailedAwaitingRetry,
} from "../repositories/httpJob.repository.js";
import { RETRY_INTAKE_KEY } from "../retry/retry.js";

const WORKER_ID = `${os.hostname()}:${process.pid}`;
const DEFAULT_HTTP_TIMEOUT_MS = 30_000;
export const HEARTBEAT_SET_KEY = "execution:heartbeats";
const PROCESSING_QUEUE_KEY = `${EXECUTION_QUEUE_KEY}:processing:${WORKER_ID}`;
export const PROCESSING_INDEX_KEY = `${EXECUTION_QUEUE_KEY}:processing:index`;

const DEFAULT_REDIRECT_POLICY = {
  maxRedirects: 10,
  allowCrossOrigin: false,
  allowHttpDowngrade: false,
};
const REDIRECT_STATUS_CODES = new Set([301, 302, 303, 307, 308]);

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

  const redirect_policy = {
    ...DEFAULT_REDIRECT_POLICY,
    ...(execution.redirect_policy ?? {}),
  };

  const redirects = [];
  const visitedUrls = new Set();

  try {
    let url = buildUrl(execution);
    let method = execution.method;
    let body;
    const hasBody = !["GET", "DELETE"].includes(method);
    if (hasBody) {
      const { payload } = buildRequestBody(execution);
      body = payload;
    }

    while (true) {
      if (visitedUrls.has(url)) {
        return {
          success: false,
          responseStatus: null,
          responseBody: null,
          redirectOccurred: redirects.length > 0,
          redirectCount: redirects.length,
          redirects,
          error: "Redirect loop detected",
        };
      }
      visitedUrls.add(url);

      let headers = {
        ...buildAuthHeaders(execution),
        ...(execution.headers ?? {}),
      };

      const res = await fetch(url, {
        method,
        headers,
        body,
        redirect: "manual",
        signal: controller.signal,
      });

      if (!REDIRECT_STATUS_CODES.has(res.status)) {
        const responseBody = await res.text();
        return {
          success: res.ok,
          responseStatus: res.status,
          responseBody: responseBody.slice(0, 10_000),
          redirectOccurred: redirects.length > 0,
          redirectCount: redirects.length,
          redirects,
          error: res.ok ? null : `HTTP ${res.status}`,
        };
      }

      const location = res.headers.get("location");
      if (!location) {
        return {
          success: false,
          responseStatus: res.status,
          responseBody: null,
          redirectOccurred: redirects.length > 0,
          redirectCount: redirects.length,
          redirects,
          error: `Redirect response ${res.status} missing Location header`,
        };
      }

      const nextUrl = new URL(location, url).href;

      if (redirects.length >= redirect_policy.maxRedirects) {
        return {
          success: false,
          responseStatus: res.status,
          responseBody: null,
          redirectOccurred: true,
          redirectCount: redirects.length,
          redirects,
          error: `Maximum redirect count (${redirect_policy.maxRedirects}) exceeded`,
        };
      }

      const previous = new URL(url);
      const next = new URL(nextUrl);
      const crossOrigin = previous.origin !== next.origin;

      if (crossOrigin && !redirect_policy.allowCrossOrigin) {
        return {
          success: false,
          responseStatus: res.status,
          responseBody: null,
          redirectOccurred: true,
          redirectCount: redirects.length,
          redirects,
          error: `Cross-origin redirect to ${next.origin} is not allowed by this job's redirect policy`,
        };
      }

      // --- HTTPS -> HTTP downgrade gate ---
      if (
        !redirect_policy.allowHttpDowngrade &&
        previous.protocol === "https:" &&
        next.protocol === "http:"
      ) {
        return {
          success: false,
          responseStatus: res.status,
          responseBody: null,
          redirectOccurred: true,
          redirectCount: redirects.length,
          redirects,
          error:
            "HTTPS to HTTP redirect is not allowed by this job's redirect policy",
        };
      }

      redirects.push({
        status: res.status,
        from: url,
        location,
        to: nextUrl,
      });

      // Credentials are stripped on every cross-origin hop that is actually
      // followed. `allowCrossOrigin` controls whether the redirect itself is
      // allowed; it does not permit credentials to cross origins. This prevents
      // Authorization/Cookie headers from being forwarded to an untrusted
      // redirect target.
      if (crossOrigin) {
        const strippedHeaders = { ...headers };
        delete strippedHeaders.Authorization;
        delete strippedHeaders.authorization;
        delete strippedHeaders.Cookie;
        delete strippedHeaders.cookie;
        headers = strippedHeaders;
      }

      // Fetch redirect-following semantics:
      // 301/302: POST -> GET
      // 303: anything except HEAD -> GET
      // 307/308: preserve method and body
      if ((res.status === 301 || res.status === 302) && method === "POST") {
        method = "GET";
        body = undefined;
      } else if (res.status === 303 && method !== "HEAD") {
        method = "GET";
        body = undefined;
      }

      url = nextUrl;
    }
  } catch (err) {
    const isAbort = err instanceof Error && err.name === "AbortError";
    return {
      success: false,
      responseStatus: null,
      responseBody: null,
      redirectOccurred: redirects.length > 0,
      redirectCount: redirects.length,
      redirects,
      error: isAbort
        ? "Request timed out"
        : err instanceof Error
          ? err.message
          : String(err),
    };
  } finally {
    clearTimeout(timeout);
  }
}
async function handleExecution(job) {
  const result = await executeHttpJob(job);
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    await completeExecution(client, job.execution_id, result, WORKER_ID);

    const isRecurring = job.schedule_type === "CRON";
    const nextAttempt = job.attempts + 1;
    const exhaustedRetries = nextAttempt >= job.max_attempts;

    if (result.success) {
      await finalizeJobRun(client, job.job_id, {
        isRecurring,
      });
    } else if (!isRecurring && !exhaustedRetries) {
      // ONCE job that failed but has retries left: worker's job ends here.
      // It does NOT compute a backoff delay - it just marks the attempt and
      // leaves next_run NULL, then hands off to the retry scheduler, which
      // owns all backoff-policy logic (see src/retry/retry.js).
      await markJobFailedAwaitingRetry(client, job.job_id);
    } else {
      await finalizeJobRun(client, job.job_id, {
        isRecurring,
      });
    }

    await client.query("COMMIT");

    if (!result.success && !isRecurring && !exhaustedRetries) {
      await redis.lpush(
        RETRY_INTAKE_KEY,
        JSON.stringify({ job: job, attempt: nextAttempt }),
      );
    }
  } catch (err) {
    await client.query("ROLLBACK");
    logger.error(
      { err, executionId: job.execution_id },
      "[worker] failed to update job after execution",
    );
    throw err;
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

let shuttingDown = false;
let currentExecution = null;
export async function stopWorker() {
  shuttingDown = true;
  if (currentExecution) {
    logger.info(
      "[worker] shutdown requested, waiting for in-flight execution to finish",
    );
    await currentExecution.catch(() => {}); // already logged inside handleExecution
  }
  logger.info("[worker] shutdown: no in-flight execution, safe to exit");
}

export async function startWorker() {
  logger.info("[worker] started, waiting for executions");
  while (!shuttingDown) {
    const result = await redis.blmove(
      EXECUTION_QUEUE_KEY,
      PROCESSING_QUEUE_KEY,
      "RIGHT",
      "LEFT",
      5,
    );

    if (!result) continue; // timed out, nothing to do
    if (shuttingDown) {
      logger.warn(
        "[worker] popped a job after shutdown was requested, finishing it anyway",
      );
    }
    const job = JSON.parse(result);
    await redis.hset(
      PROCESSING_INDEX_KEY,
      job.execution_id,
      JSON.stringify({ workerId: WORKER_ID, raw: result }),
    );
    await markExecutionRunning(job.execution_id);
    await sendHeartBeat(job.execution_id);
    const heartbeat = setInterval(() => {
      sendHeartBeat(job.execution_id);
    }, 10_000);
    currentExecution = handleExecution(job).catch((err) => {
      logger.error({ err }, "[worker] loop error, retrying in 1s");
      return new Promise((r) => setTimeout(r, 1000));
    });
    try {
      await currentExecution;
      await redis.lrem(PROCESSING_QUEUE_KEY, 1, result);
    } finally {
      clearInterval(heartbeat);
      await clearHeartBeats(job.execution_id);
      await redis.hdel(PROCESSING_INDEX_KEY, job.execution_id);
      currentExecution = null;
    }
  }
  logger.info("[worker] loop exited, no longer pulling new jobs");
}
