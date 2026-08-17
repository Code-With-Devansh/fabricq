import redis from "../config/redis.js";
import { pool } from "../config/db.js";
import logger from "../config/logger/index.js";
import os from "os";
import { EXECUTION_QUEUE_KEY } from "../scheduler/scheduler.js";
import {
  disableJob,
} from "../repositories/httpJob.repository.js";
import { scheduleExecutionRetry } from "../retry/scheduleRetry.js";
import config from "../config/index.js";
import {
  pushExecutionEvent,
  recordExecutionStatus,
} from "../streams/executionResults.js";
// NOTE: recovery.js imports HEARTBEAT_SET_KEY/PROCESSING_INDEX_KEY from this
// module, so this is a circular import. That's fine here - recoverExecution
// is only ever called from inside an async function body (never at module
// load time), so by the time it actually runs both modules have finished
// initializing.
import { recoverExecution } from "../recovery/recovery.js";

const WORKER_ID = `${os.hostname()}:${process.pid}`;
const DEFAULT_HTTP_TIMEOUT_MS = 30_000;
export const HEARTBEAT_SET_KEY = "execution:heartbeats";
const PROCESSING_QUEUE_KEY = `${EXECUTION_QUEUE_KEY}:processing:${WORKER_ID}`;
export const PROCESSING_INDEX_KEY = `${EXECUTION_QUEUE_KEY}:processing:index`;
const CONCURRENCY = Math.max(1, config.worker.concurrency);

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
  const redirectMode = execution.redirect_mode ?? "follow";

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
      if (redirectMode === "error") {
        return {
          success: false,
          responseStatus: res.status,
          responseBody: null,
          redirectOccurred: true,
          redirectCount: redirects.length,
          redirects,
          error: `Redirect encountered (${res.status}) but redirect_mode is "error"`,
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
      if (redirectMode === "manual") {
        redirects.push({
          status: res.status,
          from: url,
          location,
          to: nextUrl,
        });

        return {
          success: true,
          responseStatus: res.status,
          responseBody: null,
          redirectOccurred: true,
          redirectCount: redirects.length,
          redirects,
          error: null,
        };
      }
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

  // Scheduling-critical state stays on the synchronous Postgres path,
  // driven entirely off the in-memory `result` - it never reads or waits
  // on the write-behind execution-detail path below, so deferring that
  // can't create a race here.
  //
  // Retry state lives entirely on this execution's own job_executions row
  // (see migration 020) - http_jobs.next_run is a pure schedule cursor
  // that already advanced independently at claim time (scheduler.js) and
  // is never touched here, for ONCE or CRON alike. The only http_jobs
  // write left is disabling a ONCE job once it's fully resolved.
  const client = await pool.connect();
  let willRetry = false;
  try {
    await client.query("BEGIN");

    const isRecurring = job.schedule_type === "CRON";
    const attempt = job.attempt; // this execution's current attempt number
    const exhausted = attempt >= job.max_attempts;

    if (result.success) {
      if (!isRecurring) {
        await disableJob(client, job.job_id);
      }
    } else if (!exhausted) {
      // Retries now work the same way for ONCE and CRON: a single
      // triggered execution gets retried in place, independent of the
      // job's own schedule cursor. See migration 020 / scheduleRetry.js.
      await scheduleExecutionRetry(client, {
        executionId: job.execution_id,
        job,
        attempt,
      });
      willRetry = true;
    } else if (!isRecurring) {
      await disableJob(client, job.job_id);
    }
    // CRON, exhausted: nothing to do to http_jobs - the next tick is
    // already scheduled regardless of this execution's outcome.

    await client.query("COMMIT");
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

  // Execution-detail row (response body, status, redirects) is write-behind:
  // record the outcome in Redis (fast, synchronous, cheap - used by
  // recovery.js so it never races the merger) and hand the heavy payload
  // off to the stream for the merger process to batch into Postgres.
  //
  // A retry doesn't get "completed" here - scheduleExecutionRetry already
  // moved the row to retry_wait above, and the retry scheduler will flip
  // it to queued and republish it. Only genuinely final outcomes
  // (success, or failure with no retries left) get recorded as such.
  if (!willRetry) {
    const status = result.success ? "success" : "failed";
    await recordExecutionStatus(job.execution_id, status);
    await pushExecutionEvent({
      executionId: job.execution_id,
      type: "completed",
      payload: { ...result, workerId: WORKER_ID },
    });
  }

  logger.info(
    {
      executionId: job.execution_id,
      jobId: job.job_id,
      success: result.success,
      willRetry,
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
// Every in-flight job's settlement promise, keyed by execution_id purely
// for readability in logs/debugging - the Set itself is what stopWorker and
// the main loop use to know how many/which executions are still running.
const inFlight = new Set();

export async function stopWorker() {
  shuttingDown = true;
  if (inFlight.size > 0) {
    logger.info(
      { count: inFlight.size },
      "[worker] shutdown requested, waiting for in-flight executions to finish",
    );
    // Each entry already catches its own errors internally (see
    // processJob), so this never rejects - allSettled is just extra
    // insurance against that invariant ever slipping.
    await Promise.allSettled([...inFlight]);
  }
  logger.info("[worker] shutdown: no in-flight executions, safe to exit");
}

// Runs one job end-to-end: claim bookkeeping, heartbeat, execute, finalize
// or hand off to recovery on failure. Deliberately swallows all errors
// internally (never rejects) so it's safe to fire-and-forget from the pull
// loop below without producing an unhandled rejection.
async function processJob(job, raw) {
  // These three are independent of each other - run them concurrently
  // instead of paying for three sequential round trips before work starts.
  await Promise.all([
    redis.hset(
      PROCESSING_INDEX_KEY,
      job.execution_id,
      JSON.stringify({ workerId: WORKER_ID, raw }),
    ),
    recordExecutionStatus(job.execution_id, "running"),
    pushExecutionEvent({ executionId: job.execution_id, type: "running", payload: {} }),
    sendHeartBeat(job.execution_id),
  ]);
  const heartbeat = setInterval(() => {
    sendHeartBeat(job.execution_id);
  }, 10_000);

  try {
    await handleExecution(job);
    clearInterval(heartbeat);
    // Independent cleanup ops - pipeline instead of three sequential round trips.
    await redis
      .multi()
      .lrem(PROCESSING_QUEUE_KEY, 1, raw)
      .zrem(HEARTBEAT_SET_KEY, job.execution_id)
      .hdel(PROCESSING_INDEX_KEY, job.execution_id)
      .exec();
  } catch (err) {
    logger.error(
      { err, executionId: job.execution_id },
      "[worker] failed to finalize execution, handing off to recovery",
    );
    clearInterval(heartbeat);
    await clearHeartBeats(job.execution_id);
    try {
      await recoverExecution(job.execution_id, { skipFreshnessCheck: true });
    } catch (recoveryErr) {
      logger.error(
        { err: recoveryErr, executionId: job.execution_id },
        "[worker] fast-path recovery also failed, leaving heartbeat-less entry for periodic recovery sweep",
      );
    }
  }
}

export async function startWorker() {
  logger.info(
    { concurrency: CONCURRENCY, workerId: WORKER_ID },
    "[worker] started, waiting for executions",
  );

  while (!shuttingDown) {
    if (inFlight.size >= CONCURRENCY) {
      await Promise.race(inFlight);
      continue;
    }
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
    const execution = processJob(job, result).finally(() => {
      inFlight.delete(execution);
    });
    inFlight.add(execution);
  }

  logger.info(
    { inFlight: inFlight.size },
    "[worker] loop exited, no longer pulling new jobs",
  );
}