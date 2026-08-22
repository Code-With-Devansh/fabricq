import redis from "../config/redis.js";
import { pool } from "../config/db.js";
import logger from "../config/logger/index.js";
import os from "os";
import {
  EXECUTION_QUEUE_KEY,
  EXECUTION_QUEUE_GROUP,
  ensureExecutionQueueGroup,
} from "../scheduler/scheduler.js";
import {
  disableJob,
} from "../repositories/httpJob.repository.js";
import { scheduleExecutionRetry } from "../retry/scheduleRetry.js";
import { parseRetryAfter } from "../retry/backoff.js";
import { classifyFailure } from "../retry/classifyFailure.js";
import config from "../config/index.js";
import {
  pushExecutionEvent,
  recordExecutionStatus,
} from "../streams/executionResults.js";
import { recoverExecution } from "../recovery/recovery.js";
import {
  assertUrlSyntaxIsSafe,
  ssrfSafeAgent,
  SsrfBlockedError,
} from "../validators/ssrf_guard.js";

const WORKER_ID = `${os.hostname()}:${process.pid}`;
const DEFAULT_HTTP_TIMEOUT_MS = 30_000;
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

function buildIdempotencyHeader(execution) {
  return {
    "X-FabricQ-Idempotency-Key": `${execution.execution_id}:${execution.attempt}`,
  };
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
          failureReason: "redirect_loop",
        };
      }
      visitedUrls.add(url);

      // SSRF guard: check scheme/IP-literal/blocked-hostname synchronously,
      // then fetch through an agent that pins the DNS resolution used for
      // the syntax check to the one used for the actual TCP connect - this
      // must run on every hop (initial URL and every redirect target),
      // since users control redirect destinations just as much as the
      // initial URL.
      try {
        assertUrlSyntaxIsSafe(url);
      } catch (err) {
        return {
          success: false,
          responseStatus: null,
          responseBody: null,
          redirectOccurred: redirects.length > 0,
          redirectCount: redirects.length,
          redirects,
          error: err instanceof SsrfBlockedError ? err.message : String(err),
          failureReason: "ssrf_blocked",
        };
      }

      let headers = {
        ...buildAuthHeaders(execution),
        ...(execution.headers ?? {}),
        // Deliberately last: this is an internal correctness mechanism,
        // not a user-configurable header, so a same-named header on the
        // job definition must not be able to silently clobber it.
        ...buildIdempotencyHeader(execution),
      };

      let res;
      try {
        res = await fetch(url, {
          method,
          headers,
          body,
          redirect: "manual",
          signal: controller.signal,
          dispatcher: ssrfSafeAgent,
        });
      } catch (err) {
        if (err instanceof SsrfBlockedError || err?.cause instanceof SsrfBlockedError) {
          return {
            success: false,
            responseStatus: null,
            responseBody: null,
            redirectOccurred: redirects.length > 0,
            redirectCount: redirects.length,
            redirects,
            error: (err.cause ?? err).message,
            failureReason: "ssrf_blocked",
          };
        }
        throw err;
      }

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
          // Parsed eagerly regardless of job.honor_retry_after - cheap, and
          // keeps the "should we honor it" decision entirely in
          // scheduleRetry.js/backoff.js rather than duplicated here.
          retryAfterSeconds: res.ok ? null : parseRetryAfter(res.headers.get("retry-after")),
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
          failureReason: "redirect_mode_error",
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
          failureReason: "redirect_missing_location",
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
          failureReason: "redirect_max_exceeded",
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
          failureReason: "redirect_policy_violation",
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
          failureReason: "redirect_policy_violation",
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
  //
  // No explicit transaction: every branch below performs at most one
  // write (disableJob XOR scheduleExecutionRetry, never both), and a
  // single statement is already atomic in Postgres. Using pool.query
  // directly instead of checking out a client avoids holding a pooled
  // connection for the duration of this branch. If a future change adds
  // a second write to any one branch, wrap that branch in an explicit
  // client + BEGIN/COMMIT again - don't let two writes land non-atomically.
  let willRetry = false;
  // Final status for this execution row, set below in every branch except
  // the "will retry" one (scheduleExecutionRetry owns the row's status -
  // retry_wait - in that case, see migration 020).
  let finalStatus = null;
  try {
    const isRecurring = job.schedule_type === "CRON";
    const attempt = job.attempt; // this execution's current attempt number
    const exhausted = attempt >= job.max_attempts;

    if (result.success) {
      finalStatus = "success";
      if (!isRecurring) {
        await disableJob(pool, job.job_id);
      }
    } else {
      // Cause (why it failed) and retryability (whether to try again) are
      // separate axes - classifyFailure only ever answers the latter.
      // "permanent" always stops retries regardless of attempts left;
      // "retryable" still stops once attempts are exhausted, just for a
      // different reason.
      const classification = classifyFailure(result);

      if (classification === "permanent") {
        finalStatus = "failed_permanent";
        if (!isRecurring) {
          await disableJob(pool, job.job_id);
        }
      } else if (!exhausted) {
        // Retries now work the same way for ONCE and CRON: a single
        // triggered execution gets retried in place, independent of the
        // job's own schedule cursor. See migration 020 / scheduleRetry.js.
        await scheduleExecutionRetry(pool, {
          executionId: job.execution_id,
          createdAt: job.created_at ?? null,
          job,
          attempt,
          retryAfterSeconds: result.retryAfterSeconds ?? null,
        });
        willRetry = true;
      } else {
        finalStatus = "failed_max_retries";
        if (!isRecurring) {
          await disableJob(pool, job.job_id);
        }
      }
    }
    // CRON, not retrying (permanent or exhausted): nothing further to do
    // to http_jobs - the next tick is already scheduled regardless of
    // this execution's outcome.
  } catch (err) {
    logger.error(
      { err, executionId: job.execution_id },
      "[worker] failed to update job after execution",
    );
    throw err;
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
    await recordExecutionStatus(job.execution_id, finalStatus, { final: true });
    await pushExecutionEvent({
      executionId: job.execution_id,
      createdAt: job.created_at ?? null,
      type: "completed",
      payload: { ...result, status: finalStatus, workerId: WORKER_ID },
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

// Runs one job end-to-end: bookkeeping, execute, finalize (XACK) or hand
// off to recovery on failure. Deliberately swallows all errors internally
// (never rejects) so it's safe to fire-and-forget from the pull loop below
// without producing an unhandled rejection.
//
// No claim bookkeeping needed here anymore - XREADGROUP already recorded
// this execution as owned by this consumer (WORKER_ID) in the group's
// pending-entries list the moment it was read, atomically with the read
// itself. There's no separate index write, and so no crash window between
// "job popped" and "ownership recorded" for recovery to fall back on.
async function processJob(job, streamId) {
  await Promise.all([
    recordExecutionStatus(job.execution_id, "running", { final: false }),
    pushExecutionEvent({ executionId: job.execution_id, createdAt: job.created_at ?? null, type: "running", payload: {} }),
  ]);

  try {
    await handleExecution(job);
    await redis.xack(EXECUTION_QUEUE_KEY, EXECUTION_QUEUE_GROUP, streamId);
  } catch (err) {
    logger.error(
      { err, executionId: job.execution_id },
      "[worker] failed to finalize execution, handing off to recovery",
    );
    try {
      await recoverExecution(job.execution_id, streamId, { skipFreshnessCheck: true });
    } catch (recoveryErr) {
      logger.error(
        { err: recoveryErr, executionId: job.execution_id },
        "[worker] fast-path recovery also failed, leaving entry pending for periodic recovery sweep",
      );
    }
  }
}

// Pulls the "payload" field back out of the flat field/value array
// XREADGROUP returns (e.g. ["payload", "<json>"]) - mirrors
// merger.js's parseEntry for the same wire shape.
function extractPayload(fields) {
  for (let i = 0; i < fields.length; i += 2) {
    if (fields[i] === "payload") return fields[i + 1];
  }
  return null;
}

export async function startWorker() {
  await ensureExecutionQueueGroup();

  logger.info(
    { concurrency: CONCURRENCY, workerId: WORKER_ID },
    "[worker] started, waiting for executions",
  );

  while (!shuttingDown) {
    if (inFlight.size >= CONCURRENCY) {
      await Promise.race(inFlight);
      continue;
    }

    let result;
    try {
      result = await redis.xreadgroup(
        "GROUP",
        EXECUTION_QUEUE_GROUP,
        WORKER_ID,
        "COUNT",
        1,
        "BLOCK",
        5000,
        "STREAMS",
        EXECUTION_QUEUE_KEY,
        ">",
      );
    } catch (err) {
      logger.error({ err }, "[worker] xreadgroup failed, backing off");
      await new Promise((r) => setTimeout(r, 1000));
      continue;
    }

    if (!result) continue; // timed out, nothing to do
    if (shuttingDown) {
      logger.warn(
        "[worker] popped a job after shutdown was requested, finishing it anyway",
      );
    }

    const [[, entries]] = result;
    const [streamId, fields] = entries[0];
    const raw = extractPayload(fields);
    if (!raw) {
      logger.error({ streamId }, "[worker] entry missing payload field, acking to drop it");
      await redis.xack(EXECUTION_QUEUE_KEY, EXECUTION_QUEUE_GROUP, streamId);
      continue;
    }

    const job = JSON.parse(raw);
    const execution = processJob(job, streamId).finally(() => {
      inFlight.delete(execution);
    });
    inFlight.add(execution);
  }

  logger.info(
    { inFlight: inFlight.size },
    "[worker] loop exited, no longer pulling new jobs",
  );
}