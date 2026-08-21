import crypto from "crypto";
import redis from "../config/redis.js";
import { pool } from "../config/db.js";
import logger from "../config/logger/index.js";
import {
  EXECUTION_QUEUE_KEY,
  EXECUTION_QUEUE_GROUP,
  ensureExecutionQueueGroup,
} from "../scheduler/scheduler.js";
import { getExecutionById } from "../repositories/execution.repository.js";
import { disableJob } from "../repositories/httpJob.repository.js";
import { scheduleExecutionRetry } from "../retry/scheduleRetry.js";
import {
  pushExecutionEvent,
  recordExecutionStatus,
  getExecutionStatus,
} from "../streams/executionResults.js";

// Staleness is judged per-entry against that job's own timeout_ms, not a
// single global constant - jobs are allowed to legitimately run up to the
// timeout_ms validator's max (120s per worker_process.js), and a worker
// never touches its PEL entry again after XREADGROUP until it XACKs, so a
// fixed threshold shorter than a job's own timeout would reclaim entries
// that are still genuinely in flight. Two constants instead:
//
// SCAN_FLOOR_IDLE_MS: the IDLE filter passed to XPENDING itself, just to
// keep the initial scan cheap (skip entries that can't possibly be stale
// yet under any job's timeout). The real per-entry decision happens in
// runRecoveryCycle() once each candidate's own timeout_ms is known.
const SCAN_FLOOR_IDLE_MS = 10_000;
// RECOVERY_SAFETY_MARGIN_MS: headroom added on top of a job's own
// timeout_ms before recovery will treat it as abandoned - covers the gap
// between the worker's AbortController firing and it finishing the
// Postgres transaction + write-behind status/event calls in
// handleExecution, so a worker that's still legitimately wrapping up
// can't lose the race to recovery.
const RECOVERY_SAFETY_MARGIN_MS = 15_000;
// Fallback when a payload predates timeout_ms or omits it - must match
// worker.js's DEFAULT_HTTP_TIMEOUT_MS. Duplicated rather than imported:
// worker.js already imports recoverExecution from this module, so
// importing back would be circular.
const DEFAULT_HTTP_TIMEOUT_MS = 30_000;
const CLAIM_BATCH_SIZE = 100;

// Identity this process claims entries under. Distinct from any worker's
// WORKER_ID so a recovered entry's ownership is unambiguous in XPENDING
// output while recovery is working on it.
const RECOVERY_CONSUMER = `recovery:${crypto.randomUUID()}`;

// Guards against a poison-pill payload (e.g. one that always throws before
// the worker can even parse it) looping forever between "claimed by a
// worker", "worker dies/crashes on it immediately", "reclaimed by
// recovery", repeat. XPENDING's delivery-count tells us how many times an
// entry has been claimed; past this many, stop retrying it as a live job
// and just fail it outright.
const MAX_DELIVERY_COUNT = 5;

const unlockScript = `
  if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
  else
    return 0
  end
`;
const LOCK_PREFIX = "recovery:lock:";
const LOCK_TTL_MS = 30_000;

async function acquireLock(executionId) {
  const token = crypto.randomUUID();
  const ok = await redis.set(
    `${LOCK_PREFIX}${executionId}`,
    token,
    "PX",
    LOCK_TTL_MS,
    "NX",
  );
  return ok === "OK" ? token : null;
}

async function releaseLock(executionId, token) {
  await redis.eval(unlockScript, 1, `${LOCK_PREFIX}${executionId}`, token);
}

function extractPayload(fields) {
  for (let i = 0; i < fields.length; i += 2) {
    if (fields[i] === "payload") return fields[i + 1];
  }
  return null;
}

// Lists PEL entries idle longer than SCAN_FLOOR_IDLE_MS - a broad first
// pass, cheap because XPENDING already tracks per-entry idle time and
// delivery count natively (no separate bookkeeping structure for a worker
// to maintain, no crash window between "job claimed" and "bookkeeping
// written"). Candidates returned here still need to be checked against
// their own job's timeout_ms in runRecoveryCycle() before being treated
// as genuinely stale.
async function getStalePendingEntries() {
  // [id, consumer, idleMs, deliveryCount][]
  const entries = await redis.xpending(
    EXECUTION_QUEUE_KEY,
    EXECUTION_QUEUE_GROUP,
    "IDLE",
    SCAN_FLOOR_IDLE_MS,
    "-",
    "+",
    CLAIM_BATCH_SIZE,
  );
  return entries ?? [];
}

// Marks a stale execution as failed-with-a-pending-retry after a crash.
// Mirrors the retry branch in worker.js's handleExecution so a crash and a
// clean failure end up in the same place: this exact execution row moves
// to retry_wait, independent of the job's own next_run cursor (ONCE or
// CRON alike - see migration 020). Locked_at needs no attention here -
// http_jobs.next_run/locked_at already advanced independently at claim
// time (scheduler.js), regardless of how this execution resolves.
async function rescheduleForRetry(client, executionId, job, attempt) {
  await scheduleExecutionRetry(client, { executionId, createdAt: job.created_at ?? null, job, attempt });
}

// Handles a single stale execution: decide whether the worker actually
// finished before dying (nothing to do but ack/tidy) or genuinely
// abandoned it mid-flight (fail the execution, retry/finalize the job).
//
// streamId: the entry's ID in EXECUTION_QUEUE_KEY. Passed directly by the
// worker's fast-path call (it already has it from its own XREADGROUP), or
// discovered via getStalePendingEntries() by the periodic sweep.
//
// skipFreshnessCheck: set by the worker's own fast-path call (see
// worker.js) when IT is the one telling us the execution was abandoned. In
// that case there's no idle-time race to re-check - the worker calling
// this function *is* the one who held the claim, so waiting to see if it
// goes stale would just add latency for no additional safety. The
// periodic sweep in runRecoveryCycle() never sets this - it always
// re-claims via XCLAIM, whose min-idle-time check IS the re-check.
// minIdleMs: the min-idle-time gate passed to XCLAIM on the periodic-sweep
// path. This is a concurrency guard, not the staleness decision itself -
// the caller (runRecoveryCycle) has already decided this entry's idle
// time clears its own job's timeout_ms + RECOVERY_SAFETY_MARGIN_MS before
// ever calling in here. Defaults to the pre-fix constant's value only for
// callers that don't have a job-specific figure on hand yet.
async function recoverExecution(
  executionId,
  streamId,
  { skipFreshnessCheck = false, minIdleMs = DEFAULT_HTTP_TIMEOUT_MS } = {},
) {
  // The lock only earns its keep on the fast-path call: skipFreshnessCheck
  // means the caller is trusting its own ownership of the entry with no
  // re-check against Redis, so nothing else guards against two calls
  // racing on the same executionId (e.g. worker's crash-handling path
  // firing twice). On the periodic-sweep path, XCLAIM's min-idle-time
  // check is already atomic at the Redis level - two recovery processes
  // racing on the same stale entry will have exactly one XCLAIM succeed,
  // so a second application-level lock there is redundant: two extra
  // round trips per swept entry for safety XCLAIM already provides.
  const token = skipFreshnessCheck ? await acquireLock(executionId) : null;
  if (skipFreshnessCheck && !token) {
    logger.debug({ executionId }, "[recovery] lock held by another recovery run, skipping");
    return;
  }

  try {
    let raw;
    let deliveryCount = 1;

    if (skipFreshnessCheck) {
      // The calling worker already owns this entry outright (it's the one
      // that read it and is now telling us it died on it) - no need to
      // reclaim, just read the payload back off the stream directly.
      const range = await redis.xrange(EXECUTION_QUEUE_KEY, streamId, streamId);
      if (range.length === 0) {
        logger.warn({ executionId, streamId }, "[recovery] entry no longer in stream, nothing to recover");
        return;
      }
      raw = extractPayload(range[0][1]);
    } else {
      // XCLAIM's min-idle-time gate IS the concurrency guard here - if
      // another worker or recovery run touched this entry more recently
      // than minIdleMs ago, the claim below simply returns nothing and
      // we back off, exactly like re-checking a heartbeat (or a
      // Redis-level lock) used to, but atomically and for free as part of
      // the claim itself.
      const claimed = await redis.xclaim(
        EXECUTION_QUEUE_KEY,
        EXECUTION_QUEUE_GROUP,
        RECOVERY_CONSUMER,
        minIdleMs,
        streamId,
      );
      if (claimed.length === 0) {
        logger.debug({ executionId, streamId }, "[recovery] entry is fresh again or already claimed, skipping");
        return;
      }
      raw = extractPayload(claimed[0][1]);

      const pendingDetail = await redis.xpending(
        EXECUTION_QUEUE_KEY,
        EXECUTION_QUEUE_GROUP,
        streamId,
        streamId,
        1,
      );
      if (pendingDetail?.[0]) deliveryCount = Number(pendingDetail[0][3]) || 1;
    }

    if (!raw) {
      logger.error({ executionId, streamId }, "[recovery] entry missing payload field, acking to drop it");
      await redis.xack(EXECUTION_QUEUE_KEY, EXECUTION_QUEUE_GROUP, streamId);
      return;
    }
    const job = JSON.parse(raw);

    // Fast path first: worker's own write-behind status hash, which is
    // written synchronously (see streams/executionResults.js) so it can't
    // lag behind the merger's batched Postgres flush. Only fall back to
    // Postgres if the hash entry is missing - e.g. its TTL expired, or
    // this is a very old entry from before the merger existed.
    let executionStatus = await getExecutionStatus(executionId);
    if (!executionStatus) {
      const execution = await getExecutionById(executionId);
      if (!execution) {
        logger.error({ executionId }, "[recovery] execution not found in postgres, dropping orphaned entry");
        await redis.xack(EXECUTION_QUEUE_KEY, EXECUTION_QUEUE_GROUP, streamId);
        return;
      }
      executionStatus = execution.status;
    }

    if (
      executionStatus === "success" ||
      executionStatus === "failed" || // legacy value, pre-migration-022 rows
      executionStatus === "failed_permanent" ||
      executionStatus === "failed_max_retries"
    ) {
      // Worker completed the HTTP call and recorded its outcome, but
      // crashed before it could XACK. The execution result is already
      // correct (or queued for the merger to apply) - just ack it off
      // the stream.
      logger.info({ executionId, status: executionStatus }, "[recovery] execution already finished, acking only");
      await redis.xack(EXECUTION_QUEUE_KEY, EXECUTION_QUEUE_GROUP, streamId);
      return;
    }

    if (deliveryCount > MAX_DELIVERY_COUNT) {
      // Entry has been claimed and lost this many times without ever
      // reaching a final status - treat it as poisoned rather than
      // handing it back out for another worker to choke on. Fail the
      // job outright (no further retry scheduling) and drop it.
      logger.error(
        { executionId, deliveryCount },
        "[recovery] exceeded max delivery count, failing execution and dropping entry",
      );
      // Operational give-up, not a classification of *why* the HTTP call
      // failed (it may never have completed one) - always
      // failed_max_retries, never failed_permanent. See classifyFailure.js.
      await recordExecutionStatus(executionId, "failed_max_retries", { final: true });
      // `job` may be undefined here if `raw` failed to parse earlier and
      // we already returned - by this point it's always a parsed object,
      // just possibly from a payload predating the created_at field.
      await pushExecutionEvent({
        executionId,
        createdAt: job?.created_at ?? null,
        type: "completed",
        payload: {
          success: false,
          status: "failed_max_retries",
          error: `Execution abandoned: exceeded max delivery count (${MAX_DELIVERY_COUNT})`,
          workerId: null,
        },
      });
      await redis.xack(EXECUTION_QUEUE_KEY, EXECUTION_QUEUE_GROUP, streamId);
      return;
    }

    // Still "running"/"queued" with no live claimant: genuinely
    // abandoned.
    const client = await pool.connect();
    let willRetry = false;
    try {
      await client.query("BEGIN");

      const isRecurring = job.schedule_type === "CRON";
      const attempt = job.attempt;
      const exhausted = attempt >= job.max_attempts;
      willRetry = !exhausted;

      if (willRetry) {
        await rescheduleForRetry(client, executionId, job, attempt);
      } else if (!isRecurring) {
        await disableJob(client, job.job_id);
      }
      // CRON, exhausted: nothing to do to http_jobs, same as worker.js.

      await client.query("COMMIT");
    } catch (err) {
      await client.query("ROLLBACK");
      logger.error({ err, executionId, jobId: job.job_id }, "[recovery] failed to recover abandoned execution, leaving in place for next cycle");
      return;
    } finally {
      client.release();
    }

    // Execution-detail row, same write-behind path as worker.js: fast
    // status recorded synchronously, heavy payload deferred to the merger.
    // Only a genuinely final outcome gets recorded this way - a retry
    // already moved the row to retry_wait above, and the retry scheduler
    // owns republishing it from there.
    if (!willRetry) {
      // Abandoned with no attempts left - same reasoning as the
      // delivery-count branch above: this is exhaustion, not a
      // classification of the failure cause, so always failed_max_retries.
      await recordExecutionStatus(executionId, "failed_max_retries", { final: true });
      await pushExecutionEvent({
        executionId,
        type: "completed",
        payload: {
          success: false,
          status: "failed_max_retries",
          error: "Execution abandoned: worker died mid-flight",
          workerId: null,
        },
      });
    }

    await redis.xack(EXECUTION_QUEUE_KEY, EXECUTION_QUEUE_GROUP, streamId);

    logger.warn(
      { executionId, jobId: job.job_id, streamId },
      "[recovery] recovered abandoned execution",
    );
  } finally {
    // token is only ever set on the skipFreshnessCheck (fast-path) branch;
    // releaseLock is a harmless no-op if called with a null token, but
    // skip it outright on the sweep path to avoid the wasted round trip.
    if (token) await releaseLock(executionId, token);
  }
}

export { recoverExecution };

export async function runRecoveryCycle() {
  await ensureExecutionQueueGroup();

  const staleEntries = await getStalePendingEntries();
  if (staleEntries.length === 0) return;

  logger.info({ count: staleEntries.length }, "[recovery] found stale pending entries");

  for (const [streamId, , idleMsRaw, deliveryCountRaw] of staleEntries) {
    try {
      // We don't have execution_id (or timeout_ms) without reading the
      // payload first, and recoverExecution itself will XCLAIM to read it
      // - so resolve them via a cheap XRANGE first (no claim side
      // effects). recoverExecution needs execution_id up front regardless
      // of locking (it's used for the status-hash lookup before any claim
      // happens); the sweep path no longer takes a Redis lock on it,
      // since XCLAIM's own min-idle-time check already serializes
      // concurrent recovery attempts on the same entry.
      const range = await redis.xrange(EXECUTION_QUEUE_KEY, streamId, streamId);
      if (range.length === 0) continue; // acked/trimmed between XPENDING and now
      const raw = extractPayload(range[0][1]);
      if (!raw) {
        logger.error({ streamId }, "[recovery] pending entry missing payload field, acking to drop it");
        await redis.xack(EXECUTION_QUEUE_KEY, EXECUTION_QUEUE_GROUP, streamId);
        continue;
      }
      const { execution_id: executionId, timeout_ms: timeoutMs } = JSON.parse(raw);

      // Staleness is relative to *this* job's own timeout, not the scan
      // floor above - a job legitimately still running (worker's own
      // AbortController hasn't even fired yet, let alone finished writing
      // back) must not be reclaimed out from under it. See the
      // RECOVERY_SAFETY_MARGIN_MS comment near the top of this file.
      const staleThresholdMs = (timeoutMs ?? DEFAULT_HTTP_TIMEOUT_MS) + RECOVERY_SAFETY_MARGIN_MS;
      const idleMs = Number(idleMsRaw) || 0;
      if (idleMs < staleThresholdMs) {
        logger.debug(
          { executionId, streamId, idleMs, staleThresholdMs },
          "[recovery] entry idle but still within its own job's timeout budget, skipping this cycle",
        );
        continue;
      }

      await recoverExecution(executionId, streamId, { minIdleMs: staleThresholdMs });
    } catch (err) {
      logger.error({ err, streamId, deliveryCount: deliveryCountRaw }, "[recovery] unexpected error recovering entry");
    }
  }
}