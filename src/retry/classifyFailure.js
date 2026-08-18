// Decides whether a failed execution result is worth retrying.
//
// Default is retryable. Permanent is an explicit whitelist below - a new
// failure shape added later (new redirect check, new error type) falls
// through to retryable by default rather than silently becoming permanent.
//
// Keys off `result.failureReason`, a tag set at the source in
// executeHttpJob's deterministic return paths (worker.js) - not off
// parsing `result.error` text, which is a human-readable message and not
// a stable contract.
//
// Only called on genuine HTTP-attempt outcomes (worker.js's
// executeHttpJob result). recovery.js's give-up conditions (poison-pill
// delivery count, abandoned-with-no-attempts-left) are operational, not
// a classification of *why* the call failed, so they never call this -
// they always resolve to 'failed_max_retries' directly.
//
// Do not call this on a successful result.

// Deterministic outcomes: same URL / same policy / same target on every
// attempt, so retrying can't change the outcome.
const PERMANENT_FAILURE_REASONS = new Set([
  "ssrf_blocked",
  "redirect_loop",
  "redirect_policy_violation", // cross-origin blocked, or https->http downgrade blocked
  "redirect_missing_location",
  "redirect_max_exceeded",
  "redirect_mode_error",
]);

// 4xx codes that ARE retryable despite being 4xx - transient/rate-limit
// flavored rather than "the request itself is wrong."
const RETRYABLE_4XX = new Set([408, 409, 425, 429]);

export function classifyFailure(result) {
  const { responseStatus, failureReason } = result;

  if (failureReason && PERMANENT_FAILURE_REASONS.has(failureReason)) {
    return "permanent";
  }

  if (responseStatus != null && responseStatus >= 400 && responseStatus < 500) {
    return RETRYABLE_4XX.has(responseStatus) ? "retryable" : "permanent";
  }

  // Everything else - 5xx, network/fetch errors, timeouts, no response at
  // all with an unrecognized/untagged failure - defaults retryable.
  return "retryable";
}