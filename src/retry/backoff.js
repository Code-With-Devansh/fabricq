// Pure delay calculators - no I/O, no Redis, no Postgres. Kept separate from
// retry.js so the formulas can be unit tested in isolation and swapped
// without touching any queueing/locking logic.

function fibonacci(n) {
  // fib(1) = 1, fib(2) = 1, fib(3) = 2, fib(4) = 3, fib(5) = 5, ...
  let a = 0;
  let b = 1;
  for (let i = 0; i < n; i++) {
    [a, b] = [b, a + b];
  }
  return a;
}

// attempt is 1-indexed: the attempt number about to be tried (i.e. the one
// that just failed, plus one - see scheduleRetry.js). computeDelaySeconds
// answers "how long to wait before that attempt".
//
// retryAfterSeconds, when given, is a pre-parsed, already-validated-positive
// Retry-After delay (see parseRetryAfter below). It's only honored when
// job.honor_retry_after is true - the caller (scheduleRetry.js) is
// responsible for deciding whether the failure was even retryable; this
// function just substitutes the delay for that one attempt and still runs
// it through the same cap as every other strategy.
export function computeDelaySeconds(job, attempt, retryAfterSeconds = null) {
  if (job.honor_retry_after && retryAfterSeconds != null) {
    return applyCap(retryAfterSeconds, job.retry_max_seconds);
  }

  const base = job.backoff_seconds;
  const multiplier = Number(job.retry_multiplier);

  let delay;
  switch (job.retry_strategy) {
    case "IMMEDIATE":
      delay = 0;
      break;
    case "FIXED":
      delay = base;
      break;
    case "LINEAR":
      delay = base * attempt;
      break;
    case "EXPONENTIAL":
      delay = base * Math.pow(multiplier, attempt - 1);
      break;
    case "EXPONENTIAL_JITTER": {
      // Full jitter (AWS Architecture Blog): spreads retries across the
      // whole window instead of clustering at the exponential value,
      // which matters when many jobs are backing off against the same
      // failing downstream.
      const exp = base * Math.pow(multiplier, attempt - 1);
      delay = Math.random() * exp;
      break;
    }
    case "FIBONACCI":
      delay = base * fibonacci(attempt);
      break;
    default:
      // Unknown strategy - fail safe to FIXED rather than throwing, since
      // this runs on the hot path of every retry.
      delay = base;
  }

  return applyCap(delay, job.retry_max_seconds);
}

// cap === null/undefined means "no ceiling" (see migration 028) - applies
// uniformly to every strategy's output, not just Retry-After.
function applyCap(delay, cap) {
  const floor = Math.max(0, Math.round(delay));
  return cap == null ? floor : Math.min(floor, cap);
}

// Parses a Retry-After header value (RFC 7231 §7.1.3): either delta-seconds
// ("120") or an HTTP-date. Returns a positive integer number of seconds to
// wait, or null if the header is absent, unparseable, or would resolve to a
// non-future point in time (stale/past date, "0", negative) - callers treat
// null as "don't honor it, fall back to the configured strategy".
export function parseRetryAfter(headerValue) {
  if (!headerValue) return null;

  const trimmed = headerValue.trim();

  if (/^\d+$/.test(trimmed)) {
    const seconds = Number(trimmed);
    return seconds > 0 ? seconds : null;
  }

  const dateMs = Date.parse(trimmed);
  if (Number.isNaN(dateMs)) return null;

  const deltaSeconds = Math.round((dateMs - Date.now()) / 1000);
  return deltaSeconds > 0 ? deltaSeconds : null;
}