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

// attempt is 1-indexed: the attempt number that just failed (job.attempts
// after the worker/recovery increment). computeDelaySeconds answers "how
// long to wait before the *next* attempt".
export function computeDelaySeconds(job, attempt) {
  const base = job.backoff_seconds;
  const multiplier = Number(job.retry_multiplier);
  const cap = job.retry_max_seconds;

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

  return Math.min(Math.max(0, Math.round(delay)), cap);
}
