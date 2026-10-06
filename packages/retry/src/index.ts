/**
 * Sol's operation-level retry helper for TypeScript, the counterpart to the
 * OCaml `sol-retry` (`framework/ocaml/sol-retry`).
 *
 * Retry the *operation* -- the dependency call inside a handler -- never the
 * message or the handler. A worker's outcome vocabulary stays exactly
 * `Ack | Fail`; re-running a handler is a redelivery decision, not a retry.
 *
 * One bounded, jittered policy vocabulary is shared with `@sol-fab/jobs`:
 * `baseDelayS`, `maxDelayS`, `maxAttempts`, `jitterRatio`. `maxAttempts`
 * negative means unbounded and zero is refused. Attempts are separated with an
 * `await`ed delay rather than a blocking sleep, cancellation propagates through
 * an `AbortSignal`, and exhaustion returns the last error to the caller, who
 * decides what it means. The retried operation must be safe to repeat.
 */

export interface RetryPolicy {
  /** Initial backoff in seconds; doubles on each consecutive failure. */
  readonly baseDelayS: number;
  /** Backoff cap, even after jitter. */
  readonly maxDelayS: number;
  /** Maximum operation invocations. Negative = retry indefinitely. */
  readonly maxAttempts: number;
  /** Symmetric jitter as a fraction of the raw delay (0.1 = +-10%). 0 disables it. */
  readonly jitterRatio: number;
}

export const DEFAULT_RETRY_POLICY: RetryPolicy = {
  baseDelayS: 1.0,
  maxDelayS: 600.0,
  maxAttempts: 5,
  jitterRatio: 0.1,
};

export interface PolicyError {
  readonly kind: "config";
  readonly message: string;
}

export class RetryPolicyError extends Error {
  readonly kind = "config";

  constructor(message: string) {
    super(`sol-retry: invalid policy: ${message}`);
    this.name = "RetryPolicyError";
  }
}

export function validateRetryPolicy(policy: RetryPolicy): PolicyError | undefined {
  const nonNegativeFinite = (name: string, value: number): PolicyError | undefined =>
    Number.isFinite(value) && value >= 0
      ? undefined
      : { kind: "config", message: `retryPolicy.${name} must be a finite number >= 0 (got ${value})` };
  if (policy.maxAttempts === 0) {
    return { kind: "config", message: "retryPolicy.maxAttempts must be nonzero (negative = unlimited)" };
  }
  return (
    nonNegativeFinite("baseDelayS", policy.baseDelayS) ??
    nonNegativeFinite("maxDelayS", policy.maxDelayS) ??
    (Number.isFinite(policy.jitterRatio) && policy.jitterRatio >= 0 && policy.jitterRatio <= 1
      ? undefined
      : {
          kind: "config",
          message: `retryPolicy.jitterRatio must be a finite number within [0, 1] (got ${policy.jitterRatio})`,
        })
  );
}

export function backoffS(
  policy: RetryPolicy,
  attempt: number,
  rng: () => number = Math.random,
): number {
  const raw = policy.baseDelayS * 2 ** (attempt - 1);
  if (policy.jitterRatio <= 0) return Math.min(policy.maxDelayS, Math.max(0, raw));
  const jitterUnit = rng() * (2 * policy.jitterRatio);
  const jittered = raw * (1 + (jitterUnit - policy.jitterRatio));
  return Math.min(policy.maxDelayS, Math.max(0, jittered));
}

/** Resolve after `ms` milliseconds, or reject as soon as `signal` aborts. */
export type Sleep = (ms: number, signal?: AbortSignal) => Promise<void>;

export const defaultSleep: Sleep = (ms, signal) =>
  new Promise((resolve, reject) => {
    if (signal?.aborted) {
      reject(signal.reason ?? new Error("sol-retry: aborted"));
      return;
    }
    const onAbort = () => {
      clearTimeout(timer);
      reject(signal?.reason ?? new Error("sol-retry: aborted"));
    };
    const timer = setTimeout(() => {
      signal?.removeEventListener("abort", onAbort);
      resolve();
    }, ms);
    signal?.addEventListener("abort", onAbort, { once: true });
  });

export interface RetryOptions {
  /** Defaults to `DEFAULT_RETRY_POLICY`. */
  readonly policy?: RetryPolicy;
  /** Cancels the wait and stops further attempts. */
  readonly signal?: AbortSignal;
  /** Jitter source; defaults to `Math.random`. */
  readonly rng?: () => number;
  /** Test seam; defaults to the timer-backed `defaultSleep`. */
  readonly sleep?: Sleep;
  /** Observes each failed attempt before its backoff wait. */
  readonly onAttemptFailed?: (error: unknown, attempt: number) => void;
}

function abortReason(signal: AbortSignal): unknown {
  return signal.reason ?? new Error("sol-retry: aborted");
}

/**
 * Retry `operation` in place until it resolves, the policy's attempt budget is
 * exhausted, or `signal` aborts. Throws the last operation error (or the abort
 * reason) on failure.
 */
export async function retry<T>(
  operation: () => Promise<T>,
  options: RetryOptions = {},
): Promise<T> {
  const policy = options.policy ?? DEFAULT_RETRY_POLICY;
  const validation = validateRetryPolicy(policy);
  if (validation) throw new RetryPolicyError(validation.message);
  const sleep = options.sleep ?? defaultSleep;
  const rng = options.rng ?? Math.random;
  let attempt = 1;
  for (;;) {
    if (options.signal?.aborted) throw abortReason(options.signal);
    try {
      return await operation();
    } catch (error) {
      if (options.signal?.aborted) throw abortReason(options.signal);
      if (policy.maxAttempts >= 0 && attempt >= policy.maxAttempts) throw error;
      options.onAttemptFailed?.(error, attempt);
      await sleep(backoffS(policy, attempt, rng) * 1000, options.signal);
      attempt += 1;
    }
  }
}

export interface Retry {
  readonly policy: RetryPolicy;
  run<T>(operation: () => Promise<T>, options?: Omit<RetryOptions, "policy">): Promise<T>;
}

/** Validate a policy once, then run many operations under it. */
export function retryWith(policy: RetryPolicy): Retry {
  const validation = validateRetryPolicy(policy);
  if (validation) throw new RetryPolicyError(validation.message);
  return {
    policy,
    run: (operation, options = {}) => retry(operation, { ...options, policy }),
  };
}
