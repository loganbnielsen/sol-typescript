/**
 * Sol's durable Postgres job queue for TypeScript, mirroring `sol-jobs`
 * (framework/ocaml/sol-jobs) in capability and observable behaviour rather than
 * implementation (DEC-022):
 *
 *   - `enqueue` is a plain `INSERT`, so calling it with a pool handle from
 *     inside a transaction puts the job in the *same* Postgres transaction as
 *     the state change that caused it -- no dual-write hole, and no second
 *     broker. That is the one thing Kafka structurally cannot give you.
 *   - the runner claims with `FOR UPDATE SKIP LOCKED`, holds a lease it renews
 *     while the handler runs, backs off between attempts, and leaves a terminal
 *     row when the attempts are exhausted.
 *
 * It is a library, not a fourth deployable primitive (DEC-021): host it from an
 * ordinary `-worker` binary by calling `runJobs` instead of consuming a topic.
 * Postgres says "this must happen"; Kafka says "this happened" and belongs to
 * `@sol-fab/kafka`.
 */
import type { Pool } from "pg";

export interface RetryPolicy {
  /** Initial backoff in seconds; doubles on each consecutive failure. */
  readonly baseDelayS: number;
  /** Backoff cap, even after jitter. */
  readonly maxDelayS: number;
  /** Maximum handler invocations. Negative = retry indefinitely. */
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

export const DEFAULT_POLL_INTERVAL_S = 1.0;
export const DEFAULT_LEASE_S = 300.0;
export const DEFAULT_MAX_CLAIM_FAILURES = 30;
export const DEFAULT_TERMINAL_RETENTION_S = 604_800.0;
export const DEFAULT_SWEEP_INTERVAL_S = 60.0;

/** The shared table. An app migration must create it; see the sol-jobs spec. */
export const JOBS_TABLE = "sol_jobs";

/** The metric names `sol-jobs` emits, for a cross-language dashboard. */
export const SOL_JOBS_PROCESSED_TOTAL = "sol_jobs_processed_total";
export const SOL_JOBS_JOB_DURATION_SECONDS = "sol_jobs_job_duration_seconds";

export type RunError =
  | { readonly kind: "config"; readonly message: string }
  | { readonly kind: "database"; readonly message: string };

export function runErrorToString(error: RunError): string {
  return error.kind === "config"
    ? `sol-jobs: invalid configuration: ${error.message}`
    : `sol-jobs: job table unusable: ${error.message}`;
}

/**
 * Anything that can run a query: a `pg` `Pool` or a `PoolClient`. Passing the
 * client from inside `BEGIN` is what makes the enqueue transactional.
 */
export interface Queryable {
  query(text: string, values?: unknown[]): Promise<unknown>;
}

export interface JobContract<T> {
  /** The workspace whose rows this queue owns; rows of other workspaces are never claimed. */
  readonly workspace: string;
  /** Every kind this worker can claim. A kind outside it would never be claimed. */
  readonly kinds: readonly string[];
  readonly kind: (job: T) => string;
  readonly encode: (job: T) => string;
  /** Throw to reject a payload; the job is retried or failed, never silently dropped. */
  readonly decode: (payload: string) => T;
  /** Resolve for success; throw to fail the attempt. */
  readonly handle: (job: T) => Promise<void>;
}

export interface EnqueueOptions {
  /** Seconds since the epoch; defaults to now. */
  readonly runAt?: number;
  /** Idempotency key, unique per (workspace, kind). */
  readonly dedupeKey?: string;
}

const MAX_WORKSPACE_LENGTH = 63;

function isKindChar(c: string): boolean {
  return /[a-z0-9_.-]/.test(c);
}

function isWorkspaceChar(c: string): boolean {
  return /[a-zA-Z0-9_.-]/.test(c);
}

export function validateRetryPolicy(policy: RetryPolicy): RunError | undefined {
  const nonNegativeFinite = (name: string, value: number): RunError | undefined =>
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

export function validateTiming(pollIntervalS: number, leaseS: number): RunError | undefined {
  const positiveFinite = (name: string, value: number): RunError | undefined =>
    Number.isFinite(value) && value > 0
      ? undefined
      : { kind: "config", message: `${name} must be a finite number > 0 (got ${value})` };
  return positiveFinite("pollIntervalS", pollIntervalS) ?? positiveFinite("leaseS", leaseS);
}

export function validateKinds(kinds: readonly string[]): RunError | undefined {
  if (kinds.length === 0) {
    return { kind: "config", message: "kinds is empty -- a poller that claims no kind would do nothing" };
  }
  const bad = kinds.find((k) => k === "" || ![...k].every(isKindChar));
  return bad === undefined
    ? undefined
    : {
        kind: "config",
        message: `kinds entry ${JSON.stringify(bad)} is invalid: kinds are non-empty and use only a-z, 0-9, _, ., -`,
      };
}

export function validateWorkspace(workspace: string): RunError | undefined {
  if (workspace === "") {
    return {
      kind: "config",
      message: "workspace is empty -- a queue with no workspace identity would claim every other workspace's rows",
    };
  }
  if (workspace.length > MAX_WORKSPACE_LENGTH) {
    return {
      kind: "config",
      message: `workspace is ${workspace.length} bytes, longer than the ${MAX_WORKSPACE_LENGTH}-byte limit`,
    };
  }
  return [...workspace].every(isWorkspaceChar)
    ? undefined
    : { kind: "config", message: `workspace ${JSON.stringify(workspace)} is invalid: use only a-z, A-Z, 0-9, _, ., -` };
}

export function validateRetention(terminalRetentionS: number, sweepIntervalS: number): RunError | undefined {
  const nonNegative = (name: string, value: number): RunError | undefined =>
    Number.isFinite(value) && value >= 0
      ? undefined
      : { kind: "config", message: `${name} must be a finite number >= 0 (got ${value})` };
  return nonNegative("terminalRetentionS", terminalRetentionS) ?? nonNegative("sweepIntervalS", sweepIntervalS);
}

/**
 * `sol_jobs.ml`'s `backoff_s`: exponential from `attempt`, symmetric jitter
 * applied *before* the `maxDelayS` clamp, and no RNG consultation at all when
 * `jitterRatio` is 0.
 */
export function backoffS(policy: RetryPolicy, attempt: number, rng: () => number = Math.random): number {
  const raw = policy.baseDelayS * 2 ** (attempt - 1);
  if (policy.jitterRatio <= 0) return Math.min(policy.maxDelayS, Math.max(0, raw));
  const jitterUnit = rng() * (2 * policy.jitterRatio);
  const jittered = raw * (1 + (jitterUnit - policy.jitterRatio));
  return Math.min(policy.maxDelayS, Math.max(0, jittered));
}

/**
 * Enqueue a job. Pass a `PoolClient` from inside a transaction to make the
 * enqueue part of that transaction; pass the `Pool` to enqueue standalone.
 * A repeated `dedupeKey` for the same (workspace, kind) is a no-op.
 */
export async function enqueue<T>(
  client: Queryable,
  contract: JobContract<T>,
  job: T,
  options: EnqueueOptions = {},
): Promise<void> {
  const kind = contract.kind(job);
  if (!contract.kinds.includes(kind)) {
    throw new Error(`sol-jobs: kind ${JSON.stringify(kind)} is not in kinds; nothing would claim it`);
  }
  const workspaceError = validateWorkspace(contract.workspace);
  if (workspaceError) throw new Error(`sol-jobs: ${workspaceError.message}`);

  await client.query(
    `INSERT INTO ${JOBS_TABLE} (workspace, kind, payload, run_at, dedupe_key)
     VALUES ($1, $2, $3, to_timestamp($4), $5)
     ON CONFLICT (workspace, kind, dedupe_key) WHERE dedupe_key IS NOT NULL DO NOTHING`,
    [contract.workspace, kind, contract.encode(job), options.runAt ?? Date.now() / 1000, options.dedupeKey ?? null],
  );
}

export interface JobOutcome {
  readonly kind: string;
  readonly status: "ok" | "retry" | "failed";
  readonly durationS: number;
}

export interface RunJobsOptions<T> {
  readonly pool: Pool;
  readonly contract: JobContract<T>;
  readonly retryPolicy?: RetryPolicy;
  readonly pollIntervalS?: number;
  readonly leaseS?: number;
  /** Stop after this many terminal outcomes (tests). */
  readonly maxJobs?: number;
  readonly maxClaimFailures?: number;
  readonly terminalRetentionS?: number;
  readonly sweepIntervalS?: number;
  /** Fires once the table is readable and the loop is about to start. */
  readonly onReady?: () => void;
  /** Cooperative stop, e.g. from the worker's drain. */
  readonly signal?: AbortSignal;
  /** `sol_jobs_processed_total{status,kind}` and the duration histogram, if the app wires them. */
  readonly onOutcome?: (outcome: JobOutcome) => void;
  /** Lease-lost and query-failure warnings; defaults to `console.warn`. */
  readonly onWarning?: (fields: Readonly<Record<string, string>>, message: string) => void;
  readonly sleep?: (seconds: number, signal?: AbortSignal) => Promise<void>;
  readonly now?: () => number;
  readonly rng?: () => number;
}

interface ClaimedRow {
  id: number;
  kind: string;
  payload: string;
  attempts: number;
  status: string;
}

const CLAIM_SQL = `WITH candidate AS (
    SELECT id, attempts, $1::int AS budget, $2::float8 AS lease FROM ${JOBS_TABLE}
    WHERE status = 'pending'
      AND workspace = $3
      AND kind = ANY(string_to_array($4, ','))
      AND run_at <= now()
      AND (locked_until IS NULL OR locked_until <= now())
    ORDER BY run_at
    FOR UPDATE SKIP LOCKED
    LIMIT 1
  )
  UPDATE ${JOBS_TABLE} AS job
  SET status = CASE WHEN candidate.attempts >= candidate.budget AND candidate.budget >= 0
                    THEN 'failed' ELSE 'pending' END,
      locked_until = CASE WHEN candidate.attempts >= candidate.budget AND candidate.budget >= 0
                          THEN NULL ELSE now() + (candidate.lease * interval '1 second') END,
      last_error = CASE WHEN candidate.attempts >= candidate.budget AND candidate.budget >= 0
                        THEN 'worker stopped before finishing the previous attempt'
                        ELSE job.last_error END,
      finished_at = CASE WHEN candidate.attempts >= candidate.budget AND candidate.budget >= 0
                         THEN now() ELSE job.finished_at END,
      attempts = CASE WHEN candidate.attempts >= candidate.budget AND candidate.budget >= 0
                      THEN job.attempts ELSE job.attempts + 1 END
  FROM candidate WHERE job.id = candidate.id
  RETURNING job.id, job.kind, job.payload, job.attempts, job.status`;

const COMPLETE_SQL = `UPDATE ${JOBS_TABLE}
  SET status = 'completed', finished_at = now(), locked_until = NULL
  WHERE id = $1 AND attempts = $2 AND workspace = $3 AND status = 'pending'
  RETURNING id`;

const RETRY_SQL = `UPDATE ${JOBS_TABLE}
  SET run_at = now() + ($1::float8 * interval '1 second'), locked_until = NULL, last_error = $2
  WHERE id = $3 AND attempts = $4 AND workspace = $5 AND status = 'pending'
  RETURNING id`;

const FAIL_SQL = `UPDATE ${JOBS_TABLE}
  SET status = 'failed', finished_at = now(), locked_until = NULL, last_error = $1
  WHERE id = $2 AND attempts = $3 AND workspace = $4 AND status = 'pending'
  RETURNING id`;

const RENEW_SQL = `UPDATE ${JOBS_TABLE}
  SET locked_until = now() + ($1::float8 * interval '1 second')
  WHERE id = $2 AND attempts = $3 AND workspace = $4 AND status = 'pending' AND locked_until > now()
  RETURNING id`;

const SWEEP_SQL = `DELETE FROM ${JOBS_TABLE}
  WHERE status <> 'pending' AND workspace = $1 AND finished_at IS NOT NULL
    AND finished_at < now() - ($2::float8 * interval '1 second')`;

const defaultSleep = (seconds: number, signal?: AbortSignal): Promise<void> =>
  new Promise((resolve) => {
    if (signal?.aborted) {
      resolve();
      return;
    }
    const timer = setTimeout(resolve, Math.max(0, seconds) * 1000);
    signal?.addEventListener(
      "abort",
      () => {
        clearTimeout(timer);
        resolve();
      },
      { once: true },
    );
  });

/**
 * Own the polling loop: claim, handle, complete/retry/fail, sweep. Returns
 * `undefined` on a clean stop (drain or `maxJobs`), or the `RunError` that
 * stopped it.
 */
export async function runJobs<T>(options: RunJobsOptions<T>): Promise<RunError | undefined> {
  const { contract, pool } = options;
  const policy = options.retryPolicy ?? DEFAULT_RETRY_POLICY;
  const pollIntervalS = options.pollIntervalS ?? DEFAULT_POLL_INTERVAL_S;
  const leaseS = options.leaseS ?? DEFAULT_LEASE_S;
  const maxClaimFailures = options.maxClaimFailures ?? DEFAULT_MAX_CLAIM_FAILURES;
  const terminalRetentionS = options.terminalRetentionS ?? DEFAULT_TERMINAL_RETENTION_S;
  const sweepIntervalS = options.sweepIntervalS ?? DEFAULT_SWEEP_INTERVAL_S;
  const sleep = options.sleep ?? defaultSleep;
  const now = options.now ?? (() => Date.now() / 1000);
  const rng = options.rng ?? Math.random;
  const warn =
    options.onWarning ??
    ((fields: Readonly<Record<string, string>>, message: string) => {
      console.warn(message, fields);
    });
  const stopped = (): boolean => options.signal?.aborted ?? false;

  for (const error of [
    validateRetryPolicy(policy),
    validateTiming(pollIntervalS, leaseS),
    validateKinds(contract.kinds),
    validateWorkspace(contract.workspace),
    validateRetention(terminalRetentionS, sweepIntervalS),
  ]) {
    if (error) return error;
  }

  try {
    await pool.query(`SELECT 1 FROM ${JOBS_TABLE} LIMIT 1`);
  } catch (error) {
    return {
      kind: "database",
      message:
        `cannot read the ${JOBS_TABLE} table (an app migration must create it -- see the sol-jobs spec): ` +
        String(error),
    };
  }

  options.onReady?.();

  const kindsParam = contract.kinds.join(",");
  let failures = 0;
  let remaining = options.maxJobs;
  let lastSweep = 0;

  const leaseLost = (id: number, attempts: number, action: string): void =>
    warn(
      { job_id: String(id), attempt: String(attempts), action },
      "sol-jobs: lease lost -- the job was re-claimed while this handler ran; this outcome was not " +
        "recorded and the job may have run concurrently",
    );

  const recordTerminal = (outcome: JobOutcome): void => {
    if (remaining !== undefined) remaining -= 1;
    options.onOutcome?.(outcome);
  };

  const runHandler = async (row: ClaimedRow): Promise<{ ok: true } | { ok: false; message: string }> => {
    let renewing = true;
    const renewalStop = new AbortController();
    const renewal = (async () => {
      while (renewing) {
        await sleep(leaseS / 3, renewalStop.signal);
        if (!renewing) return;
        try {
          const result = (await pool.query(RENEW_SQL, [leaseS, row.id, row.attempts, contract.workspace])) as {
            rowCount: number | null;
          };
          if (result.rowCount === 0) {
            leaseLost(row.id, row.attempts, "renew");
            return;
          }
        } catch (error) {
          warn(
            { job_id: String(row.id), error: String(error) },
            "sol-jobs: lease renewal failed",
          );
        }
      }
    })();
    try {
      let job: T;
      try {
        job = contract.decode(row.payload);
      } catch (error) {
        return { ok: false, message: error instanceof Error ? error.message : String(error) };
      }
      try {
        await contract.handle(job);
        return { ok: true };
      } catch (error) {
        return { ok: false, message: error instanceof Error ? error.message : String(error) };
      }
    } finally {
      renewing = false;
      renewalStop.abort();
      void renewal.catch(() => {});
    }
  };

  while (!stopped()) {
    if (now() - lastSweep >= sweepIntervalS) {
      try {
        await pool.query(SWEEP_SQL, [contract.workspace, terminalRetentionS]);
      } catch (error) {
        warn({ error: String(error) }, "sol-jobs: failed to sweep expired terminal jobs");
      }
      lastSweep = now();
    }
    if (stopped()) break;
    if (remaining !== undefined && remaining <= 0) break;

    let row: ClaimedRow | undefined;
    try {
      const result = (await pool.query(CLAIM_SQL, [
        policy.maxAttempts,
        leaseS,
        contract.workspace,
        kindsParam,
      ])) as { rows: ClaimedRow[] };
      row = result.rows[0];
    } catch (error) {
      failures += 1;
      if (failures >= maxClaimFailures) {
        return {
          kind: "database",
          message: `${failures} consecutive claim queries failed; last: ${String(error)}`,
        };
      }
      warn(
        { error: String(error), consecutive_failures: String(failures) },
        "sol-jobs: claim query failed",
      );
      await sleep(pollIntervalS);
      continue;
    }
    failures = 0;

    if (row === undefined) {
      await sleep(pollIntervalS);
      continue;
    }
    if (row.status === "failed") {
      recordTerminal({ kind: row.kind, status: "failed", durationS: 0 });
      continue;
    }

    const t0 = now();
    const outcome = await runHandler(row);
    const durationS = now() - t0;
    if (outcome.ok) {
      try {
        const result = (await pool.query(COMPLETE_SQL, [
          row.id,
          row.attempts,
          contract.workspace,
        ])) as { rowCount: number | null };
        if (result.rowCount === 0) leaseLost(row.id, row.attempts, "complete");
      } catch (error) {
        warn(
          { job_id: String(row.id), error: String(error) },
          "sol-jobs: failed to mark the job completed (will be reclaimed after its lease expires and re-run)",
        );
      }
      recordTerminal({ kind: row.kind, status: "ok", durationS });
      continue;
    }

    const exhausted = policy.maxAttempts >= 0 && row.attempts >= policy.maxAttempts;
    if (exhausted) {
      try {
        const result = (await pool.query(FAIL_SQL, [
          outcome.message,
          row.id,
          row.attempts,
          contract.workspace,
        ])) as { rowCount: number | null };
        if (result.rowCount === 0) leaseLost(row.id, row.attempts, "fail");
      } catch (error) {
        warn(
          { job_id: String(row.id), error: String(error) },
          "sol-jobs: failed to mark job permanently failed",
        );
      }
      recordTerminal({ kind: row.kind, status: "failed", durationS });
      continue;
    }

    const delay = backoffS(policy, row.attempts, rng);
    try {
      const result = (await pool.query(RETRY_SQL, [
        delay,
        outcome.message,
        row.id,
        row.attempts,
        contract.workspace,
      ])) as { rowCount: number | null };
      if (result.rowCount === 0) leaseLost(row.id, row.attempts, "retry");
    } catch (error) {
      warn(
        { job_id: String(row.id), error: String(error) },
        "sol-jobs: failed to schedule job retry (will be reclaimed after its lease expires and re-run)",
      );
    }
    options.onOutcome?.({ kind: row.kind, status: "retry", durationS });
  }

  return undefined;
}
