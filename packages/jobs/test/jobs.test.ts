import { test } from "node:test";
import assert from "node:assert/strict";
import pg from "pg";
import {
  DEFAULT_RETRY_POLICY,
  JOBS_TABLE,
  backoffS,
  enqueue,
  runJobs,
  runErrorToString,
  validateKinds,
  validateRetryPolicy,
  validateTiming,
  validateWorkspace,
  type JobContract,
  type Queryable,
} from "../src/index.js";

// The validation and backoff rules are pure, so they run everywhere. The queue
// behaviour needs a real Postgres (FOR UPDATE SKIP LOCKED, intervals, unique
// partial indexes); those cases self-skip unless POSTGRES_URL is set, the same
// way sol-kafka's broker-backed tests do.

test("retry policy: a zero maxAttempts is rejected; negative means unlimited", () => {
  assert.match(
    validateRetryPolicy({ ...DEFAULT_RETRY_POLICY, maxAttempts: 0 })?.message ?? "",
    /must be nonzero/,
  );
  assert.equal(validateRetryPolicy({ ...DEFAULT_RETRY_POLICY, maxAttempts: -1 }), undefined);
});

test("retry policy: delays must be finite and non-negative, jitter within [0, 1]", () => {
  assert.match(
    validateRetryPolicy({ ...DEFAULT_RETRY_POLICY, baseDelayS: -1 })?.message ?? "",
    /baseDelayS/,
  );
  assert.match(
    validateRetryPolicy({ ...DEFAULT_RETRY_POLICY, maxDelayS: Number.POSITIVE_INFINITY })?.message ?? "",
    /maxDelayS/,
  );
  assert.match(
    validateRetryPolicy({ ...DEFAULT_RETRY_POLICY, jitterRatio: 1.5 })?.message ?? "",
    /jitterRatio/,
  );
});

test("timing: poll interval and lease must be positive and finite", () => {
  assert.equal(validateTiming(1, 300), undefined);
  assert.match(validateTiming(0, 300)?.message ?? "", /pollIntervalS/);
  assert.match(validateTiming(1, 0)?.message ?? "", /leaseS/);
});

test("kinds: empty is rejected, and so is a kind with a character nothing would match", () => {
  assert.match(validateKinds([])?.message ?? "", /empty/);
  assert.equal(validateKinds(["send-email", "gen_pdf.v2"]), undefined);
  assert.match(validateKinds(["Send Email"])?.message ?? "", /invalid/);
});

test("workspace: empty, over-length and invalid characters are all rejected", () => {
  assert.match(validateWorkspace("")?.message ?? "", /empty/);
  assert.match(validateWorkspace("x".repeat(64))?.message ?? "", /longer than the 63-byte limit/);
  assert.equal(validateWorkspace("pluto.notify_worker"), undefined);
  assert.match(validateWorkspace("has space")?.message ?? "", /invalid/);
});

test("backoff: exponential, clamped, and jitterRatio 0 never consults the RNG", () => {
  let calls = 0;
  const rng = () => {
    calls += 1;
    return 0.5;
  };
  const policy = { ...DEFAULT_RETRY_POLICY, baseDelayS: 1, maxDelayS: 8, jitterRatio: 0 };
  assert.deepEqual([1, 2, 3, 4, 5].map((a) => backoffS(policy, a, rng)), [1, 2, 4, 8, 8]);
  assert.equal(calls, 0, "no jitter means no RNG");
});

test("backoff: symmetric jitter is applied before the clamp", () => {
  const policy = { ...DEFAULT_RETRY_POLICY, baseDelayS: 4, maxDelayS: 100, jitterRatio: 0.5 };
  assert.equal(backoffS(policy, 1, () => 0), 2); // 4 * (1 - 0.5)
  assert.equal(backoffS(policy, 1, () => 1), 6); // 4 * (1 + 0.5)
  assert.equal(backoffS(policy, 1, () => 0.5), 4); // jitter cancels out
});

test("runErrorToString names the class of failure", () => {
  assert.match(runErrorToString({ kind: "config", message: "x" }), /invalid configuration/);
  assert.match(runErrorToString({ kind: "database", message: "x" }), /job table unusable/);
});

interface Email {
  readonly user: string;
}

const contract: JobContract<Email> = {
  workspace: "pluto.notify_worker",
  kinds: ["send_email"],
  kind: () => "send_email",
  encode: (job) => JSON.stringify(job),
  decode: (payload) => JSON.parse(payload) as Email,
  handle: async () => {},
};

const POSTGRES_URL = process.env.POSTGRES_URL;
const withDb = POSTGRES_URL ? test : test.skip;

const MIGRATION = `
CREATE TABLE IF NOT EXISTS ${JOBS_TABLE} (
  id           SERIAL      PRIMARY KEY,
  workspace    TEXT        NOT NULL,
  kind         TEXT        NOT NULL,
  payload      TEXT        NOT NULL,
  status       TEXT        NOT NULL DEFAULT 'pending',
  attempts     INT         NOT NULL DEFAULT 0,
  run_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  locked_until TIMESTAMPTZ,
  last_error   TEXT,
  dedupe_key   TEXT,
  inserted_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at  TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS sol_jobs_claim_idx ON ${JOBS_TABLE} (workspace, run_at) WHERE status = 'pending';
CREATE UNIQUE INDEX IF NOT EXISTS sol_jobs_dedupe_idx
  ON ${JOBS_TABLE} (workspace, kind, dedupe_key) WHERE dedupe_key IS NOT NULL;
`;

async function withFreshTable(body: (pool: pg.Pool) => Promise<void>): Promise<void> {
  const pool = new pg.Pool({ connectionString: POSTGRES_URL, max: 4 });
  try {
    await pool.query(`DROP TABLE IF EXISTS ${JOBS_TABLE}`);
    await pool.query(MIGRATION);
    await body(pool);
  } finally {
    await pool.end();
  }
}

async function rows(pool: pg.Pool): Promise<Array<Record<string, unknown>>> {
  const result = await pool.query(
    `SELECT status, attempts, last_error, dedupe_key, run_at > now() AS future FROM ${JOBS_TABLE} ORDER BY id`,
  );
  return result.rows;
}

withDb("enqueue is idempotent on (workspace, kind, dedupe_key)", async () => {
  await withFreshTable(async (pool) => {
    await enqueue(pool as unknown as Queryable, contract, { user: "a" }, { dedupeKey: "fact-1" });
    await enqueue(pool as unknown as Queryable, contract, { user: "a" }, { dedupeKey: "fact-1" });
    await enqueue(pool as unknown as Queryable, contract, { user: "b" }, { dedupeKey: "fact-2" });
    assert.equal((await rows(pool)).length, 2);
  });
});

withDb("enqueue joins the caller's transaction: a rollback leaves no job", async () => {
  await withFreshTable(async (pool) => {
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      await enqueue(client as unknown as Queryable, contract, { user: "a" }, { dedupeKey: "tx" });
      await client.query("ROLLBACK");
    } finally {
      client.release();
    }
    assert.equal((await rows(pool)).length, 0, "the job must not outlive the rolled-back state change");

    await pool.query("BEGIN");
    await enqueue(pool as unknown as Queryable, contract, { user: "a" }, { dedupeKey: "tx" });
    await pool.query("COMMIT");
    assert.equal((await rows(pool)).length, 1, "a committed transaction keeps the job");
  });
});

withDb("the runner claims, handles and completes a job exactly once", async () => {
  await withFreshTable(async (pool) => {
    await enqueue(pool as unknown as Queryable, contract, { user: "a" });
    let handled = 0;
    const controller = new AbortController();
    const error = await runJobs<Email>({
      pool,
      contract: { ...contract, handle: async () => { handled += 1; } },
      pollIntervalS: 0.05,
      onOutcome: () => controller.abort(),
      signal: controller.signal,
    });
    assert.equal(error, undefined);
    assert.equal(handled, 1);
    const [row] = await rows(pool);
    assert.equal(row?.status, "completed");
    assert.equal(row?.attempts, 1);
  });
});

withDb("a failing handler retries with backoff and records the error", async () => {
  await withFreshTable(async (pool) => {
    await enqueue(pool as unknown as Queryable, contract, { user: "a" });
    const controller = new AbortController();
    const error = await runJobs<Email>({
      pool,
      contract: { ...contract, handle: async () => { throw new Error("smtp down"); } },
      pollIntervalS: 0.05,
      retryPolicy: { ...DEFAULT_RETRY_POLICY, jitterRatio: 0, baseDelayS: 30, maxAttempts: 5 },
      onOutcome: () => controller.abort(),
      signal: controller.signal,
    });
    assert.equal(error, undefined);
    const [row] = await rows(pool);
    assert.equal(row?.status, "pending", "a retry stays pending");
    assert.equal(row?.attempts, 1);
    assert.equal(row?.last_error, "smtp down");
    assert.equal(row?.future, true, "run_at is pushed into the future by the backoff");
  });
});

withDb("attempts exhausted leaves a terminal failed row", async () => {
  await withFreshTable(async (pool) => {
    await enqueue(pool as unknown as Queryable, contract, { user: "a" });
    const controller = new AbortController();
    const error = await runJobs<Email>({
      pool,
      contract: { ...contract, handle: async () => { throw new Error("permanent"); } },
      pollIntervalS: 0.05,
      retryPolicy: { ...DEFAULT_RETRY_POLICY, maxAttempts: 1 },
      onOutcome: () => controller.abort(),
      signal: controller.signal,
    });
    assert.equal(error, undefined);
    const [row] = await rows(pool);
    assert.equal(row?.status, "failed");
    assert.equal(row?.last_error, "permanent");
  });
});

withDb("an unreadable table is a database error naming the migration, not a crash", async () => {
  const pool = new pg.Pool({ connectionString: POSTGRES_URL });
  try {
    await pool.query(`DROP TABLE IF EXISTS ${JOBS_TABLE}`);
    const error = await runJobs<Email>({ pool, contract, pollIntervalS: 0.05 });
    assert.equal(error?.kind, "database");
    assert.match(error?.message ?? "", /migration/);
  } finally {
    await pool.end();
  }
});

// BUG-125: the renewal heartbeat used to leave a `leaseS / 3` timer pending
// after the handler finished, holding the Node event loop open (with the default
// lease that is a 100-second linger after the last job completes). `sleep` now
// takes the renewal's abort signal, and the handler aborts it in a `finally`.
// `leaseS: 3` makes the renewal sleep `1` second, distinct from the `0.05`
// poll interval, so the injected sleep can tell the two apart.
withDb("the lease-renewal sleep is aborted when the handler finishes", async () => {
  await withFreshTable(async (pool) => {
    await enqueue(pool as unknown as Queryable, contract, { user: "a" });
    const controller = new AbortController();
    let renewalSignal: AbortSignal | undefined;
    let renewalSleepWithoutSignal = false;
    const sleep = (seconds: number, signal?: AbortSignal): Promise<void> => {
      if (Math.abs(seconds - 1) >= 1e-9) return Promise.resolve();
      if (signal === undefined) {
        renewalSleepWithoutSignal = true;
        return Promise.resolve();
      }
      renewalSignal = signal;
      return new Promise<void>((resolve) => {
        if (signal.aborted) {
          resolve();
          return;
        }
        signal.addEventListener("abort", () => resolve(), { once: true });
      });
    };
    const error = await runJobs<Email>({
      pool,
      contract: { ...contract, handle: async () => {} },
      pollIntervalS: 0.05,
      leaseS: 3,
      sleep,
      onOutcome: () => controller.abort(),
      signal: controller.signal,
    });
    assert.equal(error, undefined);
    assert.equal(renewalSleepWithoutSignal, false, "the renewal sleep must receive an abort signal");
    assert.ok(renewalSignal, "the renewal sleep must run");
    assert.equal(renewalSignal.aborted, true, "the renewal sleep must be aborted once the handler finishes");
  });
});
