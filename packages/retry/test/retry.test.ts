import { test } from "node:test";
import assert from "node:assert/strict";
import {
  DEFAULT_RETRY_POLICY,
  RetryPolicyError,
  backoffS,
  defaultSleep,
  retry,
  retryWith,
  validateRetryPolicy,
  type RetryPolicy,
  type Sleep,
} from "../src/index.js";

// Jitter off by default so delays are deterministic; individual tests opt in.
const policy = (overrides: Partial<RetryPolicy> = {}): RetryPolicy => ({
  ...DEFAULT_RETRY_POLICY,
  jitterRatio: 0,
  ...overrides,
});

test("default policy matches the OCaml sol-retry defaults", () => {
  assert.deepEqual(DEFAULT_RETRY_POLICY, {
    baseDelayS: 1.0,
    maxDelayS: 600.0,
    maxAttempts: 5,
    jitterRatio: 0.1,
  });
});

test("validation refuses zero attempts, invalid delays and jitter outside [0, 1]", () => {
  assert.match(
    validateRetryPolicy({ ...DEFAULT_RETRY_POLICY, maxAttempts: 0 })?.message ?? "",
    /maxAttempts/,
  );
  assert.equal(validateRetryPolicy({ ...DEFAULT_RETRY_POLICY, maxAttempts: -1 }), undefined);
  assert.match(
    validateRetryPolicy({ ...DEFAULT_RETRY_POLICY, baseDelayS: -1 })?.message ?? "",
    /baseDelayS/,
  );
  assert.match(
    validateRetryPolicy({ ...DEFAULT_RETRY_POLICY, maxDelayS: Number.NaN })?.message ?? "",
    /maxDelayS/,
  );
  assert.match(
    validateRetryPolicy({ ...DEFAULT_RETRY_POLICY, jitterRatio: 1.5 })?.message ?? "",
    /jitterRatio/,
  );
  assert.match(
    validateRetryPolicy({ ...DEFAULT_RETRY_POLICY, jitterRatio: -0.1 })?.message ?? "",
    /jitterRatio/,
  );
});

test("retryWith validates once and refuses an unusable policy", () => {
  assert.throws(() => retryWith({ ...DEFAULT_RETRY_POLICY, maxAttempts: 0 }), RetryPolicyError);
  const runner = retryWith(policy({ maxAttempts: 3 }));
  assert.equal(runner.policy.maxAttempts, 3);
});

test("a transient failure is retried and the successful value returned", async () => {
  const delays: number[] = [];
  let calls = 0;
  const value = await retry(
    async () => {
      calls += 1;
      if (calls < 3) throw new Error(`transient ${calls}`);
      return "done";
    },
    {
      policy: policy({ maxAttempts: 5, baseDelayS: 2 }),
      sleep: async (ms) => {
        delays.push(ms);
      },
    },
  );
  assert.equal(value, "done");
  assert.equal(calls, 3);
  assert.deepEqual(delays, [2000, 4000], "exponential backoff between attempts, in milliseconds");
});

test("exhaustion returns the last operation error, not a wrapper", async () => {
  const errors = [new Error("first"), new Error("second"), new Error("third")];
  const last = errors[errors.length - 1];
  let calls = 0;
  await assert.rejects(
    retry(
      async () => {
        throw errors[calls++];
      },
      { policy: policy({ maxAttempts: 3 }), sleep: async () => {} },
    ),
    (error: unknown) => error === last,
  );
  assert.equal(calls, 3);
});

test("a negative maxAttempts retries until the operation succeeds", async () => {
  let calls = 0;
  const value = await retry(
    async () => {
      calls += 1;
      if (calls < 50) throw new Error("not yet");
      return calls;
    },
    { policy: policy({ maxAttempts: -1 }), sleep: async () => {} },
  );
  assert.equal(value, 50);
});

test("an abort cancels the budget instead of finishing it", async () => {
  const controller = new AbortController();
  let calls = 0;
  await assert.rejects(
    retry(
      async () => {
        calls += 1;
        controller.abort(new Error("cancelled"));
        throw new Error("transient");
      },
      { policy: policy({ maxAttempts: 5 }), signal: controller.signal, sleep: async () => {} },
    ),
    /cancelled/,
  );
  assert.equal(calls, 1, "no further attempt after the abort");

  const pre = new AbortController();
  pre.abort(new Error("already"));
  let preCalls = 0;
  await assert.rejects(
    retry(
      async () => {
        preCalls += 1;
        return 1;
      },
      { signal: pre.signal, sleep: async () => {} },
    ),
    /already/,
  );
  assert.equal(preCalls, 0, "an already-aborted operation is never invoked");
});

test("an abort during the backoff wait stops the retry", async () => {
  const controller = new AbortController();
  let calls = 0;
  const sleep: Sleep = async (_ms, signal) => {
    controller.abort(new Error("stopped waiting"));
    if (signal?.aborted) throw signal.reason;
  };
  await assert.rejects(
    retry(
      async () => {
        calls += 1;
        throw new Error("transient");
      },
      { policy: policy({ maxAttempts: 5 }), signal: controller.signal, sleep },
    ),
    /stopped waiting/,
  );
  assert.equal(calls, 1);
});

test("onAttemptFailed observes each failure before its backoff wait", async () => {
  const observed: number[] = [];
  await assert.rejects(
    retry(async () => { throw new Error("nope"); }, {
      policy: policy({ maxAttempts: 3 }),
      sleep: async () => {},
      onAttemptFailed: (_error, attempt) => observed.push(attempt),
    }),
  );
  assert.deepEqual(observed, [1, 2], "observed before attempts 2 and 3, not the final failure");
});

test("backoff is exponential, clamped, and jittered symmetrically", () => {
  assert.deepEqual(
    [1, 2, 3, 4, 5].map((attempt) =>
      backoffS(policy({ baseDelayS: 1, maxDelayS: 8 }), attempt, () => 0.5),
    ),
    [1, 2, 4, 8, 8],
  );

  const jittered = policy({ baseDelayS: 4, maxDelayS: 100, jitterRatio: 0.5 });
  assert.equal(backoffS(jittered, 1, () => 0), 2); // 4 * (1 - 0.5)
  assert.equal(backoffS(jittered, 1, () => 1), 6); // 4 * (1 + 0.5)
  assert.equal(backoffS(jittered, 1, () => 0.5), 4); // jitter cancels out

  let rngCalls = 0;
  const rng = () => {
    rngCalls += 1;
    return 0.5;
  };
  assert.equal(backoffS(policy({ baseDelayS: 1 }), 1, rng), 1);
  assert.equal(rngCalls, 0, "jitterRatio 0 never consults the RNG");
});

test("the default sleep resolves, and rejects promptly when aborted", async () => {
  await defaultSleep(1);
  const controller = new AbortController();
  const pending = defaultSleep(1000, controller.signal);
  controller.abort(new Error("abort now"));
  await assert.rejects(pending, /abort now/);
});
