import { test } from "node:test";
import assert from "node:assert/strict";
import { runService, DEFAULT_DRAIN_TIMEOUT_MS, DEFAULT_SHUTDOWN_DELAY_MS } from "../src/index.js";

test("default drain timeout matches sol-svc's drain_timeout_s (30s)", () => {
  assert.equal(DEFAULT_DRAIN_TIMEOUT_MS, 30_000);
});

test("default shutdown delay matches sol-svc's shutdown_delay_s (5s)", () => {
  assert.equal(DEFAULT_SHUTDOWN_DELAY_MS, 5_000);
});

test("isReady() flips false immediately on shutdown, before the drain starts", async () => {
  let drainRan = false;
  const lifecycle = runService({
    drain: async () => {
      drainRan = true;
    },
    shutdownDelayMs: 50,
    exit: () => {},
  });
  try {
    assert.equal(lifecycle.isReady(), true);
    const shutting = lifecycle.shutdown();
    assert.equal(lifecycle.isReady(), false, "readiness must flip synchronously with shutdown");
    assert.equal(drainRan, false, "the drain must not start before the shutdown delay elapses");
    await shutting;
    assert.equal(drainRan, true);
    assert.equal(lifecycle.isReady(), false, "readiness must stay off once shutdown began");
  } finally {
    lifecycle.dispose();
  }
});

test("shutdownDelayMs: 0 starts the drain without waiting", async () => {
  let drainRan = false;
  const lifecycle = runService({
    drain: async () => {
      drainRan = true;
    },
    shutdownDelayMs: 0,
    exit: () => {},
  });
  try {
    await lifecycle.shutdown();
    assert.equal(drainRan, true);
    assert.equal(lifecycle.isReady(), false);
  } finally {
    lifecycle.dispose();
  }
});

test("shutdown is idempotent -- concurrent calls drain exactly once", async () => {
  let drainCalls = 0;
  let exitCode: number | undefined;
  const lifecycle = runService({
    drain: async () => {
      drainCalls++;
    },
    shutdownDelayMs: 0,
    exit: (code) => {
      exitCode = code;
    },
  });
  try {
    await Promise.all([lifecycle.shutdown(), lifecycle.shutdown()]);
    assert.equal(drainCalls, 1);
    assert.equal(exitCode, 0);
  } finally {
    lifecycle.dispose();
  }
});

test("a drain that never resolves is force-cancelled at the timeout, not hung forever", async () => {
  let exited = false;
  let hookRan = false;
  const lifecycle = runService({
    drain: () => new Promise(() => {}), // never resolves
    drainTimeoutMs: 20,
    shutdownDelayMs: 0,
    shutdownHooks: [
      async () => {
        hookRan = true;
      },
    ],
    exit: () => {
      exited = true;
    },
  });
  try {
    await lifecycle.shutdown();
    assert.equal(exited, true);
    assert.equal(hookRan, true);
  } finally {
    lifecycle.dispose();
  }
});

test("shutdown hooks run in order after the drain settles", async () => {
  const order: string[] = [];
  const lifecycle = runService({
    drain: async () => {
      order.push("drain");
    },
    shutdownDelayMs: 0,
    shutdownHooks: [
      async () => {
        order.push("producer");
      },
      async () => {
        order.push("tracing");
      },
    ],
    exit: () => {},
  });
  try {
    await lifecycle.shutdown();
    assert.deepEqual(order, ["drain", "producer", "tracing"]);
  } finally {
    lifecycle.dispose();
  }
});

test("onDrainStart fires exactly once, even if SIGTERM and SIGINT both arrive", async () => {
  let calls = 0;
  const lifecycle = runService({
    drain: async () => {},
    shutdownDelayMs: 0,
    onDrainStart: () => {
      calls++;
    },
    exit: () => {},
  });
  try {
    process.emit("SIGTERM", "SIGTERM");
    process.emit("SIGINT", "SIGINT");
    await new Promise((resolve) => setTimeout(resolve, 10));
    assert.equal(calls, 1);
  } finally {
    lifecycle.dispose();
  }
});

test("the drain timeout timer is cleared (not left running) when drain wins the race", async () => {
  const originalSetTimeout = global.setTimeout;
  const originalClearTimeout = global.clearTimeout;
  let createdTimer: NodeJS.Timeout | undefined;
  let clearedTimer: unknown;
  // @ts-expect-error -- narrower test spy than the real overloaded signature
  global.setTimeout = (fn: () => void, ms: number) => {
    createdTimer = originalSetTimeout(fn, ms);
    return createdTimer;
  };
  global.clearTimeout = (timer: unknown) => {
    clearedTimer = timer;
    return originalClearTimeout(timer as NodeJS.Timeout);
  };
  try {
    const lifecycle = runService({
      drain: async () => {}, // resolves immediately -- drain wins, not the timeout
      drainTimeoutMs: 5_000,
      shutdownDelayMs: 0,
      exit: () => {},
    });
    try {
      await lifecycle.shutdown();
      assert.ok(createdTimer !== undefined, "a drain-timeout timer must be created");
      assert.equal(clearedTimer, createdTimer, "the timer that lost the race must be cleared");
    } finally {
      lifecycle.dispose();
    }
  } finally {
    global.setTimeout = originalSetTimeout;
    global.clearTimeout = originalClearTimeout;
  }
});

test("a drain() that throws reports via onError, exits 1, and shutdown() rejects", async () => {
  let exitCode: number | undefined;
  let reportedErr: unknown;
  const boom = new Error("boom");
  const lifecycle = runService({
    drain: async () => {
      throw boom;
    },
    shutdownDelayMs: 0,
    onError: (err) => {
      reportedErr = err;
    },
    exit: (code) => {
      exitCode = code;
    },
  });
  try {
    await assert.rejects(() => lifecycle.shutdown(), boom);
    assert.equal(exitCode, 1);
    assert.equal(reportedErr, boom);
  } finally {
    lifecycle.dispose();
  }
});

test("a shutdown hook that throws reports via onError and exits 1", async () => {
  let exitCode: number | undefined;
  const boom = new Error("hook boom");
  const lifecycle = runService({
    drain: async () => {},
    shutdownDelayMs: 0,
    shutdownHooks: [
      async () => {
        throw boom;
      },
    ],
    onError: () => {},
    exit: (code) => {
      exitCode = code;
    },
  });
  try {
    await assert.rejects(() => lifecycle.shutdown(), boom);
    assert.equal(exitCode, 1);
  } finally {
    lifecycle.dispose();
  }
});

test("a rejected drain() still clears the drain-timeout timer (regression: was left running for the full timeout)", async () => {
  const originalClearTimeout = global.clearTimeout;
  let cleared = false;
  global.clearTimeout = (timer: unknown) => {
    cleared = true;
    return originalClearTimeout(timer as NodeJS.Timeout);
  };
  try {
    const lifecycle = runService({
      drain: async () => {
        throw new Error("boom");
      },
      drainTimeoutMs: 30_000,
      shutdownDelayMs: 0,
      onError: () => {},
      exit: () => {},
    });
    try {
      await assert.rejects(() => lifecycle.shutdown());
      assert.equal(cleared, true, "the drain-timeout timer must be cleared even when drain() rejects");
    } finally {
      lifecycle.dispose();
    }
  } finally {
    global.clearTimeout = originalClearTimeout;
  }
});

test("a signal-triggered failure does not produce an unhandled rejection", async () => {
  const boom = new Error("signal boom");
  const lifecycle = runService({
    drain: async () => {
      throw boom;
    },
    shutdownDelayMs: 0,
    onError: () => {},
    exit: () => {},
  });
  try {
    process.emit("SIGTERM", "SIGTERM");
    await new Promise((resolve) => setTimeout(resolve, 10));
    // If the signal path left an unhandled rejection, Node's own
    // process-level handler would have already reported it by now; the
    // fact that this test completes at all is the assertion.
  } finally {
    lifecycle.dispose();
  }
});
