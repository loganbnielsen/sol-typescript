import { test } from "node:test";
import assert from "node:assert/strict";
import { runWorker } from "../src/index.js";

test("shutdown is idempotent -- concurrent calls drain exactly once", async () => {
  let drainCalls = 0;
  let exitCode: number | undefined;
  const lifecycle = runWorker({
    drain: async () => {
      drainCalls++;
    },
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

test("drain is awaited with no bound -- does not exit before an in-flight drain resolves", async () => {
  let resolveDrain: (() => void) | undefined;
  let exited = false;
  const lifecycle = runWorker({
    drain: () =>
      new Promise<void>((resolve) => {
        resolveDrain = resolve;
      }),
    exit: () => {
      exited = true;
    },
  });
  try {
    const shutdownPromise = lifecycle.shutdown();
    await new Promise((resolve) => setTimeout(resolve, 20));
    assert.equal(exited, false, "must not exit before drain() resolves -- no worker-side timeout");
    resolveDrain?.();
    await shutdownPromise;
    assert.equal(exited, true);
  } finally {
    lifecycle.dispose();
  }
});

test("shutdown hooks run in order after the drain resolves", async () => {
  const order: string[] = [];
  const lifecycle = runWorker({
    drain: async () => {
      order.push("drain");
    },
    shutdownHooks: [
      async () => {
        order.push("metrics");
      },
      async () => {
        order.push("db");
      },
      async () => {
        order.push("tracing");
      },
    ],
    exit: () => {},
  });
  try {
    await lifecycle.shutdown();
    assert.deepEqual(order, ["drain", "metrics", "db", "tracing"]);
  } finally {
    lifecycle.dispose();
  }
});

test("onDrainStart fires exactly once, even if SIGTERM and SIGINT both arrive", async () => {
  let calls = 0;
  const lifecycle = runWorker({
    drain: async () => {},
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

test("a drain() that throws reports via onError, exits 1, and shutdown() rejects", async () => {
  let exitCode: number | undefined;
  let reportedErr: unknown;
  const boom = new Error("boom");
  const lifecycle = runWorker({
    drain: async () => {
      throw boom;
    },
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
  const lifecycle = runWorker({
    drain: async () => {},
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

test("a signal-triggered failure does not produce an unhandled rejection", async () => {
  const boom = new Error("signal boom");
  const lifecycle = runWorker({
    drain: async () => {
      throw boom;
    },
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
