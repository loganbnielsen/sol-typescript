// Matches framework/sol-svc/lib/service.mli's `?drain_timeout_s` default
// (30.0): an HTTP client can hold a connection open indefinitely, so the
// drain is bounded rather than awaited forever -- unlike @sol-fab/worker,
// whose sol-worker counterpart (worker.mli) has no such parameter at all.
export const DEFAULT_DRAIN_TIMEOUT_MS = 30_000;

export interface RunServiceOptions {
  /** Stop accepting new requests and let in-flight ones finish, e.g. `() => app.close()`. */
  drain: () => Promise<void>;
  /** Default 30_000, matching sol-svc's `drain_timeout_s`. */
  drainTimeoutMs?: number;
  /** Run in order once the drain settles, e.g. producer/tracing teardown. */
  shutdownHooks?: Array<() => Promise<void>>;
  /** Defaults to `process.exit`; overridable so tests don't kill the process. */
  exit?: (code: number) => void;
  /** Called exactly once, on the first SIGTERM/SIGINT, before the drain starts. */
  onDrainStart?: () => void;
  /**
   * Called if `drain()` or a shutdown hook throws (a bug, not the ordinary
   * drain-timeout path -- that one is handled internally, matching
   * service.ml's `Drain_timeout` catch). Exit code is 1 in this case
   * instead of 0.
   */
  onError?: (err: unknown) => void;
}

export interface ServiceLifecycle {
  /**
   * Trigger shutdown programmatically, same idempotent path as a signal.
   * Rejects if `drain()` or a shutdown hook threw (see `onError`); resolves
   * on the ordinary path, including a drain-timeout force-cancel.
   */
  shutdown: () => Promise<void>;
  /** Remove the installed SIGTERM/SIGINT listeners (for tests). */
  dispose: () => void;
}

/**
 * Installs Sol's service-lifecycle contract: idempotent SIGTERM/SIGINT, a
 * drain bounded by `drainTimeoutMs`, ordered shutdown hooks, exit 0. Does
 * not own routing, auth, or the `/metrics` endpoint -- those stay
 * Fastify/Express and `@sol-fab/obs`.
 */
export function runService(opts: RunServiceOptions): ServiceLifecycle {
  const drainTimeoutMs = opts.drainTimeoutMs ?? DEFAULT_DRAIN_TIMEOUT_MS;
  const exit = opts.exit ?? ((code: number) => process.exit(code));
  const hooks = opts.shutdownHooks ?? [];

  let shuttingDown = false;
  let resolveDone: (() => void) | undefined;
  let rejectDone: ((err: unknown) => void) | undefined;
  const done = new Promise<void>((resolve, reject) => {
    resolveDone = resolve;
    rejectDone = reject;
  });
  // A rejected `done` with no second concurrent shutdown() call to consume
  // it would otherwise be an unhandled rejection in its own right.
  done.catch(() => {});

  const shutdown = async (): Promise<void> => {
    if (shuttingDown) return done;
    shuttingDown = true;
    opts.onDrainStart?.();

    let timer: NodeJS.Timeout;
    const timeout = new Promise<void>((resolve) => {
      timer = setTimeout(resolve, drainTimeoutMs);
    });
    try {
      try {
        await Promise.race([opts.drain(), timeout]);
      } finally {
        // Clearing (not merely `.unref()`ing) the loser is what makes this
        // safe from a context that never reaches process.exit, e.g. a
        // test -- and it must run whether drain() resolved, timed out, or
        // threw, or a rejected drain() leaves this timer running for the
        // full drainTimeoutMs regardless.
        clearTimeout(timer!);
      }

      for (const hook of hooks) {
        await hook();
      }

      resolveDone?.();
      exit(0);
    } catch (err) {
      opts.onError?.(err);
      exit(1);
      rejectDone?.(err);
      throw err;
    }
  };

  // The signal path swallows a rejection here deliberately: `onError` above
  // is the reporting channel for a signal-triggered shutdown, so a second,
  // unhandled rejection on top of it would just be noise. A caller invoking
  // `.shutdown()` directly (e.g. a test) still observes the rejection.
  const onSigterm = () => void shutdown().catch(() => {});
  const onSigint = () => void shutdown().catch(() => {});
  process.on("SIGTERM", onSigterm);
  process.on("SIGINT", onSigint);

  return {
    shutdown,
    dispose: () => {
      process.off("SIGTERM", onSigterm);
      process.off("SIGINT", onSigint);
    },
  };
}
