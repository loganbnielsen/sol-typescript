export interface RunWorkerOptions {
  /**
   * Stop consuming and let in-flight work finish, e.g.
   * `() => consumer.disconnect()`. Awaited with no bound -- matches
   * framework/sol-worker/lib/worker.mli's `Worker.Make.run`, which has no
   * `drain_timeout_s` at all (unlike sol-svc): a worker's forced-shutdown
   * safety is Kafka redelivery plus the orchestrator's grace period, not a
   * package-level timeout. See FEAT-082 #2/#2b in the sol repo for the
   * measured contract this mirrors.
   */
  drain: () => Promise<void>;
  /** Run in order once the drain resolves, e.g. metrics server/DB/tracing teardown. */
  shutdownHooks?: Array<() => Promise<void>>;
  /** Defaults to `process.exit`; overridable so tests don't kill the process. */
  exit?: (code: number) => void;
  /** Called exactly once, on the first SIGTERM/SIGINT, before the drain starts. */
  onDrainStart?: () => void;
  /** Called if `drain()` or a shutdown hook throws. Exit code is 1 in this case instead of 0. */
  onError?: (err: unknown) => void;
}

export interface WorkerLifecycle {
  /**
   * Trigger shutdown programmatically, same idempotent path as a signal.
   * Rejects if `drain()` or a shutdown hook threw (see `onError`).
   */
  shutdown: () => Promise<void>;
  /** Remove the installed SIGTERM/SIGINT listeners (for tests). */
  dispose: () => void;
}

/**
 * Installs Sol's worker-lifecycle contract: idempotent SIGTERM/SIGINT, an
 * unbounded drain, ordered shutdown hooks, exit 0. Does not own consuming,
 * retry/DLQ routing, or metrics -- those stay `kafkajs`, `@sol-fab/kafka`,
 * and `@sol-fab/obs`.
 */
export function runWorker(opts: RunWorkerOptions): WorkerLifecycle {
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

    try {
      await opts.drain();

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
