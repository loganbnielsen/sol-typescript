// Matches framework/sol-svc/lib/service.mli's `?drain_timeout_s` default
// (30.0): an HTTP client can hold a connection open indefinitely, so the
// drain is bounded rather than awaited forever -- unlike @sol-fab/worker,
// whose sol-worker counterpart (worker.mli) has no such parameter at all.
export const DEFAULT_DRAIN_TIMEOUT_MS = 30_000;

// Matches framework/sol-svc/lib/service.mli's `?shutdown_delay_s` default (5.0):
// the listener keeps serving for this long after readiness turns off, so an
// orchestrator's readiness probe observes the flip before the socket closes.
export const DEFAULT_SHUTDOWN_DELAY_MS = 5_000;

export { declaredPeer, peerHeaders, peerUrl } from "./peer.js";
export type { Peer, PeerHeadersOptions } from "./peer.js";

export {
  createWorkloadIdentityGuard,
  parseCalledBy,
  resetWorkloadIdentityCache,
  verifyWorkloadIdentity,
  WorkloadJwksCache,
  workloadIdentityConfigFromEnv,
} from "./workload-identity.js";
export type {
  VerifyWorkloadIdentityOptions,
  WorkloadIdentity,
  WorkloadIdentityConfig,
  WorkloadIdentityDecision,
  WorkloadIdentityDenialStatus,
  WorkloadIdentityGuard,
  WorkloadIdentityGuardOptions,
  WorkloadIdentityRequest,
  WorkloadIdentityResult,
} from "./workload-identity.js";

export interface RunServiceOptions {
  /** Stop accepting new requests and let in-flight ones finish, e.g. `() => app.close()`. */
  drain: () => Promise<void>;
  /** Default 30_000, matching sol-svc's `drain_timeout_s`. */
  drainTimeoutMs?: number;
  /**
   * Time between readiness turning off and the drain starting. Default 5_000,
   * matching sol-svc's `shutdown_delay_s`; a probe interval must fit inside it.
   */
  shutdownDelayMs?: number;
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
  /**
   * False from the instant shutdown begins, so a `/readyz` route returns 503
   * while the listener still serves -- matching service.ml's
   * `~ready:(fun () -> Atomic.get ready)` and its 503 body.
   */
  isReady: () => boolean;
}

/**
 * Installs Sol's service-lifecycle contract: idempotent SIGTERM/SIGINT, a
 * drain bounded by `drainTimeoutMs`, ordered shutdown hooks, exit 0. Does
 * not own routing, auth, or the `/metrics` endpoint -- those stay
 * Fastify/Express and `@sol-fab/obs`.
 */
export function runService(opts: RunServiceOptions): ServiceLifecycle {
  const drainTimeoutMs = opts.drainTimeoutMs ?? DEFAULT_DRAIN_TIMEOUT_MS;
  const shutdownDelayMs = opts.shutdownDelayMs ?? DEFAULT_SHUTDOWN_DELAY_MS;
  const exit = opts.exit ?? ((code: number) => process.exit(code));
  const hooks = opts.shutdownHooks ?? [];

  let shuttingDown = false;
  let ready = true;
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
    // service.ml flips `ready` before the delay and the delay before the
    // listener stops, so a readiness probe sees 503 while requests still
    // succeed. `onDrainStart` runs after the flip so its own observation of
    // isReady() is already false.
    ready = false;
    opts.onDrainStart?.();
    if (shutdownDelayMs > 0) {
      await new Promise<void>((resolve) => setTimeout(resolve, shutdownDelayMs));
    }

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
    isReady: () => ready,
    dispose: () => {
      process.off("SIGTERM", onSigterm);
      process.off("SIGINT", onSigint);
    },
  };
}
