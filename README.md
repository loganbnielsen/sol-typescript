# sol-typescript

Source repository for Sol's TypeScript application-runtime packages:

- **`packages/svc`** → `@sol-fab/svc` — Sol's service lifecycle and outbound
  peer-authentication helpers for TypeScript HTTP services.
- **`packages/worker`** → `@sol-fab/worker` — Sol's worker-lifecycle contract
  for TypeScript Kafka workers.
- **`packages/jobs`** → `@sol-fab/jobs` — Sol's durable Postgres job queue: a
  transactional, dedupe-keyed `enqueue` and a leased `runJobs` runner, mirroring
  `framework/sol-jobs`.
- **`packages/outbox`** → `@sol-fab/outbox` — Sol's Postgres transactional
  outbox: a `publish` that joins the caller's transaction and a `runRelay` that
  publishes each key's events in order, deleting a row only after the broker
  acknowledged it. Mirrors `framework/sol-outbox`.
- **`packages/retry`** → `@sol-fab/retry` — Sol's operation-level retry helper:
  one bounded, jittered policy vocabulary (`baseDelayS`, `maxDelayS`,
  `maxAttempts`, `jitterRatio`) that retries a dependency call in place, never a
  message or a handler. Mirrors `framework/sol-retry`; `@sol-fab/jobs` consumes
  the same vocabulary rather than keeping its own copy.

The packages keep lifecycle and helper ownership narrow: routing and inbound
HTTP stay Fastify/Express, Kafka consumption stays `kafkajs`, DLQ routing stays
`@sol-fab/kafka`, and metrics/tracing naming stays `@sol-fab/obs`.

`@sol-fab/svc` and `@sol-fab/worker` deliberately differ in one respect,
because their OCaml counterparts do: `sol-svc`'s `service.mli` exposes a
`drain_timeout_s` (an HTTP client can hold a connection open indefinitely);
`sol-worker`'s `worker.mli` has no equivalent parameter at all (a worker's
forced-shutdown safety is Kafka redelivery plus the orchestrator's grace
period, not a package-level timeout).

`@sol-fab/svc` also mirrors `sol-svc`'s readiness: `runService` returns
`isReady()`, false from the instant shutdown begins, so the app's `/readyz`
route returns 503 while the listener still serves. The listener keeps serving
for `shutdownDelayMs` (default 5s, `sol-svc`'s `shutdown_delay_s`) before the
drain starts, so a readiness probe observes the flip before the socket closes.

`@sol-fab/svc` exports `declaredPeer`, `peerUrl`, and `peerHeaders` for generated
application bindings. `peerHeaders` reads the projected token file for each call
so token rotation is observed; it uses the local API-key fallback only when
`SOL_ALLOW_PLAINTEXT_PEER_AUTH=1` is explicitly set.

`@sol-fab/jobs` is a library, not a fourth primitive (DEC-021): an ordinary
`@sol-fab/worker` binary hosts it by calling `runJobs` instead of consuming a
topic. Kafka says "this happened"; the job table says "this must happen", and
`enqueue` accepts a `PoolClient` so it can join the transaction that caused it.

No `@sol-fab/fn` package exists yet -- `sol-fn`'s OCaml contract (run once,
return, `SIGTERM` before the next invocation) has no measured TypeScript gap
to justify one; see the `sol` repo's `FEAT-036` ticket.

## Status

Published to npm as `@sol-fab/svc`, `@sol-fab/worker`, `@sol-fab/jobs`,
`@sol-fab/outbox` and `@sol-fab/retry`; a tag (`svc-v*`, `worker-v*`,
`jobs-v*`, `outbox-v*`, `retry-v*`) publishes through the OIDC release
workflow. Publish `retry` before a `jobs` release that depends on it. See the
`sol` repo's `pipeline/tickets/` for sequencing.

The `@sol-fab/jobs` and `@sol-fab/outbox` behaviour tests need a Postgres: CI
runs one and sets `POSTGRES_URL`, and without it those cases self-skip.

## Development

```bash
npm install
npm run build --workspaces
npm run test --workspaces
```
