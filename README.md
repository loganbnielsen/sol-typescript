# sol-typescript

Source repository for Sol's TypeScript application-runtime packages:

- **`packages/svc`** → `@sol-fab/svc` — Sol's service-lifecycle contract for
  TypeScript HTTP services.
- **`packages/worker`** → `@sol-fab/worker` — Sol's worker-lifecycle contract
  for TypeScript Kafka workers.

Each package owns exactly what its OCaml counterpart
(`framework/sol-svc`/`framework/sol-worker` in the `sol` repo) owns for
process lifecycle — idempotent `SIGTERM`/`SIGINT`, drain semantics, ordered
shutdown — and nothing else: routing/HTTP stays Fastify/Express, Kafka
consumption stays `kafkajs`, retry/DLQ stays `@sol-fab/kafka`, metrics/tracing
naming stays `@sol-fab/obs`.

`@sol-fab/svc` and `@sol-fab/worker` deliberately differ in one respect,
because their OCaml counterparts do: `sol-svc`'s `service.mli` exposes a
`drain_timeout_s` (an HTTP client can hold a connection open indefinitely);
`sol-worker`'s `worker.mli` has no equivalent parameter at all (a worker's
forced-shutdown safety is Kafka redelivery plus the orchestrator's grace
period, not a package-level timeout). See each package's README for details.

No `@sol-fab/fn` package exists yet -- `sol-fn`'s OCaml contract (run once,
return, `SIGTERM` before the next invocation) has no measured TypeScript gap
to justify one; see the `sol` repo's `FEAT-036` ticket.

## Status

Not yet published to npm. See the `sol` repo's `pipeline/tickets/` for
sequencing (`FEAT-036` decided the API boundary; publish + wiring the
`examples/pluto` demo through it is tracked as a follow-up).

## Development

```bash
npm install
npm run build --workspaces
npm run test --workspaces
```
