/**
 * Sol's Postgres transactional outbox for TypeScript, mirroring `sol-outbox`
 * (framework/ocaml/sol-outbox) in capability and observable behaviour rather
 * than implementation (DEC-022):
 *
 *   - `publish` is a plain `INSERT`, so calling it with a pool handle from
 *     inside a transaction puts the event in the *same* Postgres transaction as
 *     the state change that caused it. "An event exists iff the state change
 *     committed" holds of the outbox row -- the publication intent -- never of
 *     the Kafka record.
 *   - the relay publishes the oldest unpublished event of each key, in `ord`
 *     order, and deletes the row only once the broker acknowledged it. A
 *     failure leaves the row in place and does not advance the key, so a crash
 *     between acknowledgement and deletion duplicates rather than gaps or
 *     inverts. Per-key order, at-least-once; consumers must be idempotent.
 *
 * It is a library, not a fourth deployable primitive (DEC-021): host the relay
 * from an ordinary `-worker` binary by calling `runRelay` alongside its consumer.
 * The publish callback is injected so this package does not depend on the Kafka
 * layer; build it on `@sol-fab/kafka`'s producer, passing the publication's key
 * as the record key so downstream partition order is the published order.
 */
import type { Pool } from "pg";

/** The shared table. An app migration must create it; see the sol-outbox spec. */
export const OUTBOX_TABLE = "sol_outbox";

/** The metric names `sol-outbox` emits, for a cross-language dashboard. */
export const SOL_OUTBOX_PUBLISHED_TOTAL = "sol_outbox_published_total";
export const SOL_OUTBOX_PENDING = "sol_outbox_pending";
export const SOL_OUTBOX_OLDEST_PENDING_SECONDS = "sol_outbox_oldest_pending_seconds";

export const DEFAULT_POLL_INTERVAL_S = 0.5;
export const DEFAULT_BATCH = 100;

export type RunError =
  | { readonly kind: "config"; readonly message: string }
  | { readonly kind: "database"; readonly message: string };

export function runErrorToString(error: RunError): string {
  return error.kind === "config"
    ? `sol-outbox: invalid configuration: ${error.message}`
    : `sol-outbox: outbox table unusable: ${error.message}`;
}

/**
 * Anything that can run a query: a `pg` `Pool` or a `PoolClient`. Passing the
 * client from inside `BEGIN` is what makes the publish transactional.
 */
export interface Queryable {
  query(text: string, values?: unknown[]): Promise<unknown>;
}

export interface OutboxContract<T> {
  /** Every kind the relay may publish. A kind outside it would never be sent. */
  readonly kinds: readonly string[];
  readonly kind: (event: T) => string;
  readonly encode: (event: T) => string;
}

export interface PublishOptions {
  /** The aggregate key. Every event sharing it is published in `ord` order. */
  readonly key: string;
  /**
   * The caller's per-key ordering token, assigned while that key is serialized
   * (a per-key version, or a value read under the same lock or compare-and-set
   * that makes the mutation serial). The outbox has no sequence generator and no
   * global-ordering fallback: a unique index on `(aggregate_key, ord)` refuses a
   * token collision instead of publishing two events in one position.
   */
  readonly ord: number;
}

export interface Publication {
  readonly kind: string;
  readonly key: string;
  readonly ord: number;
  readonly payload: string;
}

function isKindChar(c: string): boolean {
  return /[a-z0-9_.-]/.test(c);
}

export function validateKinds(kinds: readonly string[]): RunError | undefined {
  if (kinds.length === 0) {
    return {
      kind: "config",
      message: "kinds is empty -- a relay that can publish no kind would leave every row in place",
    };
  }
  const bad = kinds.find((k) => k === "" || ![...k].every(isKindChar));
  return bad === undefined
    ? undefined
    : {
        kind: "config",
        message: `kinds entry ${JSON.stringify(bad)} is invalid: kinds are non-empty and use only a-z, 0-9, _, ., -`,
      };
}

export function validateTiming(pollIntervalS: number, batch: number): RunError | undefined {
  if (!Number.isInteger(batch) || batch < 1) {
    return { kind: "config", message: `batch must be an integer >= 1 (got ${batch})` };
  }
  return Number.isFinite(pollIntervalS) && pollIntervalS >= 0
    ? undefined
    : { kind: "config", message: `pollIntervalS must be a finite number >= 0 (got ${pollIntervalS})` };
}

/**
 * Publish an event intent. Pass a `PoolClient` from inside a transaction to make
 * the intent commit with the state change that caused it; pass the `Pool` to
 * publish standalone. A `(key, ord)` collision is rejected by the unique index,
 * not silently accepted.
 */
export async function publish<T>(
  client: Queryable,
  contract: OutboxContract<T>,
  event: T,
  options: PublishOptions,
): Promise<void> {
  const kind = contract.kind(event);
  if (!contract.kinds.includes(kind)) {
    throw new Error(
      `sol-outbox: kind ${JSON.stringify(kind)} is not in kinds; the relay would leave it unpublished forever`,
    );
  }
  await client.query(
    `INSERT INTO ${OUTBOX_TABLE} (kind, aggregate_key, ord, payload) VALUES ($1, $2, $3, $4)`,
    [kind, options.key, options.ord, contract.encode(event)],
  );
}

export interface PendingEvent {
  readonly key: string;
  readonly ord: number;
}

/** Not published yet, ordered by key then `ord` (tests and diagnostics). */
export async function pending(pool: Pool, limit = 1000): Promise<PendingEvent[]> {
  const result = (await pool.query(
    `SELECT aggregate_key, ord FROM ${OUTBOX_TABLE} ORDER BY aggregate_key, ord LIMIT $1`,
    [limit],
  )) as { rows: Array<{ aggregate_key: string; ord: string }> };
  return result.rows.map((row) => ({ key: row.aggregate_key, ord: Number(row.ord) }));
}

export async function pendingCount(pool: Pool): Promise<number> {
  const result = (await pool.query(
    `SELECT count(*)::int AS count FROM ${OUTBOX_TABLE}`,
  )) as { rows: Array<{ count: number }> };
  return result.rows[0]?.count ?? 0;
}

export interface KindGauge {
  readonly kind: string;
  readonly value: number;
}

export interface OutboxMetrics {
  /** Rows waiting to be published, per kind (`sol_outbox_pending`). */
  readonly pendingByKind: ReadonlyArray<KindGauge>;
  /**
   * Age of the oldest unpublished row per kind, in seconds
   * (`sol_outbox_oldest_pending_seconds`): the publication lag. Reported at kind
   * granularity, never per key -- a key label is unbounded cardinality.
   */
  readonly oldestPendingSecondsByKind: ReadonlyArray<KindGauge>;
}

export type PublicationStatus = "ok" | "failed" | "mark_failed";

export interface RelayOptions {
  readonly pool: Pool;
  /**
   * Publish one event and resolve only once the broker acknowledged it. A
   * failure must reject; the relay leaves the row unpublished and does not
   * advance the key, so a broker outage is lag, not loss.
   */
  readonly publish: (publication: Publication) => Promise<void>;
  readonly pollIntervalS?: number;
  readonly batch?: number;
  /** Fires once the table is readable and the loop is about to start. */
  readonly onReady?: () => void;
  /** Each acknowledged/failed publication, for `sol_outbox_published_total`. */
  readonly onPublication?: (publication: Publication, status: PublicationStatus) => void;
  /** After each drain, for `sol_outbox_pending`/`sol_outbox_oldest_pending_seconds`. */
  readonly onMetrics?: (metrics: OutboxMetrics) => void;
  /** Query and publish failures; defaults to `console.warn`. */
  readonly onWarning?: (fields: Readonly<Record<string, string>>, message: string) => void;
  /** Cooperative stop, e.g. from the worker's drain. */
  readonly signal?: AbortSignal;
  readonly sleep?: (seconds: number) => Promise<void>;
}

const oldestPerKeySql = `SELECT o.id, o.ord, o.aggregate_key, o.kind, o.payload
  FROM ${OUTBOX_TABLE} o
  WHERE o.ord = (SELECT min(i.ord) FROM ${OUTBOX_TABLE} i WHERE i.aggregate_key = o.aggregate_key)
  ORDER BY o.id
  LIMIT $1`;

const deleteSql = `DELETE FROM ${OUTBOX_TABLE} WHERE id = $1`;

const defaultSleep = (seconds: number): Promise<void> =>
  new Promise((resolve) => {
    setTimeout(resolve, Math.max(0, seconds) * 1000);
  });

/**
 * Own the relay loop: publish the oldest unpublished event of each key, delete
 * it only after the publish resolves, and repeat. Returns `undefined` on a
 * clean stop, or the `RunError` that stopped it.
 *
 * v1 is one logical relay owner. Overlapping owners are a correctness problem,
 * not a performance one: two owners publishing a key can invert it, and a plain
 * producer carries no fencing token. Scaling is a trigger, not a design.
 */
export async function runRelay(options: RelayOptions): Promise<RunError | undefined> {
  const { pool, publish } = options;
  const pollIntervalS = options.pollIntervalS ?? DEFAULT_POLL_INTERVAL_S;
  const batch = options.batch ?? DEFAULT_BATCH;
  const sleep = options.sleep ?? defaultSleep;
  const warn =
    options.onWarning ??
    ((fields: Readonly<Record<string, string>>, message: string) => {
      console.warn(message, fields);
    });
  const stopped = (): boolean => options.signal?.aborted ?? false;

  const timingError = validateTiming(pollIntervalS, batch);
  if (timingError) return timingError;

  try {
    const result = (await pool.query(`SELECT to_regclass('${OUTBOX_TABLE}')::text AS table`)) as {
      rows: Array<{ table: string | null }>;
    };
    if (result.rows[0]?.table == null) {
      return {
        kind: "database",
        message: `the ${OUTBOX_TABLE} table does not exist; apply the migration that creates it before starting the relay`,
      };
    }
  } catch (error) {
    return {
      kind: "database",
      message: `cannot read the ${OUTBOX_TABLE} table (an app migration must create it -- see the sol-outbox spec): ${String(error)}`,
    };
  }

  options.onReady?.();

  interface Row {
    id: string;
    ord: string;
    aggregate_key: string;
    kind: string;
    payload: string;
  }

  const reportMetrics = async (): Promise<void> => {
    if (!options.onMetrics) return;
    try {
      const pendingRows = (await pool.query(
        `SELECT kind, count(*)::int AS count FROM ${OUTBOX_TABLE} GROUP BY kind`,
      )) as { rows: Array<{ kind: string; count: number }> };
      const ageRows = (await pool.query(
        `SELECT kind, extract(epoch from (now() - min(created_at)))::float8 AS age
           FROM ${OUTBOX_TABLE} GROUP BY kind`,
      )) as { rows: Array<{ kind: string; age: number }> };
      options.onMetrics({
        pendingByKind: pendingRows.rows.map((row) => ({ kind: row.kind, value: row.count })),
        oldestPendingSecondsByKind: ageRows.rows.map((row) => ({ kind: row.kind, value: row.age })),
      });
    } catch (error) {
      warn({ error: String(error) }, "sol-outbox: failed to read pending/age metrics");
    }
  };

  const drain = async (): Promise<RunError | undefined> => {
    let rows: Row[];
    try {
      const result = (await pool.query(oldestPerKeySql, [batch])) as { rows: Row[] };
      rows = result.rows;
    } catch (error) {
      return { kind: "database", message: String(error) };
    }

    for (const row of rows) {
      const publication: Publication = {
        kind: row.kind,
        key: row.aggregate_key,
        ord: Number(row.ord),
        payload: row.payload,
      };
      try {
        await publish(publication);
      } catch (error) {
        options.onPublication?.(publication, "failed");
        warn(
          {
            kind: row.kind,
            key: row.aggregate_key,
            ord: String(row.ord),
            error: String(error),
          },
          "sol-outbox: publish failed; leaving the event unpublished and not advancing the key",
        );
        continue;
      }
      try {
        await pool.query(deleteSql, [row.id]);
      } catch (error) {
        options.onPublication?.(publication, "mark_failed");
        return {
          kind: "database",
          message: `published kind ${row.kind} key ${row.aggregate_key} (ord ${row.ord}) but could not delete the row: ${String(error)}`,
        };
      }
      options.onPublication?.(publication, "ok");
    }
    return undefined;
  };

  while (!stopped()) {
    const failure = await drain();
    if (failure) return failure;
    await reportMetrics();
    if (stopped()) break;
    await sleep(pollIntervalS);
  }

  return undefined;
}
