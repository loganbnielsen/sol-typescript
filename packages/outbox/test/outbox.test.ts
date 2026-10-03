import { test } from "node:test";
import assert from "node:assert/strict";
import pg from "pg";
import {
  OUTBOX_TABLE,
  pending,
  pendingCount,
  publish,
  runErrorToString,
  runRelay,
  validateKinds,
  validateTiming,
  type OutboxContract,
  type Publication,
  type Queryable,
} from "../src/index.js";

// The validation rules are pure and run everywhere. The outbox behaviour needs
// a real Postgres (transactions, the unique `(key, ord)` index, `min(ord)` per
// key, `now() - created_at`); those cases self-skip unless POSTGRES_URL is set,
// the same way sol-kafka's broker-backed tests do.

test("kinds: empty is rejected, and so is a kind nothing would match", () => {
  assert.match(validateKinds([])?.message ?? "", /empty/);
  assert.equal(validateKinds(["order-placed", "order.shipped_v2"]), undefined);
  assert.match(validateKinds(["Order Placed"])?.message ?? "", /invalid/);
});

test("timing: batch must be a positive integer, poll interval finite and non-negative", () => {
  assert.equal(validateTiming(0.5, 100), undefined);
  assert.match(validateTiming(0.5, 0)?.message ?? "", /batch/);
  assert.match(validateTiming(0.5, 1.5)?.message ?? "", /batch/);
  assert.match(validateTiming(Number.POSITIVE_INFINITY, 100)?.message ?? "", /pollIntervalS/);
  assert.match(validateTiming(-1, 100)?.message ?? "", /pollIntervalS/);
});

test("runErrorToString: names the failure class", () => {
  assert.match(runErrorToString({ kind: "config", message: "x" }), /invalid configuration/);
  assert.match(runErrorToString({ kind: "database", message: "x" }), /outbox table unusable/);
});

interface OrderEvent {
  key: string;
  ord: number;
}

const ORDERS: OutboxContract<OrderEvent> = {
  kinds: ["order-placed"],
  kind: () => "order-placed",
  encode: (event) => JSON.stringify(event),
};

function event(key: string, ord: number): OrderEvent {
  return { key, ord };
}

const POSTGRES_URL = process.env.POSTGRES_URL;
const withDb = POSTGRES_URL ? test : test.skip;

async function withFreshTable(body: (pool: pg.Pool) => Promise<void>): Promise<void> {
  const pool = new pg.Pool({ connectionString: POSTGRES_URL, max: 4 });
  try {
    await pool.query(`DROP TABLE IF EXISTS ${OUTBOX_TABLE}`);
    await pool.query(`CREATE TABLE ${OUTBOX_TABLE} (
      id BIGSERIAL PRIMARY KEY,
      kind TEXT NOT NULL,
      aggregate_key TEXT NOT NULL,
      ord BIGINT NOT NULL,
      payload TEXT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )`);
    await pool.query(
      `CREATE UNIQUE INDEX ${OUTBOX_TABLE}_key_ord_idx ON ${OUTBOX_TABLE} (aggregate_key, ord)`,
    );
    await body(pool);
  } finally {
    await pool.end();
  }
}

/** Run a relay pass until `done` says enough was published, then abort it. */
async function relayUntil(
  pool: pg.Pool,
  publish: (publication: Publication) => Promise<void>,
  done: () => boolean,
): Promise<Awaited<ReturnType<typeof runRelay>>> {
  const controller = new AbortController();
  return runRelay({
    pool,
    publish: async (publication) => {
      await publish(publication);
      if (done()) controller.abort();
    },
    signal: controller.signal,
    sleep: async () => {},
  });
}

test("publish: a kind outside the contract is rejected before the insert", async () => {
  let queries = 0;
  const client: Queryable = {
    query: async () => {
      queries += 1;
      return { rows: [] };
    },
  };
  const unknown: OutboxContract<OrderEvent> = {
    kinds: ["order-placed"],
    kind: () => "not-a-kind",
    encode: (e) => JSON.stringify(e),
  };
  await assert.rejects(
    () => publish(client, unknown, event("a", 1), { key: "a", ord: 1 }),
    /not in kinds/,
  );
  assert.equal(queries, 0, "the contract check refuses it before any query");
});

withDb("publish: joins the caller's transaction; a rollback leaves no intent", async () => {
  await withFreshTable(async (pool) => {
    const rolledBack = await pool.connect();
    try {
      await rolledBack.query("BEGIN");
      await publish(rolledBack, ORDERS, event("a", 1), { key: "a", ord: 1 });
      await rolledBack.query("ROLLBACK");
    } finally {
      rolledBack.release();
    }
    assert.equal(await pendingCount(pool), 0, "the intent must not survive a rollback");

    const committed = await pool.connect();
    try {
      await committed.query("BEGIN");
      await publish(committed, ORDERS, event("a", 1), { key: "a", ord: 1 });
      await committed.query("COMMIT");
    } finally {
      committed.release();
    }
    assert.deepEqual(await pending(pool), [{ key: "a", ord: 1 }]);
  });
});

withDb("publish: a (key, ord) collision is refused by the unique index", async () => {
  await withFreshTable(async (pool) => {
    await publish(pool, ORDERS, event("a", 1), { key: "a", ord: 1 });
    await assert.rejects(
      () => publish(pool, ORDERS, event("a", 1), { key: "a", ord: 1 }),
      /duplicate key|unique/i,
    );
    assert.equal(await pendingCount(pool), 1);
  });
});

withDb("relay: publishes each key's oldest event first and deletes it after acknowledgement", async () => {
  await withFreshTable(async (pool) => {
    for (const ord of [1, 2, 3]) await publish(pool, ORDERS, event("a", ord), { key: "a", ord });
    await publish(pool, ORDERS, event("b", 1), { key: "b", ord: 1 });

    const published: Publication[] = [];
    const failure = await relayUntil(
      pool,
      async (publication) => {
        published.push(publication);
      },
      () => published.length === 4,
    );

    assert.equal(failure, undefined);
    const perKey = (key: string): number[] =>
      published.filter((p) => p.key === key).map((p) => p.ord);
    assert.deepEqual(perKey("a"), [1, 2, 3], "key a is published in ord order");
    assert.deepEqual(perKey("b"), [1]);
    assert.equal(await pendingCount(pool), 0, "published rows are deleted");
  });
});

withDb("relay: a failed publish leaves the row and holds later events for that key", async () => {
  await withFreshTable(async (pool) => {
    await publish(pool, ORDERS, event("a", 1), { key: "a", ord: 1 });
    await publish(pool, ORDERS, event("a", 2), { key: "a", ord: 2 });

    const attempts: number[] = [];
    const controller = new AbortController();
    let failFirst = true;
    const failure = await runRelay({
      pool,
      publish: async (publication) => {
        attempts.push(publication.ord);
        if (failFirst) {
          failFirst = false;
          throw new Error("broker unreachable");
        }
        controller.abort();
      },
      signal: controller.signal,
      sleep: async () => {},
      onWarning: () => {},
    });

    assert.equal(failure, undefined);
    assert.deepEqual(attempts, [1, 1], "only the blocked key's head is retried; ord 2 is never published first");
    assert.deepEqual(await pending(pool), [{ key: "a", ord: 2 }], "ord 1 was published and removed; ord 2 is still held");
  });
});

withDb("relay: a crash between acknowledgement and deletion duplicates rather than gaps", async () => {
  await withFreshTable(async (pool) => {
    await publish(pool, ORDERS, event("a", 1), { key: "a", ord: 1 });

    const poisoned = {
      query: (text: string, values?: unknown[]) =>
        text.includes(`DELETE FROM ${OUTBOX_TABLE}`)
          ? Promise.reject(new Error("connection lost after the broker ack"))
          : pool.query(text, values),
    } as unknown as pg.Pool;

    const first: Publication[] = [];
    const failure = await runRelay({
      pool: poisoned,
      publish: async (publication) => {
        first.push(publication);
      },
      sleep: async () => {},
      onWarning: () => {},
    });

    assert.equal(failure?.kind, "database");
    assert.equal(first.length, 1, "the event was published");
    assert.equal(await pendingCount(pool), 1, "but the delete failed, so the row survives");

    const second: Publication[] = [];
    const controller = new AbortController();
    await runRelay({
      pool,
      publish: async (publication) => {
        second.push(publication);
        controller.abort();
      },
      signal: controller.signal,
      sleep: async () => {},
    });

    assert.deepEqual(second.map((p) => p.ord), [1], "the same event is published again: at-least-once, never a gap");
    assert.equal(await pendingCount(pool), 0);
  });
});

withDb("relay: a missing table is a database error naming the migration", async () => {
  await withFreshTable(async (pool) => {
    await pool.query(`DROP TABLE ${OUTBOX_TABLE}`);
    const failure = await runRelay({
      pool,
      publish: async () => {},
      sleep: async () => {},
    });
    assert.equal(failure?.kind, "database");
    assert.match(failure?.message ?? "", /does not exist/);
  });
});

withDb("relay: reports pending count and oldest age per kind", async () => {
  await withFreshTable(async (pool) => {
    await publish(pool, ORDERS, event("a", 1), { key: "a", ord: 1 });
    await publish(pool, ORDERS, event("b", 1), { key: "b", ord: 1 });
    await publish(pool, ORDERS, event("b", 2), { key: "b", ord: 2 });

    const snapshots: Array<{ pendingByKind: Array<{ kind: string; value: number }>; oldestPendingSecondsByKind: Array<{ kind: string; value: number }> }> = [];
    const controller = new AbortController();
    const failure = await runRelay({
      pool,
      publish: async () => {
        throw new Error("hold every row so the gauges see the backlog");
      },
      signal: controller.signal,
      sleep: async () => {
        controller.abort();
      },
      onWarning: () => {},
      onMetrics: (metrics) => {
        snapshots.push(metrics as never);
      },
    });

    assert.equal(failure, undefined);
    assert.equal(snapshots.length, 1);
    assert.deepEqual(snapshots[0]?.pendingByKind, [{ kind: "order-placed", value: 3 }]);
    assert.equal(snapshots[0]?.oldestPendingSecondsByKind[0]?.kind, "order-placed");
    assert.ok((snapshots[0]?.oldestPendingSecondsByKind[0]?.value ?? -1) >= 0);
  });
});
