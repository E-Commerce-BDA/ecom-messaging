import { describe, expect, it } from "vitest";
import { PostgresOutboxStore } from "../src/outbox/postgres-outbox.store.js";
import type {
  PoolClientLike,
  PoolLike,
  Queryable,
} from "../src/outbox/types.js";

const EVENT_ID = "018f2f7e-6a4a-7a3b-9f5a-2b1c3d4e5f60";
const CART_ID = "018f2f7e-1111-7a3b-9f5a-2a2b3c4d5e61";

interface Recorded {
  text: string;
  values?: readonly unknown[];
}

class FakeClient implements PoolClientLike {
  readonly calls: Recorded[] = [];
  rows: readonly unknown[] = [];
  released = 0;

  async query(
    text: string,
    values?: readonly unknown[],
  ): Promise<{ rows: readonly unknown[] }> {
    this.calls.push({ text, values });
    return { rows: this.rows };
  }

  release(): void {
    this.released += 1;
  }
}

class FakePool implements PoolLike {
  readonly calls: Recorded[] = [];
  readonly client = new FakeClient();
  rows: readonly unknown[] = [];

  async query(
    text: string,
    values?: readonly unknown[],
  ): Promise<{ rows: readonly unknown[] }> {
    this.calls.push({ text, values });
    return { rows: this.rows };
  }

  async connect(): Promise<PoolClientLike> {
    return this.client;
  }
}

function newStore(pool: PoolLike, schema = "cart"): PostgresOutboxStore {
  return new PostgresOutboxStore({ pool: () => pool, schema, producer: "cart-svc" });
}

describe("PostgresOutboxStore.write", () => {
  it("inserts through the caller's client and never touches the pool", async () => {
    const pool = new FakePool();
    const client: Queryable = new FakeClient();
    const store = newStore(pool);

    const eventId = await store.write(client, {
      topic: "cart.updated",
      eventId: EVENT_ID,
      aggregateType: "cart",
      aggregateId: CART_ID,
      occurredAt: new Date("2026-10-30T10:00:00.000Z"),
      payload: {
        cartId: CART_ID,
        userId: null,
        sessionId: "guest-1",
        status: "active",
        items: [],
        totals: { subtotal: 0, itemCount: 0, lineCount: 0 },
        updatedAt: "2026-10-30T10:00:00.000Z",
      },
    });

    expect(eventId).toBe(EVENT_ID);
    expect(pool.calls).toHaveLength(0);

    const calls = (client as FakeClient).calls;
    expect(calls).toHaveLength(1);
    // Quoted identifiers: `order` is a reserved word, so `order.outbox` would
    // not parse. Quoting is a no-op for `cart`.
    expect(calls[0]!.text).toContain('INSERT INTO "cart"."outbox"');
    expect(calls[0]!.values?.[0]).toBe(EVENT_ID);
    expect(calls[0]!.values?.[3]).toBe("cart.updated");

    const envelope = JSON.parse(String(calls[0]!.values?.[4])) as Record<
      string,
      unknown
    >;
    expect(envelope).toMatchObject({
      id: EVENT_ID,
      type: "cart.updated",
      topic: "cart.updated",
      eventType: "cart.updated",
      producer: "cart-svc",
      idempotencyKey: EVENT_ID,
    });
  });

  it("refuses an identifier that is not a plain schema name", () => {
    const pool = new FakePool();

    expect(() => newStore(pool, "cart; drop table cart.carts")).toThrow(
      /invalid outbox schema/,
    );
    expect(() => newStore(pool, "Cart")).toThrow(/invalid outbox schema/);
  });

  it("quotes a reserved schema name so the order service can use it", async () => {
    const pool = new FakePool();
    const client = new FakeClient();

    await newStore(pool, "order").write(client, {
      topic: "cart.updated",
      eventId: EVENT_ID,
      aggregateType: "order",
      aggregateId: CART_ID,
      occurredAt: new Date("2026-10-30T10:00:00.000Z"),
      payload: {
        cartId: CART_ID,
        userId: null,
        sessionId: null,
        status: "active",
        items: [],
        totals: { subtotal: 0, itemCount: 0, lineCount: 0 },
        updatedAt: "2026-10-30T10:00:00.000Z",
      },
    });

    expect(client.calls[0]!.text).toContain('INSERT INTO "order"."outbox"');
  });
});

describe("PostgresOutboxStore.claimBatch", () => {
  it("leases rows in creation order with SKIP LOCKED", async () => {
    const pool = new FakePool();
    pool.rows = [
      {
        id: "row-1",
        event_id: EVENT_ID,
        aggregate_type: "cart",
        aggregate_id: CART_ID,
        event_type: "cart.updated",
        envelope: {},
        attempts: 0,
      },
    ];

    const rows = await newStore(pool).claimBatch(10, 30);

    expect(rows).toHaveLength(1);
    expect(pool.calls[0]!.text).toContain("FOR UPDATE SKIP LOCKED");
    expect(pool.calls[0]!.text).toContain("ORDER BY created_at");
    expect(pool.calls[0]!.text).toContain("locked_until IS NULL OR locked_until < now()");
    expect(pool.calls[0]!.values).toEqual([10, 30]);
  });
});

describe("PostgresOutboxStore.markFailed", () => {
  it("increments the attempt and lets the database decide the status", async () => {
    const pool = new FakePool();
    pool.rows = [{ attempts: 5, status: "failed" }];

    const result = await newStore(pool).markFailed(
      "row-1",
      "kafka is down",
      5,
      60,
    );

    expect(pool.calls[0]!.text).toContain("attempts + 1 >= $2");
    expect(pool.calls[0]!.values).toEqual(["row-1", 5, "kafka is down", 60]);
    expect(result).toEqual({ attempts: 5, status: "failed" });
  });

  it("throws when the row vanished", async () => {
    const pool = new FakePool();

    await expect(
      newStore(pool).markFailed("row-1", "boom", 5, 60),
    ).rejects.toThrow(/disappeared/);
  });
});

describe("PostgresOutboxStore.tryAcquireRelayLock", () => {
  it("returns null when another worker holds the lock", async () => {
    const pool = new FakePool();
    pool.client.rows = [{ locked: false }];

    await expect(newStore(pool).tryAcquireRelayLock()).resolves.toBeNull();
    expect(pool.client.released).toBe(1);
  });

  it("returns an idempotent release that unlocks and returns the client", async () => {
    const pool = new FakePool();
    pool.client.rows = [{ locked: true }];

    const release = await newStore(pool).tryAcquireRelayLock();
    expect(release).not.toBeNull();

    await release!();
    await release!();

    expect(pool.client.released).toBe(1);
    const unlock = pool.client.calls.filter((call) =>
      call.text.includes("pg_advisory_unlock"),
    );
    expect(unlock).toHaveLength(1);
  });
});
