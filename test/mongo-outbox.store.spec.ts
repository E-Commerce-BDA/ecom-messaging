import { beforeEach, describe, expect, it } from "vitest";
import { MongoOutboxStore } from "../src/outbox/mongo-outbox.store.js";
import type {
  MongoCollectionLike,
  MongoFindOneAndUpdateResult,
  MongoOutboxDocument,
  MongoOutboxDb,
  MongoSessionLike,
  MongoUpdateLike,
} from "../src/outbox/types.js";

const CART_ID = "018f2f7e-1111-7a3b-9f5a-2a2b3c4d5e61";

function eventId(n: number): string {
  return `018f2f7e-6a4a-7a3b-9f5a-${String(n).padStart(12, "0")}`;
}

type Doc = MongoOutboxDocument;

/** Applies the subset of Mongo update syntax the store is allowed to use. */
function applyUpdate(doc: Doc, update: MongoUpdateLike): Doc {
  const stages = Array.isArray(update) ? update : [update];
  let next: Record<string, unknown> = { ...doc };

  for (const stage of stages) {
    const set = (stage as { $set?: Record<string, unknown> }).$set ?? {};

    for (const [field, value] of Object.entries(set)) {
      next[field] = evaluate(value, next);
    }
  }

  return next as Doc;
}

function setOf(update: MongoUpdateLike): Record<string, unknown> {
  const stage = Array.isArray(update) ? update[0] : update;
  return (stage as { $set?: Record<string, unknown> }).$set ?? {};
}

/** Resolves the aggregation expressions the store uses (`$add`, `$cond`, `$gte`). */
function evaluate(expression: unknown, doc: Record<string, unknown>): unknown {
  if (!expression || typeof expression !== "object" || Array.isArray(expression)) {
    return expression;
  }

  const node = expression as Record<string, unknown>;

  if (Array.isArray(node.$add)) {
    const [left, right] = node.$add;
    return Number(resolve(left, doc)) + Number(resolve(right, doc));
  }
  if (Array.isArray(node.$gte)) {
    const [left, right] = node.$gte;
    return Number(resolve(left, doc)) >= Number(resolve(right, doc));
  }
  if (Array.isArray(node.$cond)) {
    const [test, whenTrue, whenFalse] = node.$cond;
    return resolve(test, doc) ? whenTrue : whenFalse;
  }

  return expression;
}

function resolve(value: unknown, doc: Record<string, unknown>): unknown {
  if (typeof value === "string" && value.startsWith("$")) {
    return doc[value.slice(1)];
  }
  return evaluate(value, doc);
}

function matches(condition: unknown, actual: unknown): boolean {
  if (condition === null) {
    return actual === null || actual === undefined;
  }
  if (condition && typeof condition === "object" && !Array.isArray(condition)) {
    const node = condition as Record<string, unknown>;

    if ("$lt" in node) {
      return actual instanceof Date && (node.$lt as Date).getTime() > actual.getTime();
    }
  }

  return actual === condition;
}

class FakeCollection implements MongoCollectionLike<Doc> {
  readonly docs: Doc[] = [];
  readonly indexes: { spec: Record<string, 1 | -1>; name?: string }[] = [];
  readonly sessions: (MongoSessionLike | undefined)[] = [];
  /**
   * Driver 6.20 resolves `findOneAndUpdate` to the document; the typings and
   * earlier releases wrap it in `{ value }`. Flipping this exercises the other
   * shape without a second fake.
   */
  bareResults = false;

  async insertOne(doc: Doc, options?: { session?: MongoSessionLike }): Promise<unknown> {
    this.sessions.push(options?.session);

    if (this.docs.some((existing) => existing._id === doc._id)) {
      throw Object.assign(new Error("E11000 duplicate key"), { code: 11000 });
    }

    this.docs.push({ ...doc });
    return { insertedId: doc._id };
  }

  async findOneAndUpdate(
    filter: Record<string, unknown>,
    update: MongoUpdateLike,
    options?: { upsert?: boolean },
  ): Promise<MongoFindOneAndUpdateResult<Doc>> {
    const candidates = this.docs.filter((doc) => this.matchesFilter(doc, filter));

    if (candidates.length === 0) {
      if (options?.upsert) {
        const filterFields = { ...filter };
        delete filterFields.$or;
        const created = { ...filterFields, ...setOf(update) } as unknown as Doc;
        if (this.docs.some((existing) => existing._id === created._id)) {
          throw Object.assign(new Error("E11000 duplicate key"), { code: 11000 });
        }
        this.docs.push(created);
        return this.bareResults ? created : { value: created };
      }
      return this.bareResults ? null : { value: null };
    }

    const target = candidates[0]!;
    const next = applyUpdate(target, update);
    this.docs[this.docs.indexOf(target)] = next;
    return this.bareResults ? next : { value: next };
  }

  async updateOne(
    filter: Record<string, unknown>,
    update: MongoUpdateLike,
  ): Promise<unknown> {
    const target = this.docs.find((doc) => this.matchesFilter(doc, filter));
    if (!target) {
      return { matchedCount: 0 };
    }

    this.docs[this.docs.indexOf(target)] = applyUpdate(target, update);
    return { matchedCount: 1 };
  }

  async createIndex(
    spec: Record<string, 1 | -1>,
    options?: { name?: string },
  ): Promise<string> {
    this.indexes.push({ spec, name: options?.name });
    return options?.name ?? "index";
  }

  private matchesFilter(doc: Doc, filter: Record<string, unknown>): boolean {
    return Object.entries(filter).every(([field, condition]) => {
      if (field === "$or") {
        return (condition as Record<string, unknown>[]).some((clause) =>
          this.matchesFilter(doc, clause),
        );
      }

      return matches(condition, (doc as Record<string, unknown>)[field]);
    });
  }
}

class FakeDb implements MongoOutboxDb {
  readonly collections = new Map<string, FakeCollection>();

  collection(name: string): FakeCollection {
    let existing = this.collections.get(name);
    if (!existing) {
      existing = new FakeCollection();
      this.collections.set(name, existing);
    }
    return existing;
  }

  get events(): FakeCollection {
    return this.collection("cms_outbox");
  }

  get locks(): FakeCollection {
    return this.collection("cms_outbox_locks");
  }
}

const SESSION: MongoSessionLike = { id: "session-1" };

function newStore(db: FakeDb): MongoOutboxStore {
  return new MongoOutboxStore({
    db: () => db,
    collection: "cms_outbox",
    producer: "cms-svc",
  });
}

function cmsPayload(key = "home") {
  return { scope: "pages", version: 1, key };
}

function seed(
  collection: FakeCollection,
  overrides: Partial<Doc> & { _id: string },
): Doc {
  const base: Omit<Doc, "_id"> = {
    event_id: overrides._id,
    aggregate_type: "page",
    aggregate_id: "home",
    event_type: "cms.updated",
    envelope: {
      id: overrides._id,
      type: "cms.updated",
      topic: "cms.updated",
      eventType: "cms.updated",
      version: 1,
      timestamp: "2026-10-31T10:00:00.000Z",
      producer: "cms-svc",
      idempotencyKey: overrides._id,
      payload: cmsPayload(),
    },
    status: "pending",
    attempts: 0,
    last_error: null,
    locked_until: null,
    created_at: new Date("2026-10-31T10:00:00.000Z"),
    published_at: null,
  };

  const doc = { ...base, ...overrides } as Doc;
  collection.docs.push(doc);
  return doc;
}

describe("MongoOutboxStore.write", () => {
  it("inserts the envelope through the caller's session", async () => {
    const db = new FakeDb();
    const store = newStore(db);

    const id = await store.write(SESSION, {
      topic: "cms.updated",
      eventId: eventId(1),
      aggregateType: "page",
      aggregateId: "home",
      occurredAt: new Date("2026-10-31T10:00:00.000Z"),
      payload: cmsPayload(),
    });

    expect(id).toBe(eventId(1));
    expect(db.events.sessions).toEqual([SESSION]);

    const [stored] = db.events.docs;
    expect(stored).toMatchObject({
      _id: eventId(1),
      event_type: "cms.updated",
      status: "pending",
      attempts: 0,
    });
    expect(stored!.envelope).toMatchObject({
      id: eventId(1),
      type: "cms.updated",
      producer: "cms-svc",
      idempotencyKey: eventId(1),
    });
  });

  it("rejects a payload that drifted from its contract before it is stored", async () => {
    const db = new FakeDb();

    await expect(
      newStore(db).write(SESSION, {
        topic: "cms.updated",
        eventId: eventId(2),
        aggregateType: "page",
        aggregateId: "home",
        occurredAt: new Date("2026-10-31T10:00:00.000Z"),
        // `version` is required and `pages` is not a scope the schema knows
        // nothing about — the extra key is what `.strict()` rejects.
        payload: { scope: "pages", key: "home", extra: true } as never,
      }),
    ).rejects.toThrow();

    expect(db.events.docs).toHaveLength(0);
  });
});

describe("MongoOutboxStore.claimBatch", () => {
  it("leases pending rows in creation order, one atomic update per row", async () => {
    const db = new FakeDb();
    seed(db.events, { _id: eventId(1) });
    seed(db.events, {
      _id: eventId(2),
      created_at: new Date("2026-10-31T10:00:01.000Z"),
    });

    const rows = await newStore(db).claimBatch(10, 30);

    expect(rows.map((row) => row.id)).toEqual([eventId(1), eventId(2)]);
    expect(rows[0]!.envelope.type).toBe("cms.updated");
    expect(db.events.docs.every((doc) => doc.locked_until instanceof Date)).toBe(
      true,
    );
  });

  it("skips a row whose lease is still running", async () => {
    const db = new FakeDb();
    seed(db.events, {
      _id: eventId(1),
      locked_until: new Date(Date.now() + 60_000),
    });

    await expect(newStore(db).claimBatch(10, 30)).resolves.toEqual([]);
  });

  it("reclaims a row whose lease expired", async () => {
    const db = new FakeDb();
    seed(db.events, {
      _id: eventId(1),
      locked_until: new Date(Date.now() - 60_000),
    });

    const rows = await newStore(db).claimBatch(10, 30);

    expect(rows).toHaveLength(1);
  });

  it("never claims more than the limit or anything that is not pending", async () => {
    const db = new FakeDb();
    seed(db.events, { _id: eventId(1) });
    seed(db.events, { _id: eventId(2) });
    seed(db.events, { _id: eventId(3), status: "published" });

    const rows = await newStore(db).claimBatch(1, 30);

    expect(rows).toHaveLength(1);
  });

  it("claims when the driver resolves the bare document instead of { value }", async () => {
    const db = new FakeDb();
    // Driver 6.20 shape: `findOneAndUpdate` returns the document itself.
    db.events.bareResults = true;
    seed(db.events, { _id: eventId(1) });

    const rows = await newStore(db).claimBatch(10, 30);

    expect(rows.map((row) => row.id)).toEqual([eventId(1)]);
    expect(db.events.docs[0]!.locked_until).toBeInstanceOf(Date);
  });
});

describe("MongoOutboxStore.markPublished", () => {
  it("clears the lease and stamps published_at", async () => {
    const db = new FakeDb();
    seed(db.events, {
      _id: eventId(1),
      locked_until: new Date(),
    });

    await newStore(db).markPublished(eventId(1));

    expect(db.events.docs[0]).toMatchObject({
      status: "published",
      locked_until: null,
      last_error: null,
    });
    expect(db.events.docs[0]!.published_at).toBeInstanceOf(Date);
  });
});

describe("MongoOutboxStore.markFailed", () => {
  let db: FakeDb;

  beforeEach(() => {
    db = new FakeDb();
  });

  it("counts the attempt, applies the backoff and keeps the row", async () => {
    seed(db.events, { _id: eventId(1) });

    const result = await newStore(db).markFailed(
      eventId(1),
      "kafka is down",
      5,
      8,
    );

    expect(result).toEqual({ attempts: 1, status: "pending" });
    const [doc] = db.events.docs;
    expect(doc).toMatchObject({ status: "pending", last_error: "kafka is down" });
    expect(doc!.locked_until!.getTime()).toBeGreaterThan(Date.now());
  });

  it("parks the row on the last attempt", async () => {
    seed(db.events, { _id: eventId(1), attempts: 4 });

    const result = await newStore(db).markFailed(eventId(1), "boom", 5, 60);

    expect(result).toEqual({ attempts: 5, status: "failed" });
    expect(db.events.docs[0]).toMatchObject({ status: "failed", locked_until: null });
  });

  it("still counts the attempt when the driver resolves the bare document", async () => {
    db.events.bareResults = true;
    seed(db.events, { _id: eventId(1) });

    const result = await newStore(db).markFailed(eventId(1), "boom", 5, 2);

    expect(result).toEqual({ attempts: 1, status: "pending" });
  });

  it("truncates the error to 500 characters", async () => {
    seed(db.events, { _id: eventId(1) });

    await newStore(db).markFailed(eventId(1), "x".repeat(900), 5, 2);

    expect(db.events.docs[0]!.last_error).toHaveLength(500);
  });

  it("throws when the row vanished", async () => {
    await expect(
      newStore(db).markFailed(eventId(9), "boom", 5, 2),
    ).rejects.toThrow(/disappeared/);
  });
});

describe("MongoOutboxStore.tryAcquireRelayLock", () => {
  it("takes the lease and releases it idempotently", async () => {
    const db = new FakeDb();
    const store = newStore(db);

    const release = await store.tryAcquireRelayLock(30);
    expect(release).not.toBeNull();

    await release!();
    await release!();

    expect(db.locks.docs[0]!.locked_until).toEqual(new Date(0));
  });

  it("returns null while another worker's lease is running", async () => {
    const db = new FakeDb();
    db.locks.docs.push({
      _id: "relay",
      locked_until: new Date(Date.now() + 60_000),
    } as unknown as Doc);

    await expect(newStore(db).tryAcquireRelayLock(30)).resolves.toBeNull();
  });

  it("takes over an expired lease", async () => {
    const db = new FakeDb();
    db.locks.docs.push({
      _id: "relay",
      locked_until: new Date(Date.now() - 60_000),
    } as unknown as Doc);

    const release = await newStore(db).tryAcquireRelayLock(30);

    expect(release).not.toBeNull();
  });
});

describe("MongoOutboxStore.initialize", () => {
  it("creates the unique event id index and the partial pending index", async () => {
    const db = new FakeDb();

    await newStore(db).initialize();

    expect(db.events.indexes).toEqual([
      { spec: { event_id: 1 }, name: "uq_outbox_event_id" },
      { spec: { status: 1, created_at: 1 }, name: "idx_outbox_pending" },
    ]);
  });
});
