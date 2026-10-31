import { describe, expect, it } from "vitest";
import type { EventEnvelope, EventTopic } from "@ecom/contracts";
import { buildEventEnvelope } from "../src/envelope.js";
import { RecordingLogger } from "../src/logger.js";
import { OutboxRelay } from "../src/outbox/outbox.relay.js";
import type {
  MarkFailedResult,
  OutboxRow,
  OutboxStorePort,
  OutboxWriteInput,
  Queryable,
  RelayLockRelease,
} from "../src/outbox/types.js";
import { RecordingPublisher } from "../src/publishers/recording.publisher.js";

const CART_ID = "018f2f7e-1111-7a3b-9f5a-2a2b3c4d5e61";
const OTHER_CART_ID = "018f2f7e-9999-7a3b-9f5a-2a2b3c4d5e69";

function envelopeFor(eventId: string, cartId = CART_ID): EventEnvelope {
  return buildEventEnvelope({
    id: eventId,
    topic: "cart.updated",
    producer: "cart-svc",
    timestamp: new Date("2026-10-30T10:00:00.000Z"),
    payload: {
      cartId,
      userId: null,
      sessionId: "guest-1",
      status: "active",
      items: [],
      totals: { subtotal: 0, itemCount: 0, lineCount: 0 },
      updatedAt: "2026-10-30T10:00:00.000Z",
    },
  });
}

/** `row-01` -> event id ending in `01`, so assertions stay readable. */
function row(id: string): OutboxRow {
  const suffix = id.slice(-2);

  return {
    id,
    event_id: `00000000-0000-7000-8000-0000000000${suffix}`,
    aggregate_type: "cart",
    aggregate_id: id === "row-02" ? OTHER_CART_ID : CART_ID,
    event_type: "cart.updated",
    envelope: envelopeFor(`00000000-0000-7000-8000-0000000000${suffix}`),
    attempts: 0,
  };
}

class FakeStore implements OutboxStorePort {
  queue: OutboxRow[] = [];
  lockAvailable = true;
  claimCalls = 0;
  readonly published: string[] = [];
  readonly failures: {
    id: string;
    error: string;
    maxAttempts: number;
    backoff: number;
  }[] = [];
  readonly results: MarkFailedResult[] = [];
  private readonly attempts = new Map<string, number>();

  async write<T extends EventTopic>(
    _client: Queryable,
    _input: OutboxWriteInput<T>,
  ): Promise<string> {
    throw new Error("the relay never writes");
  }

  async claimBatch(limit: number): Promise<OutboxRow[]> {
    this.claimCalls += 1;
    return this.queue.splice(0, limit);
  }

  async markPublished(id: string): Promise<void> {
    this.published.push(id);
  }

  async markFailed(
    id: string,
    error: string,
    maxAttempts: number,
    backoff: number,
  ): Promise<MarkFailedResult> {
    this.failures.push({ id, error, maxAttempts, backoff });

    const attempts = (this.attempts.get(id) ?? 0) + 1;
    this.attempts.set(id, attempts);

    const result: MarkFailedResult = {
      attempts,
      status: attempts >= maxAttempts ? "failed" : "pending",
    };
    this.results.push(result);

    return result;
  }

  async tryAcquireRelayLock(): Promise<RelayLockRelease | null> {
    return this.lockAvailable ? async () => undefined : null;
  }
}

function newRelay(store: FakeStore, publisher: RecordingPublisher): OutboxRelay {
  return new OutboxRelay({
    store,
    publisher,
    logger: new RecordingLogger(),
    batchSize: 10,
    leaseSeconds: 30,
    maxAttempts: 5,
  });
}

describe("OutboxRelay.drainOnce", () => {
  it("publishes each claimed row and marks it published", async () => {
    const store = new FakeStore();
    const publisher = new RecordingPublisher();
    store.queue = [row("row-01"), row("row-02")];

    const result = await newRelay(store, publisher).drainOnce();

    expect(result).toEqual({ published: 2, failed: 0, skipped: false });
    expect(store.published).toEqual(["row-01", "row-02"]);
    expect(publisher.published.map((entry) => entry.key)).toEqual([
      CART_ID,
      OTHER_CART_ID,
    ]);
    expect(
      publisher.published.every((entry) => entry.topic === "cart.updated"),
    ).toBe(true);
  });

  it("publishes in claim order", async () => {
    const store = new FakeStore();
    const publisher = new RecordingPublisher();
    store.queue = [row("row-01"), row("row-02")];

    await newRelay(store, publisher).drainOnce();

    expect(publisher.published.map((entry) => entry.envelope.id)).toEqual([
      "00000000-0000-7000-8000-000000000001",
      "00000000-0000-7000-8000-000000000002",
    ]);
  });

  it("records a failed attempt, applies the first backoff and keeps the row", async () => {
    const store = new FakeStore();
    const publisher = new RecordingPublisher();
    publisher.failNext = 1;
    store.queue = [row("row-01")];

    const result = await newRelay(store, publisher).drainOnce();

    expect(result).toEqual({ published: 0, failed: 1, skipped: false });
    expect(store.published).toEqual([]);
    expect(store.failures).toHaveLength(1);
    expect(store.failures[0]).toMatchObject({
      id: "row-01",
      maxAttempts: 5,
      backoff: 2,
    });
    expect(store.failures[0]!.error).toMatch(/unavailable/);
  });

  it("doubles the backoff on each attempt", async () => {
    const store = new FakeStore();
    const publisher = new RecordingPublisher();
    publisher.failNext = 4;
    const relay = newRelay(store, publisher);

    for (let attempt = 0; attempt < 4; attempt += 1) {
      // The store hands back the attempt counter it persisted on the row.
      store.queue = [{ ...row("row-01"), attempts: attempt }];
      await relay.drainOnce();
    }

    expect(store.failures.map((failure) => failure.backoff)).toEqual([
      2, 4, 8, 16,
    ]);
  });

  it("parks a row as failed on the fifth attempt", async () => {
    const store = new FakeStore();
    const publisher = new RecordingPublisher();
    publisher.failNext = 5;
    const relay = newRelay(store, publisher);

    for (let attempt = 0; attempt < 5; attempt += 1) {
      store.queue = [row("row-01")];
      await relay.drainOnce();
    }

    expect(store.results.map((result) => result.status)).toEqual([
      "pending",
      "pending",
      "pending",
      "pending",
      "failed",
    ]);
    expect(store.published).toEqual([]);
  });

  it("does nothing when another worker holds the relay lock", async () => {
    const store = new FakeStore();
    const publisher = new RecordingPublisher();
    store.lockAvailable = false;
    store.queue = [row("row-01")];

    const result = await newRelay(store, publisher).drainOnce();

    expect(result).toEqual({ published: 0, failed: 0, skipped: true });
    expect(store.claimCalls).toBe(0);
    expect(publisher.published).toEqual([]);
  });

  it("parks a row whose event type is not in the registry", async () => {
    const store = new FakeStore();
    const publisher = new RecordingPublisher();
    store.queue = [{ ...row("row-01"), event_type: "cart.exploded" }];

    const result = await newRelay(store, publisher).drainOnce();

    expect(result).toEqual({ published: 0, failed: 1, skipped: false });
    expect(store.failures[0]).toMatchObject({ id: "row-01", maxAttempts: 1 });
    expect(publisher.published).toEqual([]);
  });
});

describe("OutboxRelay timer", () => {
  it("stops polling after stop()", async () => {
    const store = new FakeStore();
    const publisher = new RecordingPublisher();
    const relay = new OutboxRelay({
      store,
      publisher,
      logger: new RecordingLogger(),
      intervalMs: 5,
    });

    relay.start();
    relay.start();
    await new Promise((resolve) => setTimeout(resolve, 40));
    relay.stop();
    relay.stop();

    const callsAfterStop = store.claimCalls;
    expect(callsAfterStop).toBeGreaterThan(0);

    await new Promise((resolve) => setTimeout(resolve, 40));
    expect(store.claimCalls).toBe(callsAfterStop);
  });
});
