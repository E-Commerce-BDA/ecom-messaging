import { describe, expect, it } from "vitest";
import {
  KafkaConsumerStalledError,
  KafkaEventHandler,
} from "../../src/consumer/kafka-event-handler.js";
import { ConsumerMetrics } from "../../src/consumer/consumer-metrics.js";
import type { KafkaHandlerOutcome } from "../../src/consumer/types.js";
import {
  FakeContext,
  FakeConsumer,
  FakeSleeper,
  MESSAGE,
  RecordingDlqPublisher,
  SilentLogger,
} from "./fakes.js";

const TOPIC = "order.created";
const ENVELOPE = { id: "evt-1", type: TOPIC, payload: { orderId: "o-1" } };
const RAW = JSON.stringify(ENVELOPE);

function harness(options: { translateError?: (error: unknown) => string | null } = {}) {
  const dlq = new RecordingDlqPublisher();
  const metrics = new ConsumerMetrics(() => new Date("2026-10-31T12:00:00.000Z"));
  const logger = new SilentLogger();
  const sleeper = new FakeSleeper();
  const handler = new KafkaEventHandler({
    dlq,
    metrics,
    logger,
    sleep: (ms) => sleeper.sleep(ms),
    translateError: options.translateError,
  });

  return { dlq, metrics, logger, sleeper, handler };
}

function context(offset = MESSAGE.offset, partition = 3) {
  const consumer = new FakeConsumer();
  const ctx = new FakeContext(
    consumer,
    { ...MESSAGE, offset },
    partition,
  );
  return { consumer, ctx };
}

const applied: KafkaHandlerOutcome = { kind: "applied" };

describe("KafkaEventHandler — the happy paths", () => {
  it("commits the offset one past the message and counts the commit", async () => {
    const { handler, metrics } = harness();
    const { consumer, ctx } = context("41");

    await handler.run(ctx, TOPIC, RAW, async () => applied);

    expect(consumer.commits).toEqual([
      { topic: TOPIC, partition: 3, offset: "42" },
    ]);
    expect(metrics.snapshot().commit).toBe(1);
    expect(metrics.snapshot().accepted).toBe(1);
    expect(metrics.lastCommit()).toEqual({
      topic: TOPIC,
      partition: 3,
      offset: "41",
      committedAt: "2026-10-31T12:00:00.000Z",
    });
  });

  it("passes the parsed envelope to the handler, not the raw string", async () => {
    const { handler } = harness();
    const { ctx } = context();
    let seen: unknown;

    await handler.run(ctx, TOPIC, RAW, async (envelope) => {
      seen = envelope;
      return applied;
    });

    expect(seen).toEqual(ENVELOPE);
  });

  it("counts a duplicate as its own counter and still commits", async () => {
    const { handler, metrics } = harness();
    const { consumer, ctx } = context();

    await handler.run(ctx, TOPIC, RAW, async () => ({
      kind: "duplicate",
      eventId: "evt-1",
    }));

    expect(metrics.snapshot().duplicate).toBe(1);
    expect(consumer.commits).toHaveLength(1);
  });

  it("honours the handler's own success counter", async () => {
    const { handler, metrics } = harness();
    const { ctx } = context();

    await handler.run(ctx, TOPIC, RAW, async () => ({
      kind: "applied",
      counter: "committed",
    }));

    expect(metrics.snapshot().committed).toBe(1);
    expect(metrics.snapshot().accepted).toBe(0);
  });

  it("ignores a well-formed event without a DLQ record", async () => {
    const { handler, metrics, dlq } = harness();
    const { ctx } = context();

    await handler.run(ctx, TOPIC, RAW, async () => ({
      kind: "ignored",
      reason: "not-subscribed",
      type: "order.created",
    }));

    expect(dlq.published).toHaveLength(0);
    expect(metrics.snapshot().ignored).toBe(1);
  });
});

describe("KafkaEventHandler — unreadable records", () => {
  it("dead-letters malformed JSON with the raw value", async () => {
    const { handler, metrics, dlq } = harness();
    const { consumer, ctx } = context();

    await handler.run(ctx, TOPIC, "{not json", async () => applied);

    expect(dlq.published).toHaveLength(1);
    expect(dlq.published[0]).toMatchObject({
      reason: "invalid-json",
      rawValue: "{not json",
      source: { topic: TOPIC, partition: 3, offset: "41" },
      key: "order-1",
    });
    expect(metrics.snapshot().invalid).toBe(1);
    expect(metrics.snapshot().dlq).toBe(1);
    expect(consumer.commits).toHaveLength(1);
  });

  it("maps an envelope-stage rejection to invalid-envelope", async () => {
    const { handler, metrics, dlq } = harness();
    const { consumer, ctx } = context();

    await handler.run(ctx, TOPIC, RAW, async () => ({
      kind: "invalid",
      stage: "envelope",
      error: "event envelope requires type",
    }));

    expect(dlq.published[0]).toMatchObject({ reason: "invalid-envelope" });
    expect(metrics.snapshot().invalid).toBe(1);
    expect(consumer.commits).toHaveLength(1);
  });

  it("maps a payload-stage rejection to invalid-payload and keeps the envelope", async () => {
    const { handler, dlq } = harness();
    const { ctx } = context();

    await handler.run(ctx, TOPIC, RAW, async () => ({
      kind: "invalid",
      stage: "payload",
      error: "grandTotal must be non-negative",
    }));

    expect(dlq.published[0]).toMatchObject({
      reason: "invalid-payload",
      originalPayload: ENVELOPE,
    });
  });

  it("counts the DLQ write, not the invalid outcome, when both happen", async () => {
    const { handler, metrics } = harness();
    const { ctx } = context();

    await handler.run(ctx, TOPIC, RAW, async () => ({
      kind: "invalid",
      stage: "payload",
      error: "drift",
    }));

    const snapshot = metrics.snapshot();
    expect(snapshot.invalid).toBe(1);
    expect(snapshot.dlq).toBe(1);
  });
});

describe("KafkaEventHandler — retries", () => {
  it("retries a transient failure and commits on the eventual success", async () => {
    const { handler, metrics, sleeper } = harness();
    const { consumer, ctx } = context();
    let calls = 0;

    await handler.run(ctx, TOPIC, RAW, async () => {
      calls += 1;
      if (calls < 3) {
        throw new Error("deadlock detected");
      }
      return applied;
    });

    expect(calls).toBe(3);
    expect(sleeper.waited).toEqual([1000, 2000]);
    expect(consumer.commits).toHaveLength(1);
    expect(metrics.snapshot().accepted).toBe(1);
  });

  it("walks the whole ladder before dead-lettering", async () => {
    const { handler, sleeper } = harness();
    const { ctx } = context();

    await handler.run(ctx, TOPIC, RAW, async () => {
      throw new Error("connection reset");
    });

    expect(sleeper.waited).toEqual([1000, 2000, 4000, 8000, 16000]);
    expect(sleeper.waited).toHaveLength(5);
  });

  it("dead-letters transient-exhausted and commits only after the DLQ write", async () => {
    const { handler, metrics, dlq } = harness();
    const { consumer, ctx } = context();
    const order: string[] = [];
    dlq.publish = async (input) => {
      order.push("dlq");
      dlq.published.push(input);
      return "x";
    };
    consumer.commitOffsets = async (offsets) => {
      order.push("commit");
      consumer.commits.push(...offsets);
    };

    await handler.run(ctx, TOPIC, RAW, async () => {
      throw new Error("pool exhausted");
    });

    expect(dlq.published[0]).toMatchObject({ reason: "transient-exhausted" });
    expect(order).toEqual(["dlq", "commit"]);
    expect(metrics.snapshot().dlq).toBe(1);
  });

  it("dead-letters a terminal error immediately, without a single retry", async () => {
    const { handler, sleeper, dlq } = harness({
      translateError: (error) =>
        error instanceof RangeError ? "reservation-closed" : null,
    });
    const { ctx } = context();

    await handler.run(ctx, TOPIC, RAW, async () => {
      throw new RangeError("hold was released");
    });

    expect(sleeper.waited).toEqual([]);
    expect(dlq.published[0]).toMatchObject({
      reason: "reservation-closed",
      error: "hold was released",
    });
  });
});

describe("KafkaEventHandler — a failed DLQ write is the only reason to throw", () => {
  it("leaves the offset uncommitted and throws KafkaConsumerStalledError", async () => {
    const { handler, metrics, dlq } = harness();
    const { consumer, ctx } = context();
    dlq.failure = new Error("broker unavailable");

    await expect(
      handler.run(ctx, TOPIC, RAW, async () => {
        throw new Error("unreachable");
      }),
    ).rejects.toBeInstanceOf(KafkaConsumerStalledError);

    expect(consumer.commits).toHaveLength(0);
    expect(metrics.snapshot().dlq).toBe(0);
    expect(metrics.snapshot().stalled).toBe(1);
  });

  it("carries the source coordinates so the alert names the record", async () => {
    const { handler, dlq } = harness();
    const { ctx } = context("77", 2);
    dlq.failure = new Error("broker unavailable");

    const error = await handler
      .run(ctx, TOPIC, "{bad", async () => applied)
      .catch((caught: unknown) => caught);

    expect(error).toBeInstanceOf(KafkaConsumerStalledError);
    expect((error as KafkaConsumerStalledError).source).toEqual({
      topic: TOPIC,
      partition: 2,
      offset: "77",
    });
  });

  it("leaves the offset alone when the dead letter of an invalid envelope fails", async () => {
    const { handler, dlq } = harness();
    const { consumer, ctx } = context();
    dlq.failure = new Error("broker unavailable");
    let calls = 0;

    await handler
      .run(ctx, TOPIC, RAW, async () => {
        calls += 1;
        return { kind: "invalid", stage: "envelope", error: "bad" };
      })
      .catch(() => undefined);

    expect(calls).toBe(1);
    expect(consumer.commits).toHaveLength(0);
  });
});

describe("KafkaEventHandler — counters", () => {
  it("starts every counter at zero and reports the whole set", () => {
    const metrics = new ConsumerMetrics();

    expect(metrics.snapshot()).toEqual({
      accepted: 0,
      skipped: 0,
      committed: 0,
      duplicate: 0,
      ignored: 0,
      invalid: 0,
      dlq: 0,
      commit: 0,
      stalled: 0,
    });
  });

  it("resets the counters and the commit mark", async () => {
    const { handler, metrics } = harness();
    const { ctx } = context();

    await handler.run(ctx, TOPIC, RAW, async () => applied);
    metrics.reset();

    expect(metrics.snapshot().commit).toBe(0);
    expect(metrics.lastCommit()).toBeNull();
  });

  it("counts skip separately from accept", async () => {
    const { handler, metrics } = harness();
    const { ctx } = context();

    await handler.run(ctx, TOPIC, RAW, async () => ({
      kind: "applied",
      counter: "skipped",
    }));

    expect(metrics.snapshot().skipped).toBe(1);
    expect(metrics.snapshot().accepted).toBe(0);
  });
});
