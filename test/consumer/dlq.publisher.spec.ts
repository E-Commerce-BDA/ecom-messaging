import { describe, expect, it } from "vitest";
import {
  KafkaDlqPublisher,
  dlqTopicNameFor,
  type KafkaProducerLike,
} from "../../src/consumer/dlq.publisher.js";

class FakeProducer implements KafkaProducerLike {
  readonly sent: {
    topic: string;
    messages: { key?: string | null; value: string }[];
  }[] = [];
  connects = 0;
  disconnects = 0;
  failure: Error | null = null;

  async connect(): Promise<void> {
    this.connects += 1;
  }

  async disconnect(): Promise<void> {
    this.disconnects += 1;
  }

  async send(record: {
    topic: string;
    messages: { key?: string | null; value: string }[];
  }): Promise<unknown> {
    if (this.failure) {
      throw this.failure;
    }
    this.sent.push(record);
    return [];
  }
}

function newPublisher(producer = new FakeProducer()) {
  const publisher = new KafkaDlqPublisher({
    brokers: ["localhost:9092"],
    clientId: "search-svc",
    producer,
    clock: () => new Date("2026-10-31T12:00:00.000Z"),
  });

  return { publisher, producer };
}

describe("dlqTopicNameFor", () => {
  it("takes the twin name from the registry", () => {
    expect(dlqTopicNameFor("order.created")).toBe("order.created.dlq");
    expect(dlqTopicNameFor("product.imported")).toBe("product.imported.dlq");
  });

  it("falls back to a suffix for a topic the registry does not know", () => {
    expect(dlqTopicNameFor("legacy.thing")).toBe("legacy.thing.dlq");
  });
});

describe("KafkaDlqPublisher", () => {
  it("writes the record to the twin topic with its source coordinates", async () => {
    const { publisher, producer } = newPublisher();

    const topic = await publisher.publish({
      source: { topic: "product.imported", partition: 1, offset: "8" },
      reason: "invalid-payload",
      error: "sku must be a string",
      originalPayload: { id: "evt-1" },
      key: "product-9",
    });

    expect(topic).toBe("product.imported.dlq");
    expect(producer.sent).toHaveLength(1);
    expect(producer.sent[0]?.topic).toBe("product.imported.dlq");
    expect(producer.sent[0]?.messages[0]?.key).toBe("product-9");
    expect(JSON.parse(producer.sent[0]?.messages[0]?.value ?? "{}")).toEqual({
      source: { topic: "product.imported", partition: 1, offset: "8" },
      failedAt: "2026-10-31T12:00:00.000Z",
      reason: "invalid-payload",
      error: "sku must be a string",
      originalPayload: { id: "evt-1" },
    });
  });

  it("omits rawValue and originalPayload instead of writing nulls", async () => {
    const { publisher, producer } = newPublisher();

    await publisher.publish({
      source: { topic: "order.created", partition: 0, offset: "0" },
      reason: "transient-exhausted",
      error: "timeout",
    });

    const body = JSON.parse(producer.sent[0]?.messages[0]?.value ?? "{}");
    expect(Object.keys(body).sort()).toEqual([
      "error",
      "failedAt",
      "reason",
      "source",
    ]);
  });

  it("connects once for many publishes", async () => {
    const { publisher, producer } = newPublisher();

    await publisher.publish({
      source: { topic: "order.created", partition: 0, offset: "1" },
      reason: "a",
      error: "e",
    });
    await publisher.publish({
      source: { topic: "order.created", partition: 0, offset: "2" },
      reason: "b",
      error: "e",
    });

    expect(producer.connects).toBe(1);
    expect(producer.sent).toHaveLength(2);
  });

  it("propagates a broker failure so the caller can refuse to commit", async () => {
    const { publisher, producer } = newPublisher();
    producer.failure = new Error("LEADER_NOT_AVAILABLE");

    await expect(
      publisher.publish({
        source: { topic: "cart.updated", partition: 0, offset: "5" },
        reason: "invalid-json",
        error: "Unexpected token",
        rawValue: "{",
      }),
    ).rejects.toThrow("LEADER_NOT_AVAILABLE");
  });

  it("closes the producer it opened", async () => {
    const { publisher, producer } = newPublisher();

    await publisher.publish({
      source: { topic: "order.created", partition: 0, offset: "1" },
      reason: "a",
      error: "e",
    });
    await publisher.close();

    expect(producer.disconnects).toBe(1);
  });

  it("does not connect at all when nothing is dead-lettered", async () => {
    const { publisher, producer } = newPublisher();

    await publisher.close();

    expect(producer.connects).toBe(0);
    expect(producer.disconnects).toBe(0);
  });

  it("requires a broker", () => {
    expect(
      () => new KafkaDlqPublisher({ brokers: [], clientId: "cart-svc" }),
    ).toThrow(/at least one broker/);
  });
});
