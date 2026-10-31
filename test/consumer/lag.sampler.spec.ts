import { describe, expect, it, vi } from "vitest";
import {
  KafkaLagSampler,
  type KafkaAdminLike,
} from "../../src/consumer/lag.sampler.js";
import { SilentLogger } from "./fakes.js";

class FakeAdmin implements KafkaAdminLike {
  /** topic → partition → high watermark */
  readonly ends: Record<string, { partition: number; offset: string }[]> = {};
  /** topic → partition → committed offset, as kafkajs reports it (`-1` = none). */
  readonly committed: Record<string, { partition: number; offset: string }[]> = {};
  connects = 0;
  disconnects = 0;
  failure: Error | null = null;
  connectFailure: Error | null = null;

  async connect(): Promise<void> {
    this.connects += 1;
    if (this.connectFailure) {
      throw this.connectFailure;
    }
  }

  async disconnect(): Promise<void> {
    this.disconnects += 1;
  }

  async fetchTopicOffsets(topic: string) {
    if (this.failure) {
      throw this.failure;
    }
    return this.ends[topic] ?? [];
  }

  async fetchOffsets(options: { groupId: string; topics?: string[] }) {
    if (this.failure) {
      throw this.failure;
    }
    return (options.topics ?? []).map((topic) => ({
      topic,
      partitions: this.committed[topic] ?? [],
    }));
  }
}

function newSampler(topics: readonly string[], admin = new FakeAdmin()) {
  const sampler = new KafkaLagSampler({
    brokers: ["localhost:9092"],
    clientId: "search-svc",
    groupId: "search-svc",
    topics,
    admin,
    logger: new SilentLogger(),
    clock: () => new Date("2026-10-31T12:00:00.000Z"),
  });

  return { sampler, admin };
}

describe("KafkaLagSampler", () => {
  it("subtracts the committed offset from the high watermark", async () => {
    const { sampler, admin } = newSampler(["order.created"]);
    admin.ends["order.created"] = [{ partition: 0, offset: "120" }];
    admin.committed["order.created"] = [{ partition: 0, offset: "100" }];

    const sample = await sampler.sample();

    expect(sample.lag).toEqual({ "order.created": 20 });
    expect(sample.totalLag).toBe(20);
    expect(sample.sampledAt).toBe("2026-10-31T12:00:00.000Z");
  });

  it("sums several partitions of one topic", async () => {
    const { sampler, admin } = newSampler(["order.created"]);
    admin.ends["order.created"] = [
      { partition: 0, offset: "50" },
      { partition: 1, offset: "70" },
      { partition: 2, offset: "10" },
    ];
    admin.committed["order.created"] = [
      { partition: 0, offset: "40" },
      { partition: 1, offset: "70" },
      { partition: 2, offset: "0" },
    ];

    const sample = await sampler.sample();

    expect(sample.lag["order.created"]).toBe(20);
  });

  it("treats a -1 committed offset as never committed, not as infinite lag", async () => {
    const { sampler, admin } = newSampler(["product.imported"]);
    admin.ends["product.imported"] = [
      { partition: 0, offset: "500" },
      { partition: 1, offset: "8" },
    ];
    admin.committed["product.imported"] = [
      { partition: 0, offset: "-1" },
      { partition: 1, offset: "8" },
    ];

    const sample = await sampler.sample();

    expect(sample.lag["product.imported"]).toBe(0);
    expect(sample.totalLag).toBe(0);
  });

  it("treats a partition with no committed entry the same way", async () => {
    const { sampler, admin } = newSampler(["blog.updated"]);
    admin.ends["blog.updated"] = [
      { partition: 0, offset: "40" },
      { partition: 1, offset: "90" },
    ];
    admin.committed["blog.updated"] = [{ partition: 0, offset: "30" }];

    const sample = await sampler.sample();

    expect(sample.lag["blog.updated"]).toBe(10);
  });

  it("never reports negative lag when the group is ahead of the watermark", async () => {
    const { sampler, admin } = newSampler(["category.updated"]);
    admin.ends["category.updated"] = [{ partition: 0, offset: "10" }];
    admin.committed["category.updated"] = [{ partition: 0, offset: "12" }];

    const sample = await sampler.sample();

    expect(sample.lag["category.updated"]).toBe(0);
  });

  it("reports every topic it was given, even at zero", async () => {
    const topics = ["product.updated", "category.updated"] as const;
    const { sampler, admin } = newSampler(topics);
    admin.ends["product.updated"] = [{ partition: 0, offset: "5" }];
    admin.committed["product.updated"] = [{ partition: 0, offset: "5" }];

    const sample = await sampler.sample();

    expect(Object.keys(sample.lag).sort()).toEqual([...topics].sort());
    expect(sample.totalLag).toBe(0);
  });

  it("totals across topics", async () => {
    const { sampler, admin } = newSampler(["a.b", "c.d"]);
    admin.ends["a.b"] = [{ partition: 0, offset: "7" }];
    admin.committed["a.b"] = [{ partition: 0, offset: "5" }];
    admin.ends["c.d"] = [{ partition: 0, offset: "100" }];
    admin.committed["c.d"] = [{ partition: 0, offset: "94" }];

    const sample = await sampler.sample();

    expect(sample.totalLag).toBe(8);
  });

  it("keeps the last successful sample readable", async () => {
    const { sampler, admin } = newSampler(["order.created"]);
    admin.ends["order.created"] = [{ partition: 0, offset: "3" }];
    admin.committed["order.created"] = [{ partition: 0, offset: "1" }];

    expect(sampler.latest()).toBeNull();
    await sampler.sample();

    expect(sampler.latest()?.totalLag).toBe(2);
  });

  it("propagates an admin failure from an explicit sample", async () => {
    const { sampler, admin } = newSampler(["order.created"]);
    admin.failure = new Error("broker down");

    await expect(sampler.sample()).rejects.toThrow("broker down");
  });

  it("stops the timer and disconnects", async () => {
    const { sampler, admin } = newSampler(["order.created"]);

    sampler.start(60_000);
    await sampler.stop();

    expect(admin.disconnects).toBe(1);
  });

  it("survives a broker that is unreachable at start", async () => {
    const { sampler, admin } = newSampler(["order.created"]);
    admin.connectFailure = new Error("ECONNREFUSED");

    // The connect is backgrounded, so `start` must not throw and the rejected
    // promise must never surface as an unhandled rejection.
    const unhandled = vi.fn();
    process.once("unhandledRejection", unhandled);

    sampler.start(60_000);
    await new Promise((resolve) => setTimeout(resolve, 10));
    await sampler.stop();
    process.removeListener("unhandledRejection", unhandled);

    expect(unhandled).not.toHaveBeenCalled();
    expect(sampler.latest()).toBeNull();
    // The next sample retries the connection rather than staying poisoned.
    expect(admin.connects).toBeGreaterThanOrEqual(1);
  });

  it("requires at least one topic", () => {
    expect(
      () =>
        new KafkaLagSampler({
          brokers: ["localhost:9092"],
          clientId: "cart-svc",
          groupId: "cart-svc",
          topics: [],
        }),
    ).toThrow(/at least one topic/);
  });
});
