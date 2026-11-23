import { describe, expect, it } from "vitest";
import { ServerKafka } from "@nestjs/microservices";
import {
  createKafkaConsumerStrategy,
  DEFAULT_HEARTBEAT_INTERVAL_MS,
  DEFAULT_SESSION_TIMEOUT_MS,
} from "../../src/consumer/kafka-consumer.strategy.js";

function internals(strategy: ServerKafka) {
  return strategy as unknown as {
    groupId: string;
    clientId: string;
    options: {
      run?: { autoCommit?: boolean };
      parser?: { keepBinary?: boolean };
      subscribe?: { fromBeginning?: boolean };
      consumer?: {
        allowAutoTopicCreation?: boolean;
        sessionTimeout?: number;
        heartbeatInterval?: number;
      };
      deserializer?: {
        deserialize: (message: {
          topic: string;
          value?: Buffer | null;
        }) => { pattern: string; data: string };
      };
    };
  };
}

function newStrategy(
  overrides: Partial<{
    groupId: string;
    fromBeginning: boolean;
    sessionTimeoutMs: number;
    heartbeatIntervalMs: number;
  }> = {},
) {
  return createKafkaConsumerStrategy({
    brokers: ["localhost:9092"],
    clientId: "search-svc",
    groupId: overrides.groupId ?? "search-svc",
    fromBeginning: overrides.fromBeginning,
    sessionTimeoutMs: overrides.sessionTimeoutMs,
    heartbeatIntervalMs: overrides.heartbeatIntervalMs,
  });
}

describe("createKafkaConsumerStrategy", () => {
  it("builds a Nest Kafka server strategy", () => {
    expect(newStrategy()).toBeInstanceOf(ServerKafka);
  });

  it("keeps the group id exactly as given, with no -server suffix", () => {
    expect(internals(newStrategy()).groupId).toBe("search-svc");
  });

  it("keeps the client id exactly as given, with no -server suffix", () => {
    expect(internals(newStrategy()).clientId).toBe("search-svc");
  });

  it("disables auto-commit so the offset follows the side effect", () => {
    expect(internals(newStrategy()).options.run?.autoCommit).toBe(false);
  });

  it("keeps message bytes so a malformed body still reaches the DLQ", () => {
    expect(internals(newStrategy()).options.parser?.keepBinary).toBe(true);
  });

  it("decodes the value to a string and routes by topic", () => {
    const deserialize = internals(newStrategy()).options.deserializer?.deserialize;

    expect(
      deserialize?.({ topic: "order.created", value: Buffer.from('{"a":1}') }),
    ).toEqual({ pattern: "order.created", data: '{"a":1}' });
  });

  it("delivers an empty string rather than null for a tombstone", () => {
    const deserialize = internals(newStrategy()).options.deserializer?.deserialize;

    expect(deserialize?.({ topic: "order.created", value: null })).toEqual({
      pattern: "order.created",
      data: "",
    });
  });

  it("does not replay from the beginning by default", () => {
    expect(internals(newStrategy()).options.subscribe?.fromBeginning).toBe(false);
  });

  it("replays from the beginning when asked", () => {
    expect(
      internals(newStrategy({ fromBeginning: true })).options.subscribe
        ?.fromBeginning,
    ).toBe(true);
  });

  it("refuses to let a typo create a topic", () => {
    expect(internals(newStrategy()).options.consumer?.allowAutoTopicCreation).toBe(
      false,
    );
  });

  // kafkajs's own session default (30s) is shorter than this package's retry
  // ladder (31s of sleeps). Left implicit, the coordinator evicts the member
  // mid-retry and both the dead letter and the offset commit fail with
  // UNKNOWN_MEMBER_ID — a poisoned record then never stops being redelivered.
  it("sets a session timeout with headroom over the retry ladder", () => {
    const consumer = internals(newStrategy()).options.consumer;

    expect(consumer?.sessionTimeout).toBe(DEFAULT_SESSION_TIMEOUT_MS);
    expect(consumer?.sessionTimeout).toBeGreaterThan(31_000);
  });

  it("sets the heartbeat interval below the session timeout", () => {
    const consumer = internals(newStrategy()).options.consumer;

    expect(consumer?.heartbeatInterval).toBe(DEFAULT_HEARTBEAT_INTERVAL_MS);
    expect(consumer!.heartbeatInterval!).toBeLessThan(consumer!.sessionTimeout!);
  });

  it("honours an explicit session timeout override", () => {
    const strategy = newStrategy({ sessionTimeoutMs: 60_000 });

    expect(internals(strategy).options.consumer?.sessionTimeout).toBe(60_000);
  });

  it("refuses a heartbeat that cannot outlive its own session", () => {
    expect(() =>
      newStrategy({ sessionTimeoutMs: 5_000, heartbeatIntervalMs: 5_000 }),
    ).toThrow(/shorter than the session timeout/);
  });

  it("refuses an empty broker list", () => {
    expect(() =>
      createKafkaConsumerStrategy({ brokers: [], clientId: "x", groupId: "x" }),
    ).toThrow(/at least one broker/);
  });

  it("refuses an empty group id rather than letting Nest default it", () => {
    expect(() => newStrategy({ groupId: "" })).toThrow(/group id/);
  });
});
