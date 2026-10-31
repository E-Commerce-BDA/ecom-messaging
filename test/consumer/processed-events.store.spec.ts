import { describe, expect, it } from "vitest";
import { ProcessedEventsStore } from "../../src/consumer/processed-events.store.js";
import type { Queryable } from "../../src/outbox/types.js";

const EVENT_ID = "018f2f7e-6a4a-7a3b-9f5a-2b1c3d4e5f60";

interface Recorded {
  text: string;
  values?: readonly unknown[];
}

class FakeClient implements Queryable {
  readonly calls: Recorded[] = [];
  rows: readonly unknown[] = [];

  async query(
    text: string,
    values?: readonly unknown[],
  ): Promise<{ rows: readonly unknown[] }> {
    this.calls.push({ text, values });
    return { rows: this.rows };
  }
}

describe("ProcessedEventsStore.claim", () => {
  it("inserts with an ON CONFLICT DO NOTHING claim and returns true when a row came back", async () => {
    const client = new FakeClient();
    client.rows = [{ event_id: EVENT_ID }];
    const store = new ProcessedEventsStore({ schema: "cart" });

    const claimed = await store.claim(client, EVENT_ID, "order.created");

    expect(claimed).toBe(true);
    expect(client.calls[0]?.values).toEqual([EVENT_ID, "order.created"]);
    expect(client.calls[0]?.text).toContain('"cart"."processed_events"');
    expect(client.calls[0]?.text).toContain("ON CONFLICT (event_id) DO NOTHING");
    expect(client.calls[0]?.text).toContain("RETURNING event_id");
  });

  it("returns false for an event id that was already claimed", async () => {
    const client = new FakeClient();
    client.rows = [];
    const store = new ProcessedEventsStore({ schema: "search" });

    expect(await store.claim(client, EVENT_ID, "product.updated")).toBe(false);
  });

  it("leaves the claim to the caller's transaction, so a rollback un-claims it", async () => {
    const client = new FakeClient();
    client.rows = [{ event_id: EVENT_ID }];
    const store = new ProcessedEventsStore({ schema: "catalog" });

    // The store never opens or commits anything: it issues exactly one
    // statement on the client it was handed.
    await store.claim(client, EVENT_ID, "product.imported");

    expect(client.calls).toHaveLength(1);
    expect(client.calls[0]?.text.startsWith("INSERT INTO")).toBe(true);
  });

  it("quotes the schema exactly like the outbox store does", async () => {
    const client = new FakeClient();
    const store = new ProcessedEventsStore({ schema: "cart" });

    await store.claim(client, EVENT_ID, "order.created");

    expect(client.calls[0]?.text).toContain('INSERT INTO "cart"."processed_events"');
  });

  it("accepts an overridden table name", async () => {
    const client = new FakeClient();
    const store = new ProcessedEventsStore({
      schema: "cart",
      table: "seen_events",
    });

    await store.claim(client, EVENT_ID, "order.created");

    expect(client.calls[0]?.text).toContain('"cart"."seen_events"');
  });

  it("rejects a schema that is not a bare identifier", () => {
    expect(() => new ProcessedEventsStore({ schema: "cart; DROP SCHEMA cart" })).toThrow(
      /invalid processed-events schema/,
    );
  });

  it("rejects an uppercase or dashed table name", () => {
    expect(
      () => new ProcessedEventsStore({ schema: "cart", table: "Processed-Events" }),
    ).toThrow(/invalid processed-events table/);
  });
});

describe("ProcessedEventsStore.isProcessed", () => {
  it("is a read-only peek, not a claim", async () => {
    const client = new FakeClient();
    client.rows = [{ "?column?": 1 }];
    const store = new ProcessedEventsStore({ schema: "cart" });

    const processed = await store.isProcessed(client, EVENT_ID);

    expect(processed).toBe(true);
    expect(client.calls[0]?.text.startsWith("SELECT 1")).toBe(true);
    expect(client.calls[0]?.values).toEqual([EVENT_ID]);
  });

  it("reports false when the event id is unknown", async () => {
    const client = new FakeClient();
    const store = new ProcessedEventsStore({ schema: "cart" });

    expect(await store.isProcessed(client, EVENT_ID)).toBe(false);
  });
});
