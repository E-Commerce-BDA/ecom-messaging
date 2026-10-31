import { describe, expect, it } from "vitest";
import {
  eventEnvelopeSchema,
  readEventEnvelope,
  type CartUpdatedPayload,
} from "@ecom/contracts";
import { buildEventEnvelope } from "../src/envelope.js";

const EVENT_ID = "018f2f7e-6a4a-7a3b-9f5a-2b1c3d4e5f60";
const CART_ID = "018f2f7e-1111-7a3b-9f5a-2a2b3c4d5e61";
const USER_ID = "018f2f7e-2222-7a3b-9f5a-2a2b3c4d5e62";
const ITEM_ID = "018f2f7e-3333-7a3b-9f5a-2a2b3c4d5e63";
const PRODUCT_ID = "018f2f7e-4444-7a3b-9f5a-2a2b3c4d5e64";
const TIMESTAMP = new Date("2026-10-30T10:00:00.000Z");

function cartPayload(
  overrides: Partial<CartUpdatedPayload> = {},
): CartUpdatedPayload {
  return {
    cartId: CART_ID,
    userId: USER_ID,
    sessionId: null,
    status: "active",
    items: [
      {
        id: ITEM_ID,
        productId: PRODUCT_ID,
        variantId: null,
        qty: 2,
        unitPrice: 12.5,
        productName: "Espresso cup",
        productSlug: "espresso-cup",
        imageUrl: null,
        variantLabel: null,
        addedAt: TIMESTAMP.toISOString(),
      },
    ],
    totals: { subtotal: 25, itemCount: 2, lineCount: 1 },
    updatedAt: TIMESTAMP.toISOString(),
    ...overrides,
  };
}

describe("buildEventEnvelope", () => {
  it("dual-writes type, topic and eventType and pins the idempotency key to the id", () => {
    const envelope = buildEventEnvelope({
      id: EVENT_ID,
      topic: "cart.updated",
      producer: "cart-svc",
      timestamp: TIMESTAMP,
      payload: cartPayload(),
    });

    expect(envelope).toMatchObject({
      id: EVENT_ID,
      type: "cart.updated",
      topic: "cart.updated",
      eventType: "cart.updated",
      version: 1,
      producer: "cart-svc",
      idempotencyKey: EVENT_ID,
      timestamp: "2026-10-30T10:00:00.000Z",
    });
    expect(eventEnvelopeSchema.safeParse(envelope).success).toBe(true);
    expect(readEventEnvelope(envelope).type).toBe("cart.updated");
  });

  it("accepts a version override but defaults to 1", () => {
    const envelope = buildEventEnvelope({
      id: EVENT_ID,
      topic: "cart.updated",
      producer: "cart-svc",
      timestamp: TIMESTAMP,
      payload: cartPayload(),
      version: 2,
    });

    expect(envelope.version).toBe(2);
  });

  it("rejects a payload that drifted from the contract", () => {
    expect(() =>
      buildEventEnvelope({
        id: EVENT_ID,
        topic: "cart.updated",
        producer: "cart-svc",
        timestamp: TIMESTAMP,
        payload: cartPayload({
          status: "invalid" as CartUpdatedPayload["status"],
        }),
      }),
    ).toThrow(/status/i);
  });

  it("rejects a quantity that violates the item contract", () => {
    const drifted = cartPayload();
    drifted.items[0]!.qty = 0;

    expect(() =>
      buildEventEnvelope({
        id: EVENT_ID,
        topic: "cart.updated",
        producer: "cart-svc",
        timestamp: TIMESTAMP,
        payload: drifted,
      }),
    ).toThrow(/qty/i);
  });

  it("rejects a product name longer than the contract allows", () => {
    const drifted = cartPayload();
    drifted.items[0]!.productName = "x".repeat(256);

    expect(() =>
      buildEventEnvelope({
        id: EVENT_ID,
        topic: "cart.updated",
        producer: "cart-svc",
        timestamp: TIMESTAMP,
        payload: drifted,
      }),
    ).toThrow(/productName/i);
  });

  it("accepts the user.registered payload without any PII beyond the id", () => {
    const envelope = buildEventEnvelope({
      id: EVENT_ID,
      topic: "user.registered",
      producer: "auth-svc",
      timestamp: TIMESTAMP,
      payload: {
        userId: USER_ID,
        displayName: "Alice Smith",
        registeredAt: TIMESTAMP.toISOString(),
      },
    });

    expect(JSON.stringify(envelope.payload)).not.toMatch(/@/);
  });
});
