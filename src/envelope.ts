import {
  eventEnvelopeSchema,
  parseEventPayload,
  type EventEnvelope,
  type EventPayload,
  type EventTopic,
} from "@ecom/contracts";

export interface EnvelopeInput<T extends EventTopic> {
  id: string;
  topic: T;
  producer: string;
  timestamp: Date;
  payload: EventPayload<T>;
  version?: number;
}

/**
 * Build the wire envelope for one event.
 *
 * Construction and validation happen together on purpose: this runs inside the
 * producer's transaction, so a payload that drifted from its contract aborts
 * the business write instead of leaving an unpublishable row behind.
 *
 * Contracts 2.0.0 accepts only the canonical shape: one event name, a version
 * and an idempotency key. A payload that drifts aborts the producer's write.
 */
export function buildEventEnvelope<T extends EventTopic>(
  input: EnvelopeInput<T>,
): EventEnvelope {
  const envelope = {
    id: input.id,
    type: input.topic,
    version: input.version ?? 1,
    timestamp: input.timestamp.toISOString(),
    producer: input.producer,
    idempotencyKey: input.id,
    payload: input.payload as unknown as Record<string, unknown>,
  };

  eventEnvelopeSchema.parse(envelope);
  parseEventPayload(input.topic, input.payload);

  return envelope;
}
