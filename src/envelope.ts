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
 * `topic` and `eventType` are written alongside `type` because contracts 1.x
 * still carry the pre-1.3.0 field names; `eventEnvelopeSchema` rejects the
 * envelope if the three ever disagree. They disappear in 2.0.0.
 */
export function buildEventEnvelope<T extends EventTopic>(
  input: EnvelopeInput<T>,
): EventEnvelope {
  const envelope = {
    id: input.id,
    type: input.topic,
    topic: input.topic,
    eventType: input.topic,
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
