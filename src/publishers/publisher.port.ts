import type { EventEnvelope, EventTopic } from "@ecom/contracts";

export interface PublishInput {
  topic: EventTopic;
  /** Kafka partition key: the aggregate id (`userId`, `cartId`, …). */
  key: string;
  envelope: EventEnvelope;
}

export interface PublisherPort {
  /**
   * Publish one event. MUST reject when the broker did not acknowledge it —
   * the relay treats a resolved promise as proof the event left the table.
   */
  publish(input: PublishInput): Promise<void>;

  close(): Promise<void>;
}
