import { dlqTopicName, isEventTopic } from "@ecom/contracts";
import { Kafka, Partitioners } from "kafkajs";

/**
 * The twin topic of an event topic.
 *
 * The registry wins when the topic is registered there, so the producer that
 * created `order.created.dlq` and the consumer that parks records in it cannot
 * disagree about the name. The `${topic}.dlq` fallback covers a topic consumed
 * but not yet registered, which is how an early consumer of a new event still
 * gets a parkable destination instead of a crash.
 */
export function dlqTopicNameFor(topic: string): string {
  return isEventTopic(topic) ? dlqTopicName(topic) : `${topic}.dlq`;
}

/** Where the record came from, written into the DLQ payload verbatim. */
export interface DlqSource {
  topic: string;
  partition: number;
  offset: string;
}

export interface DlqPublishInput {
  source: DlqSource;
  /** Machine-readable reason. Service-specific values are allowed. */
  reason: string;
  error: string;
  /** Present for `invalid-json`, where there is no parsed envelope to keep. */
  rawValue?: string;
  /** Present when the body parsed but was rejected or failed downstream. */
  originalPayload?: unknown;
  /** Carried through so a replay keeps the original partition key. */
  key?: string | null;
}

/**
 * The slice of a kafkajs producer the publisher needs. Injectable so a test can
 * assert on what was sent without a broker.
 */
export interface KafkaProducerLike {
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  send(record: {
    topic: string;
    messages: { key?: string | null; value: string }[];
  }): Promise<unknown>;
}

export interface KafkaDlqPublisherOptions {
  brokers: string[];
  clientId: string;
  producer?: KafkaProducerLike;
  /** Injectable for deterministic `failedAt` in tests. */
  clock?: () => Date;
}

/**
 * Publishes a dead letter and resolves only once the broker has it.
 *
 * That wait is the whole point: the caller commits the offset *after* this
 * resolves, so a broker that refuses the write leaves the record uncommitted
 * and redeliverable. The connection is lazy and shared — a consumer that never
 * dead-letters a record never opens a second connection.
 */
export class KafkaDlqPublisher {
  private readonly producer: KafkaProducerLike;
  private readonly clock: () => Date;
  private connecting: Promise<void> | null = null;
  private connected = false;

  constructor(options: KafkaDlqPublisherOptions) {
    if (options.brokers.length === 0) {
      throw new Error("dlq publisher requires at least one broker");
    }

    this.clock = options.clock ?? (() => new Date());
    this.producer =
      options.producer ??
      new Kafka({
        clientId: options.clientId,
        brokers: options.brokers,
        retry: { initialRetryTime: 300, retries: 3 },
      }).producer({ createPartitioner: Partitioners.DefaultPartitioner });
  }

  /** Resolves the DLQ topic one publish would use. Exposed for health output. */
  topicFor(topic: string): string {
    return dlqTopicNameFor(topic);
  }

  async publish(input: DlqPublishInput): Promise<string> {
    const topic = this.topicFor(input.source.topic);
    await this.connect();

    await this.producer.send({
      topic,
      messages: [
        {
          key: input.key ?? null,
          value: JSON.stringify({
            source: {
              topic: input.source.topic,
              partition: input.source.partition,
              offset: input.source.offset,
            },
            failedAt: this.clock().toISOString(),
            reason: input.reason,
            error: input.error,
            ...(input.rawValue === undefined ? {} : { rawValue: input.rawValue }),
            ...(input.originalPayload === undefined
              ? {}
              : { originalPayload: input.originalPayload }),
          }),
        },
      ],
    });

    return topic;
  }

  async close(): Promise<void> {
    if (!this.connected) {
      return;
    }
    this.connected = false;
    this.connecting = null;

    try {
      await this.producer.disconnect();
    } catch {
      // A producer that already lost its connection is not an error at shutdown.
    }
  }

  private async connect(): Promise<void> {
    if (this.connected) {
      return;
    }
    if (!this.connecting) {
      this.connecting = this.producer
        .connect()
        .then(() => {
          this.connected = true;
        })
        .catch((error: unknown) => {
          this.connecting = null;
          throw error;
        });
    }

    await this.connecting;
  }
}
