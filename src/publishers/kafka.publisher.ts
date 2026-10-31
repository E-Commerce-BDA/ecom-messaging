import { Kafka, type Producer } from "kafkajs";
import { ConsoleRelayLogger, type RelayLogger } from "../logger.js";
import type { PublishInput, PublisherPort } from "./publisher.port.js";

export interface KafkaPublisherOptions {
  /** Read lazily so `KAFKA_BROKERS` is resolved after config is loaded. */
  brokers: () => string[];
  clientId: string;
  logger?: RelayLogger;
  connectionTimeoutMs?: number;
}

/**
 * Kafka producer for the outbox relay.
 *
 * Two deliberate differences from catalog-svc's in-process `KafkaEventBus`:
 * a failed `send` is rethrown (the relay must retry, not mark `published`), and
 * the producer is idempotent with `maxInFlightRequests: 1` so a producer-level
 * retry cannot duplicate a record. That is not a substitute for consumer-side
 * dedupe: a crash between `send` and `markPublished` still republishes.
 */
export class KafkaEventPublisher implements PublisherPort {
  private readonly logger: RelayLogger;
  private readonly kafka: Kafka;
  private producer: Producer | null = null;
  private connecting: Promise<Producer> | null = null;

  constructor(private readonly options: KafkaPublisherOptions) {
    this.logger = options.logger ?? new ConsoleRelayLogger("outbox.publisher");
    this.kafka = new Kafka({
      clientId: options.clientId,
      brokers: options.brokers(),
      connectionTimeout: options.connectionTimeoutMs ?? 3000,
      retry: { initialRetryTime: 300, retries: 3 },
    });
  }

  private async connect(): Promise<Producer> {
    if (this.producer) {
      return this.producer;
    }

    this.connecting ??= (async () => {
      const producer = this.kafka.producer({
        idempotent: true,
        maxInFlightRequests: 1,
      });
      await producer.connect();
      this.producer = producer;
      this.logger.debug(
        `connected to ${this.options.brokers().join(", ")} as ${this.options.clientId}`,
      );
      return producer;
    })().finally(() => {
      this.connecting = null;
    });

    return this.connecting;
  }

  async publish({ topic, key, envelope }: PublishInput): Promise<void> {
    const producer = await this.connect();

    await producer.send({
      topic,
      acks: -1,
      messages: [
        {
          key,
          value: JSON.stringify(envelope),
          headers: {
            "x-event-id": envelope.id,
            "x-event-type": topic,
          },
        },
      ],
    });
  }

  async close(): Promise<void> {
    const producer = this.producer;
    this.producer = null;

    if (!producer) {
      return;
    }

    try {
      await producer.disconnect();
    } catch (error) {
      this.logger.warn(
        `error disconnecting the producer: ${(error as Error).message}`,
      );
    }
  }
}
