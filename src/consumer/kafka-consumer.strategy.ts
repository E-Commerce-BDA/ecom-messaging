import { ServerKafka } from "@nestjs/microservices";

export interface KafkaConsumerStrategyOptions {
  /** `KAFKA_BROKERS`, split on commas. */
  brokers: string[];
  clientId: string;
  /**
   * The consumer group, which must be exactly the service name:
   * `cart-svc`, `catalog-svc`, `search-svc`. The Nest strategy appends
   * `-server` by default and would silently create a second group.
   */
  groupId: string;
  /** Replay from the beginning of the topic. Off by default, as before. */
  fromBeginning?: boolean;
}

/**
 * The one Kafka consumer strategy every service uses.
 *
 * Three details are load-bearing, and all three were read out of
 * `@nestjs/microservices` 12.3.2 rather than assumed:
 *
 * 1. **`postfixId: ""`.** `ServerKafka`'s constructor does
 *    `groupId = (consumerOptions.groupId ?? DEFAULT_GROUP) + postfixId` with a
 *    default postfix of `-server`. Without this, a service that asks for group
 *    `search-svc` gets `search-svc-server`, which is a different group with its
 *    own offsets from the raw kafkajs consumer it replaced.
 * 2. **`run.autoCommit: false`.** Nest spreads `options.run` into
 *    `consumer.run()`. There is no top-level `autoCommit`; putting it there is
 *    silently ignored and every record is committed before it is handled.
 * 3. **`parser.keepBinary: true` + a stringing deserializer.** The default
 *    parser JSON-parses any value starting with `{` or `[`, which would hide a
 *    malformed body from the DLQ path. Keeping the bytes until the handler
 *    decodes them preserves the `invalid-json` behaviour of the consumers this
 *    replaces, including the `rawValue` in the dead letter.
 *
 * Subscribing is by pattern, which is what makes one consumer cover several
 * topics: `bindEvents` subscribes to `[...messageHandlers.keys()]`, i.e. the
 * `@EventPattern` strings themselves.
 */
export function createKafkaConsumerStrategy(
  options: KafkaConsumerStrategyOptions,
): ServerKafka {
  if (options.brokers.length === 0) {
    throw new Error("kafka consumer strategy requires at least one broker");
  }
  if (!options.groupId) {
    throw new Error("kafka consumer strategy requires a group id");
  }

  return new ServerKafka({
    client: {
      clientId: options.clientId,
      brokers: options.brokers,
    },
    consumer: {
      groupId: options.groupId,
      // A typo in an @EventPattern must fail loudly instead of creating a topic.
      allowAutoTopicCreation: false,
    },
    postfixId: "",
    run: {
      autoCommit: false,
    },
    subscribe: {
      fromBeginning: options.fromBeginning ?? false,
    },
    parser: {
      keepBinary: true,
    },
    deserializer: {
      deserialize: (message: KafkaRawMessage) => ({
        pattern: message.topic,
        data: message.value == null ? "" : message.value.toString(),
      }),
    },
  });
}

/**
 * What `deserialize` receives: the kafkajs message after `KafkaParser` has run,
 * with `topic` and `partition` merged in by `ServerKafka#handleMessage`.
 */
interface KafkaRawMessage {
  topic: string;
  value?: Buffer | string | null;
}
