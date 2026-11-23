import { ServerKafka } from "@nestjs/microservices";

/**
 * kafkajs's own default session timeout is 30 000 ms, and the retry ladder in
 * `KafkaEventHandler` runs 1+2+4+8+16 = 31 s. A member that sleeps through its
 * session deadline is evicted, and the coordinator then refuses both its offset
 * commit (`UNKNOWN_MEMBER_ID`) and the dead-letter route that commit depends on
 * — so the record is redelivered and the loop never ends. 45 s is the smallest
 * round number that leaves the documented ladder real headroom.
 */
export const DEFAULT_SESSION_TIMEOUT_MS = 45_000;

/** kafkajs's default. Pinned here so the session/heartbeat pair is one decision. */
export const DEFAULT_HEARTBEAT_INTERVAL_MS = 3_000;

export interface KafkaConsumerStrategyOptions {
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
  /**
   * Broker session timeout. Defaults to `DEFAULT_SESSION_TIMEOUT_MS`.
   *
   * Anything below the retry ladder's total sleep lets the coordinator evict the
   * member mid-retry, which is a stall that looks like a broker problem. Lower
   * this only together with a smaller `retryBudgetMs` on the handler.
   */
  sessionTimeoutMs?: number;
  /** Broker heartbeat interval. Defaults to `DEFAULT_HEARTBEAT_INTERVAL_MS`. */
  heartbeatIntervalMs?: number;
}

/**
 * The one Kafka consumer strategy every service uses.
 *
 * Four details are load-bearing. The first three were read out of
 * `@nestjs/microservices` 12.3.2 rather than assumed; the fourth is a kafkajs
 * broker contract:
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
 * 4. **`consumer.sessionTimeout` / `heartbeatInterval` are set, not defaulted.**
 *    kafkajs's 30 s session is shorter than this package's own retry ladder, and
 *    a member evicted mid-ladder cannot commit or dead-letter. Naming the pair
 *    here is what turns "the library happened to allow it" into a reviewed
 *    contract. See `DEFAULT_SESSION_TIMEOUT_MS`.
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

  const sessionTimeoutMs = options.sessionTimeoutMs ?? DEFAULT_SESSION_TIMEOUT_MS;
  const heartbeatIntervalMs =
    options.heartbeatIntervalMs ?? DEFAULT_HEARTBEAT_INTERVAL_MS;

  // kafkajs refuses a heartbeat at or above the session timeout, and the broker
  // would drop the member regardless. Fail at boot with the numbers in the
  // message instead of as an eviction at the first retryable record.
  if (heartbeatIntervalMs >= sessionTimeoutMs) {
    throw new Error(
      `kafka heartbeat interval (${heartbeatIntervalMs}ms) must be shorter than the session timeout (${sessionTimeoutMs}ms)`,
    );
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
      sessionTimeout: sessionTimeoutMs,
      heartbeatInterval: heartbeatIntervalMs,
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
