import { ConsoleRelayLogger, type RelayLogger } from "../logger.js";
import { ConsumerMetrics, recordHandlerOutcome } from "./consumer-metrics.js";
import type { DlqPublishInput } from "./dlq.publisher.js";
import type {
  KafkaHandlerFn,
  KafkaMessageContextLike,
  KafkaMessageLike,
} from "./types.js";

const DEFAULT_RETRY_DELAYS_MS = [1000, 2000, 4000, 8000, 16000];

/**
 * Thrown when the consumer must stop rather than advance.
 *
 * Exactly one cause exists: the dead-letter write failed. Committing past a
 * record that is in neither the topic nor the DLQ loses it silently, which is
 * the only failure mode bad enough to justify stopping. The service's health
 * endpoint has to surface this — a consumer that stopped and looks healthy is
 * worse than one that stopped.
 */
export class KafkaConsumerStalledError extends Error {
  constructor(
    message: string,
    readonly source: { topic: string; partition: number; offset: string },
    options?: { cause?: unknown },
  ) {
    super(message, options);
    this.name = "KafkaConsumerStalledError";
  }
}

export interface KafkaEventHandlerOptions {
  dlq: DlqPublisherLike;
  metrics?: ConsumerMetrics;
  logger?: RelayLogger;
  retryDelaysMs?: number[];
  maxRetries?: number;
  /**
   * Upper bound on the **total sleep** of the retry ladder, in ms.
   *
   * A Kafka member that is still sleeping when its broker session expires is
   * evicted, and the coordinator then refuses both its offset commit
   * (`UNKNOWN_MEMBER_ID`) and the dead-letter write that commit waits on — so
   * the record is redelivered and retried forever. The ladder has to fit inside
   * `sessionTimeout` (see `DEFAULT_SESSION_TIMEOUT_MS` on the strategy) with
   * room for the handler attempts themselves.
   *
   * When set, the ladder is truncated at construction to the longest leading run
   * of delays whose sum fits the budget, and `maxRetries` follows it. When
   * omitted the ladder runs exactly as configured.
   */
  retryBudgetMs?: number;
  // ADR-events: translateError for terminal failures (see Documentation/ARCHITECTURE_DECISIONS.md)
  translateError?: (error: unknown) => string | null;
  /** Injectable so a test can run the whole ladder without waiting 31s. */
  sleep?: (ms: number) => Promise<void>;
}

/**
 * The one method the protocol needs from a dead-letter sink.
 *
 * Declared structurally rather than as `KafkaDlqPublisher` so a service can
 * substitute its own publisher — and so a test needs no broker, no producer and
 * no cast.
 */
export interface DlqPublisherLike {
  publish(input: DlqPublishInput): Promise<string>;
}

/**
 * The consumer protocol, in one place, for every service.
 *
 * The invariant is the same one the hand-written consumers each reimplemented:
 *
 *   side effect → counter → offset
 *
 * A record whose DLQ write fails therefore keeps its offset, and the partition
 * redelivers it. `run()` is written to never throw except through
 * `KafkaConsumerStalledError`, because a thrown error inside `eachMessage`
 * stalls the partition just as effectively but without anyone knowing why.
 */
export class KafkaEventHandler {
  private readonly dlq: DlqPublisherLike;
  private readonly metrics: ConsumerMetrics;
  private readonly logger: RelayLogger;
  private readonly retryDelaysMs: number[];
  private readonly maxRetries: number;
  private readonly translateError: (error: unknown) => string | null;
  private readonly sleep: (ms: number) => Promise<void>;

  constructor(options: KafkaEventHandlerOptions) {
    this.dlq = options.dlq;
    this.metrics = options.metrics ?? new ConsumerMetrics();
    this.logger = options.logger ?? new ConsoleRelayLogger("consumer");

    const requested = options.retryDelaysMs ?? DEFAULT_RETRY_DELAYS_MS;
    const ladder = boundRetryLadder(requested, options.retryBudgetMs);
    this.retryDelaysMs = ladder.delays;

    // Without a budget, `maxRetries` keeps its own meaning and may exceed the
    // delay list (the loop then repeats the last delay). With a budget, more
    // attempts than sleeps would overshoot the very bound it exists to enforce.
    if (options.maxRetries === undefined) {
      this.maxRetries = this.retryDelaysMs.length;
    } else if (options.retryBudgetMs === undefined) {
      this.maxRetries = options.maxRetries;
    } else {
      this.maxRetries = Math.min(options.maxRetries, this.retryDelaysMs.length);
    }

    // Said out loud once at boot: a ladder that is smaller than the one the
    // service asked for must not be a silent behaviour change.
    if (ladder.budgetedTo !== undefined) {
      this.logger.warn(
        `Retry ladder bounded to ${ladder.budgetedTo}ms of sleeps: ` +
          `${requested.length} configured delays reduced to ` +
          `${this.retryDelaysMs.join("/")} (${this.maxRetries} retries).`,
      );
    }

    this.translateError = options.translateError ?? (() => null);
    this.sleep = options.sleep ?? sleep;
  }

  get counters(): ConsumerMetrics {
    return this.metrics;
  }

  // ADR-events: translateError for terminal failures (see Documentation/ARCHITECTURE_DECISIONS.md)
  async run(
    ctx: KafkaMessageContextLike,
    topic: string,
    raw: unknown,
    handle: KafkaHandlerFn,
  ): Promise<void> {
    const message = ctx.getMessage();
    const partition = ctx.getPartition();
    const { offset } = message;

    // Kafka commits "the next offset to read", not "the offset just read".
    const nextOffset = (BigInt(offset) + 1n).toString();
    const commit = async (): Promise<void> => {
      await ctx.getConsumer().commitOffsets([{ topic, partition, offset: nextOffset }]);
      this.metrics.recordCommit(topic, partition, offset);
    };

    const envelope = this.parse(topic, raw);

    if (!envelope.ok) {
      await this.deadLetter(topic, partition, message, {
        reason: "invalid-json",
        error: envelope.error,
        rawValue: envelope.raw,
      });
      this.metrics.record("invalid");
      await commit();
      return;
    }

    for (let attempt = 0; ; attempt += 1) {
      try {
        const outcome = await handle(envelope.value);

        // The DLQ write happens *before* the counter and *before* the commit:
        // if the broker refuses the dead letter, nothing claims the record was
        // handled and the partition redelivers it.
        if (outcome.kind === "invalid") {
          await this.deadLetter(topic, partition, message, {
            reason:
              outcome.stage === "envelope"
                ? "invalid-envelope"
                : "invalid-payload",
            error: outcome.error,
            originalPayload: envelope.value,
          });
        }

        recordHandlerOutcome(this.metrics, outcome);
        this.logOutcome(topic, offset, outcome);
        await commit();
        return;
      } catch (error) {
        // A stalled consumer must not be retried into a second attempt: the
        // dead letter already failed once, and re-running the handler would
        // apply a side effect whose record nobody can park.
        if (error instanceof KafkaConsumerStalledError) {
          throw error;
        }

        const terminal = this.translateError(error);
        const detail = describe(error);

        // A terminal domain failure cannot be repaired by redelivering it, so
        // retrying would only delay the dead letter by the whole ladder.
        if (terminal) {
          this.logger.error(
            `Terminal failure on ${topic} offset ${offset} (${terminal}): ${detail}`,
          );
          await this.deadLetter(topic, partition, message, {
            reason: terminal,
            error: detail,
            originalPayload: envelope.value,
          });
          await commit();
          return;
        }

        if (attempt >= this.maxRetries) {
          this.logger.error(
            `Exhausted retries on ${topic} offset ${offset}. Routing to the DLQ`,
            error,
          );
          await this.deadLetter(topic, partition, message, {
            reason: "transient-exhausted",
            error: detail,
            originalPayload: envelope.value,
          });
          // Committed only after the DLQ acknowledged.
          await commit();
          return;
        }

        const delay =
          this.retryDelaysMs[attempt] ??
          this.retryDelaysMs[this.retryDelaysMs.length - 1] ??
          1000;
        this.logger.warn(
          `Failed processing ${topic} offset ${offset} (attempt ${attempt + 1}/${this.maxRetries}): ${detail}. Retrying in ${delay}ms`,
        );
        await this.sleep(delay);
      }
    }
  }

  /**
   * Decodes the raw value.
   *
   * Accepts a string (how the Nest strategy is configured to deliver it) and an
   * already-parsed value (how a raw `eachMessage` loop delivers it), so the
   * protocol works from both entry points and a malformed body keeps its bytes
   * for the DLQ.
   */
  private parse(
    topic: string,
    raw: unknown,
  ): { ok: true; value: unknown } | { ok: false; error: string; raw: string } {
    if (typeof raw === "object" && raw !== null) {
      return { ok: true, value: raw };
    }

    const text = typeof raw === "string" ? raw : raw == null ? "" : String(raw);

    try {
      return { ok: true, value: JSON.parse(text) };
    } catch (error) {
      const reason = describe(error);
      this.logger.warn(`Malformed JSON on ${topic}: ${reason}. Routing to the DLQ`);
      return { ok: false, error: reason, raw: text };
    }
  }

  private async deadLetter(
    topic: string,
    partition: number,
    message: KafkaMessageLike,
    record: {
      reason: string;
      error: string;
      rawValue?: string;
      originalPayload?: unknown;
    },
  ): Promise<void> {
    try {
      await this.dlq.publish({
        source: { topic, partition, offset: message.offset },
        key: message.key == null ? null : message.key.toString(),
        ...record,
      });
    } catch (error) {
      this.metrics.record("stalled");
      throw new KafkaConsumerStalledError(
        `failed to dead-letter ${topic} offset ${message.offset}: ${describe(error)}`,
        { topic, partition, offset: message.offset },
        { cause: error },
      );
    }

    // Counted only here: `dlq` means a dead letter that actually reached the
    // broker, not one that was attempted.
    this.metrics.record("dlq");
  }

  private logOutcome(
    topic: string,
    offset: string,
    outcome: Awaited<ReturnType<KafkaHandlerFn>>,
  ): void {
    switch (outcome.kind) {
      case "applied":
        return;
      case "duplicate":
        this.logger.debug(
          `Event ${outcome.eventId ?? "unknown"} on ${topic} offset ${offset} was already applied; skipped`,
        );
        return;
      case "ignored":
        this.logger.debug(
          `Ignored ${outcome.type ?? "event"} on ${topic} offset ${offset} (${outcome.reason})`,
        );
        return;
      case "invalid":
        // Already routed to the DLQ by the caller.
        return;
    }
  }
}

function describe(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

/**
 * Truncate `delays` to the longest leading run whose sum fits `budgetMs`.
 *
 * Returns the ladder together with the budget it was fitted to — `undefined`
 * when no budget was given — so the caller can report a shortened ladder instead
 * of quietly running one.
 */
function boundRetryLadder(
  delays: number[],
  budgetMs?: number,
): { delays: number[]; budgetedTo?: number } {
  if (budgetMs === undefined) {
    return { delays };
  }

  const kept: number[] = [];
  let total = 0;
  for (const delay of delays) {
    if (total + delay > budgetMs) {
      break;
    }
    total += delay;
    kept.push(delay);
  }

  // Zero sleeps means zero retries: a transient error would be dead-lettered on
  // the first attempt, which is the wrong reading of a budget that is merely
  // tight. Keep the first delay and let the budget be a stated trade-off.
  return {
    delays: kept.length > 0 ? kept : delays.slice(0, 1),
    budgetedTo: budgetMs,
  };
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}
