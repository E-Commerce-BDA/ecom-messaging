import type { RelayLogger } from "../../src/logger.js";
import type {
  KafkaConsumerLike,
  KafkaMessageContextLike,
  KafkaMessageLike,
} from "../../src/consumer/types.js";
import type { DlqPublishInput } from "../../src/consumer/dlq.publisher.js";

export class FakeConsumer implements KafkaConsumerLike {
  readonly commits: { topic: string; partition: number; offset: string }[] = [];

  async commitOffsets(
    offsets: { topic: string; partition: number; offset: string }[],
  ): Promise<void> {
    this.commits.push(...offsets);
  }
}

/** The smallest thing that satisfies `KafkaContext`'s used surface. */
export class FakeContext implements KafkaMessageContextLike {
  constructor(
    readonly consumer: FakeConsumer,
    private readonly message: KafkaMessageLike,
    private readonly partition: number,
  ) {}

  getMessage(): KafkaMessageLike {
    return this.message;
  }

  getPartition(): number {
    return this.partition;
  }

  getConsumer(): KafkaConsumerLike {
    return this.consumer;
  }
}

/** Collects dead letters instead of sending them. */
export class RecordingDlqPublisher {
  readonly published: DlqPublishInput[] = [];
  /** Set to make the next publish reject, which stalls the consumer. */
  failure: Error | null = null;

  async publish(input: DlqPublishInput): Promise<string> {
    if (this.failure) {
      const failure = this.failure;
      this.failure = null;
      throw failure;
    }

    this.published.push(input);
    return `${input.source.topic}.dlq`;
  }
}

/** Collects log lines so a test can assert an operator would have seen it. */
export class SilentLogger implements RelayLogger {
  readonly lines: { level: keyof RelayLogger; message: string }[] = [];

  debug(message: string): void {
    this.lines.push({ level: "debug", message });
  }

  warn(message: string): void {
    this.lines.push({ level: "warn", message });
  }

  error(message: string): void {
    this.lines.push({ level: "error", message });
  }
}

/** Never actually waits: the retry ladder is asserted by delay values. */
export class FakeSleeper {
  readonly waited: number[] = [];

  async sleep(ms: number): Promise<void> {
    this.waited.push(ms);
  }
}

export const MESSAGE = {
  offset: "41",
  key: Buffer.from("order-1"),
  value: Buffer.from("{}"),
};
