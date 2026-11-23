import { Kafka } from "kafkajs";
import { ConsoleRelayLogger, type RelayLogger } from "../logger.js";

export interface TopicOffsetLike {
  partition: number;
  /** High watermark, i.e. the next offset a producer would write. */
  offset: string;
}

export interface GroupOffsetLike {
  topic: string;
  partitions: { partition: number; offset: string }[];
}

/**
 * The slice of a kafkajs admin client the sampler uses.
 *
 * Injectable, so the lag arithmetic can be tested without a broker — which is
 * the reason this class talks to an admin client instead of reaching into the
 * Nest strategy's consumer.
 */
export interface KafkaAdminLike {
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  fetchTopicOffsets(topic: string): Promise<TopicOffsetLike[]>;
  fetchOffsets(options: {
    groupId: string;
    topics?: string[];
  }): Promise<GroupOffsetLike[]>;
}

export interface LagSample {
  lag: Record<string, number>;
  totalLag: number;
  sampledAt: string;
}

export interface KafkaLagSamplerOptions {
  brokers: string[];
  clientId: string;
  /** The consumer group being measured. Exactly the service name. */
  groupId: string;
  topics: readonly string[];
  /** Defaults to 15s, matching the scrape interval the runbook documents. */
  intervalMs?: number;
  admin?: KafkaAdminLike;
  logger?: RelayLogger;
  clock?: () => Date;
}

const DEFAULT_INTERVAL_MS = 15_000;

/**
 * Measures consumer lag without touching the consumer.
 *
 * A separate admin connection is deliberate: the Nest strategy owns its
 * consumer, and a metric that has to ask a running consumer for its position
 * turns a monitoring outage into a processing outage. Here a broken sample is
 * only a broken sample.
 *
 * Lag is `high watermark − committed offset`. A committed offset of `-1` means
 * the group has *never committed that partition*, which is not "infinitely
 * behind" — it is "not ours yet", usually a freshly created partition — so it
 * contributes no lag instead of poisoning the total.
 */
export class KafkaLagSampler {
  private readonly admin: KafkaAdminLike;
  private readonly logger: RelayLogger;
  private readonly clock: () => Date;
  private readonly intervalMs: number;
  private readonly groupId: string;
  private readonly topics: readonly string[];
  private timer: NodeJS.Timeout | null = null;
  private latestSample: LagSample | null = null;
  private connecting: Promise<void> | null = null;
  private connected = false;

  constructor(options: KafkaLagSamplerOptions) {
    if (options.topics.length === 0) {
      throw new Error("lag sampler requires at least one topic");
    }

    this.groupId = options.groupId;
    this.topics = options.topics;
    this.intervalMs = options.intervalMs ?? DEFAULT_INTERVAL_MS;
    this.logger = options.logger ?? new ConsoleRelayLogger("lag");
    this.clock = options.clock ?? (() => new Date());
    this.admin =
      options.admin ??
      new Kafka({
        clientId: options.clientId,
        brokers: options.brokers,
        retry: { initialRetryTime: 300, retries: 3 },
      }).admin();
  }

  async sample(): Promise<LagSample> {
    const lag: Record<string, number> = {};

    for (const topic of this.topics) {
      lag[topic] = await this.topicLag(topic);
    }

    const totalLag = Object.values(lag).reduce((sum, value) => sum + value, 0);
    const sample: LagSample = {
      lag,
      totalLag,
      sampledAt: this.clock().toISOString(),
    };
    this.latestSample = sample;

    return sample;
  }

  /**
   * Samples every `intervalMs` in the background.
   *
   * The timer is unref'd: a lag gauge must never be the reason a process stays
   * alive, and it must never hold up a shutdown.
   */
  start(intervalMs: number = this.intervalMs): void {
    if (this.timer) {
      return;
    }

    // The first connect is backgrounded, so its failure has to be handled
    // here: an unhandled rejection would take the process down at boot over a
    // metric, which is exactly what a service that can still serve reads from
    // its own database must not do.
    void this.ensureConnected().catch((error: unknown) => {
      this.connecting = null;
      this.logger.warn(`lag sampler could not connect: ${describe(error)}`);
    });

    this.timer = setInterval(() => {
      void this.sampleOnce();
    }, intervalMs);
    this.timer.unref();
  }

  latest(): LagSample | null {
    return this.latestSample;
  }

  async stop(): Promise<void> {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }

    try {
      await this.admin.disconnect();
    } catch {
      // An admin client that never connected has nothing to release.
    }
  }

  private async sampleOnce(): Promise<void> {
    try {
      await this.ensureConnected();
      await this.sample();
    } catch (error) {
      // Lag is observability: a failed scrape is logged, never rethrown into
      // the timer, because an unhandled rejection here would take the process
      // down for a metric.
      this.logger.warn(`lag sample failed: ${describe(error)}`);
    }
  }

  private async ensureConnected(): Promise<void> {
    if (this.connected) {
      return;
    }
    if (!this.connecting) {
      this.connecting = this.admin
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

  private async topicLag(topic: string): Promise<number> {
    const [endOffsets, committed] = await Promise.all([
      this.admin.fetchTopicOffsets(topic),
      this.admin.fetchOffsets({ groupId: this.groupId, topics: [topic] }),
    ]);

    const committedByPartition = new Map<string, string>();
    for (const entry of committed) {
      if (entry.topic !== topic) {
        continue;
      }
      for (const partition of entry.partitions) {
        committedByPartition.set(String(partition.partition), partition.offset);
      }
    }

    let lag = 0;
    for (const end of endOffsets) {
      const at = committedByPartition.get(String(end.partition));
      // '-1' is kafkajs' "no committed offset for this group", not a position.
      if (at === undefined || at === "-1") {
        continue;
      }

      const behind = Number(end.offset) - Number(at);
      if (Number.isFinite(behind) && behind > 0) {
        lag += behind;
      }
    }

    return lag;
  }
}

function describe(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
