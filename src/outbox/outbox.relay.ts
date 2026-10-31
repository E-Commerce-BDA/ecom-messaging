import { isEventTopic } from "@ecom/contracts";
import { ConsoleRelayLogger, type RelayLogger } from "../logger.js";
import type { PublisherPort } from "../publishers/publisher.port.js";
import type { OutboxStorePort } from "./types.js";

export interface OutboxRelayOptions {
  /**
   * `never` on purpose: the relay claims, marks and publishes, but it never
   * writes an event. Typing the write context as `never` lets one relay drive
   * the Postgres store and the Mongo store without knowing which one it has.
   */
  store: OutboxStorePort<never>;
  publisher: PublisherPort;
  logger?: RelayLogger;
  /** Rows claimed per cycle. Default 20. */
  batchSize?: number;
  /** Idle poll interval. Default 2000 ms. */
  intervalMs?: number;
  /** How long a claimed row stays invisible to other workers. Default 30 s. */
  leaseSeconds?: number;
  /** Attempts before a row is parked as `failed`. Default 5 (SQS parity). */
  maxAttempts?: number;
  /** Delay before the next attempt; default `min(2 ** attempts, 60)` seconds. */
  backoffSeconds?: (attempts: number) => number;
}

export interface DrainResult {
  published: number;
  failed: number;
  /** True when another worker holds the relay lock and this cycle did nothing. */
  skipped: boolean;
}

const DEFAULTS = {
  batchSize: 20,
  intervalMs: 2000,
  leaseSeconds: 30,
  maxAttempts: 5,
};

/**
 * The only publisher of outbox rows.
 *
 * Delivery is at-least-once: a crash between `publish` and `markPublished`
 * republishes the same envelope id, which consumers dedupe. Ordering is
 * preserved by publishing a claimed batch sequentially *and* by making sure
 * only one relay instance is draining at a time.
 */
export class OutboxRelay {
  private readonly store: OutboxStorePort<never>;
  private readonly publisher: PublisherPort;
  private readonly logger: RelayLogger;
  private readonly batchSize: number;
  private readonly intervalMs: number;
  private readonly leaseSeconds: number;
  private readonly maxAttempts: number;
  private readonly backoff: (attempts: number) => number;

  private timer: NodeJS.Timeout | null = null;
  private inFlight = false;

  constructor(options: OutboxRelayOptions) {
    this.store = options.store;
    this.publisher = options.publisher;
    this.logger = options.logger ?? new ConsoleRelayLogger("outbox.relay");
    this.batchSize = options.batchSize ?? DEFAULTS.batchSize;
    this.intervalMs = options.intervalMs ?? DEFAULTS.intervalMs;
    this.leaseSeconds = options.leaseSeconds ?? DEFAULTS.leaseSeconds;
    this.maxAttempts = options.maxAttempts ?? DEFAULTS.maxAttempts;
    this.backoff =
      options.backoffSeconds ??
      ((attempts) => Math.min(2 ** attempts, 60));
  }

  start(): void {
    if (this.timer) {
      return;
    }

    this.timer = setInterval(() => {
      void this.tick();
    }, this.intervalMs);
    // Never keep the process alive just for the poll timer.
    this.timer.unref?.();

    this.logger.debug(`relay started (every ${this.intervalMs} ms)`);
  }

  stop(): void {
    if (!this.timer) {
      return;
    }

    clearInterval(this.timer);
    this.timer = null;
    this.logger.debug("relay stopped");
  }

  /** One drain cycle. Public so tests drive the relay without a timer. */
  async drainOnce(): Promise<DrainResult> {
    // The relay lease must outlive a whole drain: a lease that expires mid-batch
    // lets a second worker start, which is the one way ordering can break.
    // Postgres takes a real advisory lock and ignores the hint.
    const release = await this.store.tryAcquireRelayLock(this.leaseSeconds * 4);
    if (!release) {
      return { published: 0, failed: 0, skipped: true };
    }

    let published = 0;
    let failed = 0;

    try {
      const batch = await this.store.claimBatch(
        this.batchSize,
        this.leaseSeconds,
      );

      // Sequential on purpose: ordering is the reason the key exists.
      for (const row of batch) {
        if (!isEventTopic(row.event_type)) {
          failed += 1;
          await this.park(row.id, `unknown event type '${row.event_type}'`);
          continue;
        }

        try {
          await this.publisher.publish({
            topic: row.event_type,
            key: row.aggregate_id,
            envelope: row.envelope,
          });
          await this.store.markPublished(row.id);
          published += 1;
        } catch (error) {
          failed += 1;
          const message = error instanceof Error ? error.message : String(error);
          this.logger.error(
            `failed to publish ${row.event_type} ${row.event_id}`,
            error,
          );
          await this.markAttemptFailed(row.id, row.attempts + 1, message);
        }
      }

      if (published > 0 || failed > 0) {
        this.logger.debug(`cycle done: ${published} published, ${failed} failed`);
      }

      return { published, failed, skipped: false };
    } finally {
      try {
        await release();
      } catch (error) {
        this.logger.warn(
          `could not release the relay lock: ${(error as Error).message}`,
        );
      }
    }
  }

  private async tick(): Promise<void> {
    if (this.inFlight) {
      return;
    }

    this.inFlight = true;

    try {
      await this.drainOnce();
    } catch (error) {
      this.logger.error("relay cycle threw", error);
    } finally {
      this.inFlight = false;
    }
  }

  private async park(id: string, reason: string): Promise<void> {
    try {
      await this.store.markFailed(id, reason, 1, 0);
    } catch (error) {
      this.logger.error(`could not park outbox row ${id}`, error);
    }
  }

  private async markAttemptFailed(
    id: string,
    attempts: number,
    message: string,
  ): Promise<void> {
    try {
      const result = await this.store.markFailed(
        id,
        message,
        this.maxAttempts,
        this.backoff(attempts),
      );

      if (result.status === "failed") {
        this.logger.warn(
          `outbox row ${id} parked as failed after ${result.attempts} attempts`,
        );
      }
    } catch (error) {
      // The lease expires and the row is retried; losing the bookkeeping must
      // not abort the rest of the batch.
      this.logger.error(`could not record the failure of ${id}`, error);
    }
  }
}
