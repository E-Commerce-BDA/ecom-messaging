import type { AppliedCounter, KafkaHandlerOutcome } from "./types.js";

/**
 * Counters every consumer keeps, plus three the transport owns.
 *
 * Process-local and reset on boot, which is still the cheapest thing that can
 * answer "how many events did we drop in the last hour" without a metrics
 * sidecar. `accepted`/`skipped`/`committed` are the success counters of the
 * three services; `commit` and `stalled` belong to the commit protocol below.
 */
export type ConsumerCounter =
  | "accepted"
  | "skipped"
  | "committed"
  | "duplicate"
  | "ignored"
  | "invalid"
  | "dlq"
  | "commit"
  | "stalled";

export type ConsumerCounters = Record<ConsumerCounter, number>;

const COUNTERS: readonly ConsumerCounter[] = [
  "accepted",
  "skipped",
  "committed",
  "duplicate",
  "ignored",
  "invalid",
  "dlq",
  "commit",
  "stalled",
];

/** Where the group got to last. What a lag dashboard reads next to the lag. */
export interface ConsumerCommitMark {
  topic: string;
  partition: number;
  /** The committed offset: the message offset, i.e. the next one to read. */
  offset: string;
  committedAt: string;
}

export class ConsumerMetrics {
  private readonly counts: ConsumerCounters = {
    accepted: 0,
    skipped: 0,
    committed: 0,
    duplicate: 0,
    ignored: 0,
    invalid: 0,
    dlq: 0,
    commit: 0,
    stalled: 0,
  };

  private mark: ConsumerCommitMark | null = null;

  private readonly clock: () => Date;

  constructor(clock: () => Date = () => new Date()) {
    this.clock = clock;
  }

  record(counter: ConsumerCounter): void {
    this.counts[counter] += 1;
  }

  /**
   * Records an acknowledged offset.
   *
   * Called only after `commitOffsets` resolved, never before: a "last commit"
   * that leads the broker is worse than no gauge at all.
   */
  recordCommit(topic: string, partition: number, offset: string): void {
    this.counts.commit += 1;
    this.mark = {
      topic,
      partition,
      offset,
      committedAt: this.clock().toISOString(),
    };
  }

  snapshot(): ConsumerCounters {
    return { ...this.counts };
  }

  lastCommit(): ConsumerCommitMark | null {
    return this.mark ? { ...this.mark } : null;
  }

  reset(): void {
    for (const counter of COUNTERS) {
      this.counts[counter] = 0;
    }
    this.mark = null;
  }
}

/**
 * Folds one handled outcome into the counters.
 *
 * `dlq` is deliberately *not* recorded here: it counts DLQ writes that actually
 * succeeded, and only the transport can know that.
 */
export function recordHandlerOutcome(
  metrics: ConsumerMetrics,
  outcome: KafkaHandlerOutcome,
): void {
  switch (outcome.kind) {
    case "applied":
      metrics.record(successCounter(outcome.counter));
      break;
    case "duplicate":
      metrics.record("duplicate");
      break;
    case "ignored":
      metrics.record("ignored");
      break;
    case "invalid":
      metrics.record("invalid");
      break;
  }
}

function successCounter(counter: AppliedCounter | undefined): AppliedCounter {
  return counter ?? "accepted";
}
