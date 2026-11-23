export interface KafkaMessageLike {
  offset: string;
  key?: Buffer | string | null;
  value?: Buffer | string | null;
  headers?: Record<string, unknown>;
}

/**
 * The slice of a kafkajs consumer the commit step uses.
 *
 * `KafkaContext#getConsumer()` returns the real one, so a Nest controller needs
 * no cast. Keeping it structural is also what lets this file sit in the package
 * root without importing `@nestjs/microservices`.
 */
export interface KafkaConsumerLike {
  commitOffsets(
    offsets: { topic: string; partition: number; offset: string }[],
  ): Promise<void>;
}

/**
 * Structural view of `@nestjs/microservices`' `KafkaContext`.
 *
 * A real `KafkaContext` satisfies it as-is. The package deliberately does not
 * import the Nest type: the same `run()` call has to be usable from a plain
 * `eachMessage` loop in a test without pulling the framework in.
 */
export interface KafkaMessageContextLike {
  getMessage(): KafkaMessageLike;
  getPartition(): number;
  getConsumer(): KafkaConsumerLike;
}

/**
 * Name of a success counter.
 *
 * Three names exist for what looks like one outcome because the services
 * already disagree and their dashboards are not worth breaking: `accepted` and
 * `skipped` come from search-svc's indexer (`skipped` means the document was
 * understood but refused, e.g. an older revision), `committed` from
 * catalog-svc's reservation commit. A handler picks its own by returning
 * `counter`; the default is `accepted`.
 */
export type AppliedCounter = "accepted" | "skipped" | "committed";

/**
 * What a handler reports back.
 *
 * A handler never throws for a *predictable* failure: "this record is not for
 * me" and "this payload is broken" are outcomes, not exceptions, because they
 * have different destinations (commit vs DLQ) and the transport — not the
 * handler — owns that decision.
 */
export type KafkaHandlerOutcome =
  /** Side effect applied, or deliberately unnecessary. Committed and counted. */
  | { kind: "applied"; counter?: AppliedCounter }
  /**
   * The event id was already claimed. A no-op: the effect ran in an earlier
   * delivery, so the offset is safe to advance.
   */
  | { kind: "duplicate"; eventId?: string }
  | { kind: "ignored"; reason: string; type?: string }
  /**
   * Unreadable. Dead-lettered *before* the offset moves, so a record is never
   * counted as handled when it was actually parked.
   */
  | { kind: "invalid"; stage: "envelope" | "payload"; error: string };

export type KafkaHandlerFn = (envelope: unknown) => Promise<KafkaHandlerOutcome>;
