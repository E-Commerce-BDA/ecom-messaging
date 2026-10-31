import type {
  EventEnvelope,
  EventPayload,
  EventTopic,
} from "@ecom/contracts";

/**
 * Structural views of the `pg` API.
 *
 * The package deliberately does not depend on `pg` or `@types/pg`: a service
 * passes its own `getPool` and the structural type lines up. Keeping the types
 * structural is also what keeps this package framework- and driver-agnostic.
 */
export interface Queryable {
  query(
    text: string,
    values?: readonly unknown[],
  ): Promise<{ rows: readonly unknown[] }>;
}

export interface PoolClientLike extends Queryable {
  release(error?: Error): void;
}

export interface PoolLike extends Queryable {
  connect(): Promise<PoolClientLike>;
}

/**
 * Structural view of a Mongo driver session.
 *
 * Same rule as `Queryable`: the package never imports `mongodb` or `mongoose`.
 * The caller passes the real `ClientSession`; only the fact that it can be
 * handed back to the driver matters here.
 */
export interface MongoSessionLike {
  /** Present on every driver session; keeps the type from accepting anything. */
  readonly id?: unknown;
}

export interface MongoWriteOptionsLike {
  session?: MongoSessionLike;
}

export interface MongoFindOneAndUpdateOptionsLike extends MongoWriteOptionsLike {
  sort?: Record<string, 1 | -1>;
  upsert?: boolean;
  returnDocument?: "before" | "after";
}

export interface MongoCreateIndexOptionsLike {
  name?: string;
  unique?: boolean;
  partialFilterExpression?: Record<string, unknown>;
}

/** `{$set: ...}` document or an aggregation pipeline (`[{ $set: ... }]`). */
export type MongoUpdateLike =
  | Record<string, unknown>
  | readonly Record<string, unknown>[];

/**
 * What `findOneAndUpdate` resolves to.
 *
 * The 6.x typings promise `{ value }`, and releases before 6.0 returned exactly
 * that. Driver 6.20 — the one mongoose 8.24 ships — resolves to the document
 * itself. Both shapes are accepted, so the store does not depend on which
 * minor of the driver a service happens to install.
 */
export type MongoFindOneAndUpdateResult<TDoc> =
  | { value: TDoc | null }
  | TDoc
  | null;

/**
 * The slice of a Mongo collection the outbox store uses.
 *
 * The claim, the retry bookkeeping and the relay lease all read the result of a
 * `findOneAndUpdate`, so this method is where the two driver shapes meet: the
 * store unwraps whichever one arrives.
 */
export interface MongoCollectionLike<TDoc> {
  insertOne(doc: TDoc, options?: MongoWriteOptionsLike): Promise<unknown>;
  findOneAndUpdate(
    filter: Record<string, unknown>,
    update: MongoUpdateLike,
    options?: MongoFindOneAndUpdateOptionsLike,
  ): Promise<MongoFindOneAndUpdateResult<TDoc>>;
  updateOne(
    filter: Record<string, unknown>,
    update: MongoUpdateLike,
    options?: MongoWriteOptionsLike,
  ): Promise<unknown>;
  createIndex(
    spec: Record<string, 1 | -1>,
    options?: MongoCreateIndexOptionsLike,
  ): Promise<string>;
}

export interface MongoOutboxDb {
  collection(name: string): MongoCollectionLike<MongoOutboxDocument>;
}

/** A claimed outbox row, exactly as the relay needs it. */
export interface OutboxRow {
  id: string;
  event_id: string;
  aggregate_type: string;
  aggregate_id: string;
  event_type: string;
  envelope: EventEnvelope;
  attempts: number;
}

/**
 * One event to persist. `eventId` is supplied by the caller so the service
 * keeps its own id strategy (uuid v7), `occurredAt` is the transaction's
 * timestamp rather than wall clock at publish time.
 */
export interface OutboxWriteInput<T extends EventTopic> {
  topic: T;
  eventId: string;
  aggregateType: string;
  aggregateId: string;
  occurredAt: Date;
  payload: EventPayload<T>;
  version?: number;
}

export type MarkFailedResult = {
  attempts: number;
  status: "pending" | "failed";
};

/** Release the exclusive relay lock. Idempotent. */
export type RelayLockRelease = () => Promise<void>;

/**
 * One stored Mongo outbox document.
 *
 * `_id` is the event id, so the driver's own uniqueness constraint is the
 * idempotency guarantee — no extra index is needed to reject a duplicate write.
 */
export interface MongoOutboxDocument extends Record<string, unknown> {
  _id: string;
  event_id: string;
  aggregate_type: string;
  aggregate_id: string;
  event_type: string;
  envelope: EventEnvelope;
  status: "pending" | "published" | "failed";
  attempts: number;
  last_error: string | null;
  locked_until: Date | null;
  created_at: Date;
  published_at: Date | null;
}

/**
 * What a store must offer to be usable by the relay and by a producer.
 *
 * `Ctx` is the write context: a `Queryable` for Postgres, a session for Mongo.
 * The default keeps every existing Postgres call site compiling unchanged, and
 * the relay asks for `OutboxStorePort<never>` because it never writes.
 */
export interface OutboxStorePort<Ctx = Queryable> {
  /**
   * Insert the event using the caller's transaction context. Never opens a
   * transaction of its own: the whole point is that the row commits or rolls
   * back together with the domain mutation.
   */
  write<T extends EventTopic>(
    ctx: Ctx,
    input: OutboxWriteInput<T>,
  ): Promise<string>;

  /** Claim up to `limit` pending rows, leasing them for `leaseSeconds`. */
  claimBatch(limit: number, leaseSeconds: number): Promise<OutboxRow[]>;

  markPublished(id: string): Promise<void>;

  markFailed(
    id: string,
    error: string,
    maxAttempts: number,
    backoffSeconds: number,
  ): Promise<MarkFailedResult>;

  /**
   * Take the exclusive relay lock, or return `null` when another worker holds
   * it. Serialising the relay is what preserves per-aggregate publish order
   * when the service runs more than one replica.
   */
  tryAcquireRelayLock(leaseSeconds?: number): Promise<RelayLockRelease | null>;

  /**
   * One-time setup: indexes for Mongo, nothing for Postgres. Called by the Nest
   * module on boot, including in tests where the relay itself is disabled.
   */
  initialize?(): Promise<void>;
}

export type { EventEnvelope, EventPayload, EventTopic };
