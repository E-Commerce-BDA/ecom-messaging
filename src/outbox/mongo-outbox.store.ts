import { buildEventEnvelope } from "../envelope.js";
import type {
  EventTopic,
  MarkFailedResult,
  MongoCollectionLike,
  MongoFindOneAndUpdateResult,
  MongoOutboxDb,
  MongoOutboxDocument,
  MongoSessionLike,
  MongoUpdateLike,
  OutboxRow,
  OutboxStorePort,
  OutboxWriteInput,
  RelayLockRelease,
} from "./types.js";

export interface MongoOutboxStoreOptions {
  /** Structural access to the database, normally `() => connection.db`. */
  db: () => MongoOutboxDb;
  /** Collection that holds the events. Defaults to `outbox`. */
  collection?: string;
  /** Value written into `envelope.producer`. */
  producer: string;
  /** Collection that holds the relay lease. Defaults to `<collection>_locks`. */
  lockCollection?: string;
  /** How long a relay lease lasts. Default 30 s. */
  lockLeaseSeconds?: number;
}

/** The lease document is `{ _id: 'relay', locked_until: Date | null }`. */
const LOCK_ID = "relay";
const EPOCH = new Date(0);

function isDuplicateKey(error: unknown): boolean {
  return (
    typeof error === "object" &&
    error !== null &&
    (error as { code?: unknown }).code === 11000
  );
}

/**
 * The two driver shapes in one place.
 *
 * Driver 6.20 resolves `findOneAndUpdate` to the document; the typings and
 * earlier 6.x releases wrap it in `{ value }`. An outbox document never has a
 * `value` field, so the discriminator is unambiguous.
 */
function unwrapDocument<TDoc>(
  result: MongoFindOneAndUpdateResult<TDoc>,
): TDoc | null {
  if (result === null || result === undefined) {
    return null;
  }

  const record = result as Record<string, unknown>;
  if ("value" in record) {
    return (record.value as TDoc | null | undefined) ?? null;
  }

  return result as TDoc;
}

/**
 * Mongo-backed outbox store.
 *
 * Same contract as `PostgresOutboxStore`, expressed in the only two primitives
 * Mongo has that can stand in for the Postgres ones:
 *
 * - `FOR UPDATE SKIP LOCKED` becomes a per-document `findOneAndUpdate` whose
 *   filter only matches an expired or absent lease. One document per round
 *   trip, applied `limit` times, because that is what makes the claim atomic.
 * - `pg_try_advisory_lock` becomes a lease document in a second collection.
 *   A duplicate-key error on the upsert is the "somebody else holds it" signal.
 *
 * The relay, the envelope builder, the retry policy and the Kafka publisher are
 * shared with the Postgres store; nothing about publishing is re-implemented.
 */
export class MongoOutboxStore implements OutboxStorePort<MongoSessionLike> {
  private readonly db: () => MongoOutboxDb;
  private readonly collectionName: string;
  private readonly lockCollectionName: string;
  private readonly producer: string;
  private readonly lockLeaseSeconds: number;

  constructor(options: MongoOutboxStoreOptions) {
    const collection = options.collection ?? "outbox";

    if (!collection) {
      throw new Error("mongo outbox store requires a collection name");
    }
    if (!options.producer) {
      throw new Error("outbox store requires a producer name");
    }

    this.db = options.db;
    this.collectionName = collection;
    this.lockCollectionName = options.lockCollection ?? `${collection}_locks`;
    this.producer = options.producer;
    this.lockLeaseSeconds = options.lockLeaseSeconds ?? 30;
  }

  /** Indexes: unique on the event id, partial on the pending queue. */
  async initialize(): Promise<void> {
    const events = this.events();

    await events.createIndex(
      { event_id: 1 },
      { name: "uq_outbox_event_id", unique: true },
    );
    await events.createIndex(
      { status: 1, created_at: 1 },
      {
        name: "idx_outbox_pending",
        partialFilterExpression: { status: "pending" },
      },
    );
  }

  async write<T extends EventTopic>(
    session: MongoSessionLike,
    input: OutboxWriteInput<T>,
  ): Promise<string> {
    const envelope = buildEventEnvelope({
      id: input.eventId,
      topic: input.topic,
      producer: this.producer,
      timestamp: input.occurredAt,
      payload: input.payload,
      version: input.version,
    });

    const document: MongoOutboxDocument = {
      // The event id doubles as the primary key, so a duplicate write is
      // rejected by the driver instead of by a hand-written check.
      _id: input.eventId,
      event_id: input.eventId,
      aggregate_type: input.aggregateType,
      aggregate_id: input.aggregateId,
      event_type: input.topic,
      envelope,
      status: "pending",
      attempts: 0,
      last_error: null,
      locked_until: null,
      // Insertion time, not `occurredAt`: the relay claims in the order rows
      // became claimable, exactly like the Postgres `now()` default.
      created_at: new Date(),
      published_at: null,
    };

    await this.events().insertOne(document, { session });

    return envelope.id;
  }

  async claimBatch(limit: number, leaseSeconds: number): Promise<OutboxRow[]> {
    const events = this.events();
    const claimed: OutboxRow[] = [];
    const now = new Date();
    const leaseUntil = new Date(now.getTime() + leaseSeconds * 1000);

    // One atomic findOneAndUpdate per row: the filter and the lease update are
    // a single document operation, which is this driver's SKIP LOCKED.
    for (let index = 0; index < limit; index += 1) {
      const value = unwrapDocument(
        await events.findOneAndUpdate(
          {
            status: "pending",
            $or: [{ locked_until: null }, { locked_until: { $lt: now } }],
          },
          { $set: { locked_until: leaseUntil } },
          { sort: { created_at: 1, _id: 1 }, returnDocument: "after" },
        ),
      );

      if (!value) {
        break;
      }

      claimed.push({
        id: value._id,
        event_id: value.event_id,
        aggregate_type: value.aggregate_type,
        aggregate_id: value.aggregate_id,
        event_type: value.event_type,
        envelope: value.envelope,
        attempts: value.attempts,
      });
    }

    return claimed;
  }

  async markPublished(id: string): Promise<void> {
    await this.events().updateOne(
      { _id: id },
      {
        $set: {
          status: "published",
          published_at: new Date(),
          locked_until: null,
          last_error: null,
        },
      },
    );
  }

  /**
   * Mirrors the Postgres statement: increment, park at `maxAttempts`, and lease
   * the row again with the backoff while it still has attempts left.
   *
   * A pipeline update keeps `attempts + 1` and the decisions that depend on it
   * in one atomic document write, so two relays cannot both read `attempts = 4`.
   */
  async markFailed(
    id: string,
    error: string,
    maxAttempts: number,
    backoffSeconds: number,
  ): Promise<MarkFailedResult> {
    const nextAttempts = { $add: ["$attempts", 1] };
    const parks = { $gte: [nextAttempts, maxAttempts] };
    const retryAt = new Date(Date.now() + backoffSeconds * 1000);

    const update: MongoUpdateLike = [
      {
        $set: {
          attempts: nextAttempts,
          status: { $cond: [parks, "failed", "pending"] },
          last_error: error.slice(0, 500),
          locked_until: { $cond: [parks, null, retryAt] },
        },
      },
    ];

    const value = unwrapDocument(
      await this.events().findOneAndUpdate({ _id: id }, update, {
        returnDocument: "after",
      }),
    );

    if (!value) {
      throw new Error(`outbox row ${id} disappeared while marking it failed`);
    }

    // The pipeline can only write `pending` or `failed`; the document type is
    // wider because a row may already be `published`.
    return {
      attempts: value.attempts,
      status: value.status === "failed" ? "failed" : "pending",
    };
  }

  /**
   * Lease document instead of an advisory lock.
   *
   * An upsert that cannot match an existing document tries to insert `_id:
   * 'relay'` and fails with E11000 — which is precisely "another worker holds
   * the lock". A crashed holder stops blocking when its lease runs out, so a
   * dead process can never wedge the relay.
   */
  async tryAcquireRelayLock(
    leaseSeconds = this.lockLeaseSeconds,
  ): Promise<RelayLockRelease | null> {
    const locks = this.locks();
    const now = new Date();
    const until = new Date(now.getTime() + leaseSeconds * 1000);

    try {
      await locks.findOneAndUpdate(
        {
          _id: LOCK_ID,
          $or: [{ locked_until: null }, { locked_until: { $lt: now } }],
        },
        { $set: { locked_until: until } },
        { upsert: true, returnDocument: "after" },
      );
    } catch (error) {
      if (isDuplicateKey(error)) {
        return null;
      }
      throw error;
    }

    let released = false;

    return async () => {
      if (released) {
        return;
      }
      released = true;

      await locks.updateOne({ _id: LOCK_ID }, { $set: { locked_until: EPOCH } });
    };
  }

  private events(): MongoCollectionLike<MongoOutboxDocument> {
    return this.db().collection(this.collectionName);
  }

  private locks(): MongoCollectionLike<MongoOutboxDocument> {
    return this.db().collection(this.lockCollectionName);
  }
}
