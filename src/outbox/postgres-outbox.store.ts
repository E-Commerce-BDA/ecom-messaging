import { buildEventEnvelope } from "../envelope.js";
import type {
  EventTopic,
  MarkFailedResult,
  OutboxRow,
  OutboxStorePort,
  OutboxWriteInput,
  PoolClientLike,
  PoolLike,
  Queryable,
  RelayLockRelease,
} from "./types.js";

/** SQL identifiers cannot be bound, so they are validated before interpolation. */
const IDENTIFIER_PATTERN = /^[a-z_][a-z0-9_]{0,62}$/;

export interface PostgresOutboxStoreOptions {
  /** Structural pool accessor, normally the service's own `getPool`. */
  pool: () => PoolLike;
  /** Schema that owns the table: `auth` or `cart`. */
  schema: string;
  /** Value written into `envelope.producer`. */
  producer: string;
  /** Defaults to `outbox`. */
  table?: string;
}

/**
 * Postgres-backed outbox store.
 *
 * Two invariants live here and nowhere else:
 *
 * 1. `write` inserts through the caller's client, so the event and the domain
 *    row are one transaction.
 * 2. `claimBatch` leases rows with `FOR UPDATE SKIP LOCKED` and `tryAcquireRelayLock`
 *    serialises the relay across replicas, which is what keeps per-aggregate
 *    publish order intact.
 */
export class PostgresOutboxStore implements OutboxStorePort<Queryable> {
  private readonly pool: () => PoolLike;
  private readonly producer: string;
  /** Quoted, because `order` is a reserved word and `order.outbox` is a syntax error. */
  private readonly table: string;
  /** Unquoted, so auth/cart keep the same advisory-lock namespace they always had. */
  private readonly lockKey: string;

  constructor(options: PostgresOutboxStoreOptions) {
    const table = options.table ?? "outbox";

    if (!IDENTIFIER_PATTERN.test(options.schema)) {
      throw new Error(`invalid outbox schema: ${options.schema}`);
    }
    if (!IDENTIFIER_PATTERN.test(table)) {
      throw new Error(`invalid outbox table: ${table}`);
    }
    if (!options.producer) {
      throw new Error("outbox store requires a producer name");
    }

    this.pool = options.pool;
    this.producer = options.producer;
    this.lockKey = `${options.schema}.${table}`;
    this.table = `"${options.schema}"."${table}"`;
  }

  /** Postgres needs no setup; the table and its index come from the service's SQL. */
  async initialize(): Promise<void> {
    // Intentionally empty: `initialize` exists so both stores share one port.
  }

  async write<T extends EventTopic>(
    client: Queryable,
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

    await client.query(
      `INSERT INTO ${this.table}
         (event_id, aggregate_type, aggregate_id, event_type, envelope)
       VALUES ($1, $2, $3, $4, $5::jsonb)`,
      [
        input.eventId,
        input.aggregateType,
        input.aggregateId,
        input.topic,
        JSON.stringify(envelope),
      ],
    );

    return envelope.id;
  }

  async claimBatch(limit: number, leaseSeconds: number): Promise<OutboxRow[]> {
    const { rows } = await this.pool().query(
      `WITH claimed AS (
         SELECT id
           FROM ${this.table}
          WHERE status = 'pending'
            AND (locked_until IS NULL OR locked_until < now())
          ORDER BY created_at, id
          LIMIT $1
          FOR UPDATE SKIP LOCKED
       )
       UPDATE ${this.table} AS o
          SET locked_until = now() + make_interval(secs => $2)
         FROM claimed
        WHERE o.id = claimed.id
       RETURNING o.id, o.event_id, o.aggregate_type, o.aggregate_id,
                 o.event_type, o.envelope, o.attempts`,
      [limit, leaseSeconds],
    );

    return rows as unknown as OutboxRow[];
  }

  async markPublished(id: string): Promise<void> {
    await this.pool().query(
      `UPDATE ${this.table}
          SET status = 'published',
              published_at = now(),
              locked_until = NULL,
              last_error = NULL
        WHERE id = $1`,
      [id],
    );
  }

  async markFailed(
    id: string,
    error: string,
    maxAttempts: number,
    backoffSeconds: number,
  ): Promise<MarkFailedResult> {
    const { rows } = await this.pool().query(
      `UPDATE ${this.table}
          SET attempts = attempts + 1,
              status = CASE
                         WHEN attempts + 1 >= $2 THEN 'failed'
                         ELSE 'pending'
                       END,
              last_error = left($3, 500),
              locked_until = CASE
                               WHEN attempts + 1 >= $2 THEN NULL
                               ELSE now() + make_interval(secs => $4)
                             END
        WHERE id = $1
       RETURNING attempts, status`,
      [id, maxAttempts, error, backoffSeconds],
    );

    const row = rows[0] as MarkFailedResult | undefined;
    if (!row) {
      throw new Error(`outbox row ${id} disappeared while marking it failed`);
    }

    return row;
  }

  async tryAcquireRelayLock(_leaseSeconds?: number): Promise<RelayLockRelease | null> {
    const client = await this.pool().connect();
    let acquired = false;

    try {
      const { rows } = await client.query(
        "SELECT pg_try_advisory_lock(hashtextextended($1, 0)) AS locked",
        [this.lockKey],
      );

      acquired = (rows[0] as { locked?: boolean } | undefined)?.locked === true;
    } catch (error) {
      this.releaseClient(client);
      throw error;
    }

    if (!acquired) {
      this.releaseClient(client);
      return null;
    }

    let released = false;

    return async () => {
      if (released) {
        return;
      }
      released = true;

      try {
        await client.query(
          "SELECT pg_advisory_unlock(hashtextextended($1, 0))",
          [this.lockKey],
        );
      } finally {
        this.releaseClient(client);
      }
    };
  }

  private releaseClient(client: PoolClientLike): void {
    try {
      client.release();
    } catch {
      // A pool that already reclaimed the client is not an error here.
    }
  }
}
