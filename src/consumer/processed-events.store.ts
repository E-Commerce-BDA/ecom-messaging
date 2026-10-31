import type { Queryable } from "../outbox/types.js";

/** SQL identifiers cannot be bound, so they are validated before interpolation. */
const IDENTIFIER_PATTERN = /^[a-z_][a-z0-9_]{0,62}$/;

export interface ProcessedEventsStoreOptions {
  /** Schema that owns the table: `search`, `catalog`, `cart`. */
  schema: string;
  /** Defaults to `processed_events`. */
  table?: string;
}

/**
 * The per-consumer idempotency claim.
 *
 * Every service grew the same table by hand before this class existed —
 * `search.processed_events` (`003`), `catalog.processed_events` (`006`), and
 * `cart.processed_events` for ca-009 — with identical shape:
 * `event_id uuid PRIMARY KEY, topic text NOT NULL, processed_at timestamptz`.
 * Only the DDL is the service's; the claim is not.
 *
 * `claim` is an `INSERT … ON CONFLICT DO NOTHING RETURNING`, so the *database*
 * decides who wins a redelivery. It takes the caller's client on purpose: the
 * claim and the business write share one transaction, which is the only reason
 * a crash between them cannot leave an event marked done and unapplied.
 */
export class ProcessedEventsStore {
  /** Quoted, because `<schema>.processed_events` is never a reserved word but `<schema>.order` would be. */
  private readonly table: string;

  constructor(options: ProcessedEventsStoreOptions) {
    const table = options.table ?? "processed_events";

    if (!IDENTIFIER_PATTERN.test(options.schema)) {
      throw new Error(`invalid processed-events schema: ${options.schema}`);
    }
    if (!IDENTIFIER_PATTERN.test(table)) {
      throw new Error(`invalid processed-events table: ${table}`);
    }

    this.table = `"${options.schema}"."${table}"`;
  }

  /**
   * Claims `eventId`. Resolves `true` exactly once per event id, even when two
   * replicas deliver the same record at the same moment: the loser's
   * `ON CONFLICT` yields no row.
   */
  async claim(client: Queryable, eventId: string, topic: string): Promise<boolean> {
    const { rows } = await client.query(
      `INSERT INTO ${this.table} (event_id, topic)
       VALUES ($1, $2)
       ON CONFLICT (event_id) DO NOTHING
       RETURNING event_id`,
      [eventId, topic],
    );

    return rows.length > 0;
  }

  /**
   * Read-only peek, for handlers that want to skip work before opening a
   * transaction. Never use it to decide whether to apply: only `claim` closes
   * the race.
   */
  async isProcessed(client: Queryable, eventId: string): Promise<boolean> {
    const { rows } = await client.query(
      `SELECT 1 FROM ${this.table} WHERE event_id = $1`,
      [eventId],
    );

    return rows.length > 0;
  }
}
