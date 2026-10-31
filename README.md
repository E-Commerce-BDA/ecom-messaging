# @ecom/messaging

Transactional outbox relay and the Kafka publishers shared by every E-Commerce
producer. One implementation of the claim/lease/retry loop instead of one per
service.

`0.x` is deliberate: the API stabilises at `1.0.0` once a third service adopts
it (`ord-011`). Until then consumers pin the committed tarball.

Two stores implement one port: `PostgresOutboxStore` and `MongoOutboxStore`.
Everything above the claim — the relay, the envelope builder, the retry policy,
the Kafka publisher — is shared, so a service picks a database, not a design.

## What it guarantees

- **Atomicity** — `store.write(client, …)` inserts through the caller's
  transaction client, so the event commits or rolls back with the domain row.
  A registration or a cart mutation can never succeed without its event.
- **One publisher** — `OutboxRelay` is the only thing that writes to Kafka for
  a service's outbox. A short-lived table lock is not needed; the lease plus
  `FOR UPDATE SKIP LOCKED` handles a crashed worker.
- **Ordering** — a claimed batch is published sequentially, keyed by aggregate
  id, and `tryAcquireRelayLock` keeps a single relay instance active across
  replicas. Per-aggregate order therefore survives scale-out.
- **At-least-once** — a crash between `send` and `markPublished` republishes the
  same envelope id. Consumers dedupe on `idempotencyKey`.
- **Bounded retry** — each failure increments `attempts` and sets a backoff
  lease; the fifth failure parks the row as `failed` (parity with the SQS
  `maxReceiveCount` of 5). `failed` rows are never retried automatically.

## Usage

### 1. Table

Create the table in the owning schema (see `auth-svc/db/003-auth-outbox.sql`).
The store expects `event_id, aggregate_type, aggregate_id, event_type, envelope,
status, attempts, last_error, locked_until, created_at, published_at`.

### 2. Module

```ts
import { createOutboxModule } from "@ecom/messaging/nest";
import { getPool } from "./db/pool.js";

@Module({
  imports: [
    createOutboxModule({
      schema: "cart",
      producer: "cart-svc",
      clientId: "cart-svc",
      pool: getPool,
    }),
  ],
})
export class CartModule {}
```

The relay runs only when `DISABLE_OUTBOX_RELAY !== 'true'` **and**
`NODE_ENV !== 'test'`. That decision is read once, when the module is declared.

### 3. Write the event inside the transaction

```ts
import { OUTBOX_STORE, type OutboxStorePort } from "@ecom/messaging/nest";

@Injectable()
export class CartOutbox {
  constructor(@Inject(OUTBOX_STORE) private readonly store: OutboxStorePort) {}

  record(client: Queryable, cart: CartRow) {
    return this.store.write(client, {
      topic: "cart.updated",
      eventId: uuidv7(),
      aggregateType: "cart",
      aggregateId: cart.id,
      occurredAt: cart.updated_at,
      payload: buildCartUpdatedEvent(cart, items, totals),
    });
  }
}
```

Pass the **transaction client**, never the pool: that is the whole point.

### 4. Mongo instead of Postgres

```ts
import { createMongoOutboxModule } from "@ecom/messaging/nest";
import { Connection } from "@nestjs/mongoose";

createMongoOutboxModule({
  collection: "cms_outbox",
  producer: "cms-svc",
  clientId: "cms-svc",
  // Structural `MongoOutboxDb`; the driver's `Db` satisfies it.
  db: () => connection.db,
});
```

`write` takes a `ClientSession` instead of a `Queryable`, so the event commits
with the document. `initialize()` creates the unique `event_id` index and the
partial pending index; it runs whether or not the relay is enabled.

### 5. Tests

Override `OUTBOX_PUBLISHER` with `RecordingPublisher` and drive
`relay.drainOnce()` yourself; the timer is never involved. Keep
`DISABLE_OUTBOX_RELAY=true` in the e2e config so no suite publishes by
accident.

## What it is not

- No consumer side. Dedupe (`processed_events`), DLQ routing and offset
  handling belong to the consuming service.
- No schema knowledge. The payload types come from `@ecom/contracts`, and
  `buildEventEnvelope` rejects an envelope whose `type`/`topic`/`eventType`
  disagree or whose payload drifted from its schema.
