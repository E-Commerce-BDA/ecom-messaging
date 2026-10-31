# @ecom/messaging

Transactional outbox relay, the Kafka publishers, and the Kafka consumer
protocol shared by every E-Commerce service. One implementation of the
claim/lease/retry loop instead of one per service — and, since `0.3.0`, one
implementation of "read a record, apply it, dead-letter it if you must, then
commit" instead of one per consumer.

`0.x` is deliberate: the API stabilises at `1.0.0` once a third service adopts
the outbox (`ord-011`). The consumer surface shipped in `0.3.0` and is now what
`cart-svc`, `catalog-svc` and `search-svc` all run.

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

## Consuming events

One consumer per service, never per topic. The group id is the service name, and
`postfixId: ""` keeps Nest from appending `-server` to it.

```ts
// main.ts
app.connectMicroservice({
  strategy: createKafkaConsumerStrategy({
    brokers,
    clientId: "cart-svc",
    groupId: "cart-svc",
  }),
});
await app.startAllMicroservices();
```

```ts
// events/cart-consumer.controller.ts
@Controller()
export class CartConsumerController {
  constructor(
    private readonly events: KafkaEventHandler,
    private readonly orders: OrderCreatedHandler,
  ) {}

  @EventPattern("order.created")
  async onOrderCreated(@Payload() raw: string, @Ctx() ctx: KafkaContext) {
    await this.events.run(ctx, "order.created", raw, (envelope) =>
      this.orders.handle(envelope),
    );
  }
}
```

`handle` returns an outcome, and the transport decides the destination:

| Handler result | Counter | Offset | DLQ |
|---|---|---|---|
| `{ kind: "applied" }` | `accepted` (or `skipped`/`committed`) | committed | — |
| `{ kind: "duplicate" }` | `duplicate` | committed | — |
| `{ kind: "ignored", reason }` | `ignored` | committed | — |
| `{ kind: "invalid", stage }` | `invalid` | committed **after** the dead letter | `invalid-envelope` / `invalid-payload` |
| throws, retryable | — | retried `1/2/4/8/16s`, then committed after the dead letter | `transient-exhausted` |
| throws, `translateError` returns a reason | — | committed after the dead letter | that reason |
| malformed JSON | `invalid` | committed after the dead letter | `invalid-json` |

The claim goes inside the same transaction as the effect:

```ts
async handle(envelope: unknown) {
  const { id, payload } = parseEventEnvelope("order.created",
    readEventEnvelope(envelope));

  return withTransaction(async (client) => {
    if (!(await this.processed.claim(client, id, "order.created"))) {
      return { kind: "duplicate" as const, eventId: id };
    }
    await convertCart(client, payload.userId);
    return { kind: "applied" as const };
  });
}
```

`ProcessedEventsStore` takes the caller's client for exactly that reason: the
claim and the write commit or roll back together, so a crash can never mark an
event done whose effect never happened.

A consumer that needs lag exposes `KafkaLagSampler` on its stats endpoint —
`start(15_000)` in `onModuleInit`, `latest()` on the request. The timer is
unref'd.

## What it is not

- No scheduler, no service discovery, no routing layer. Each service decides
  which topics it subscribes to with `@EventPattern`.
- No business rules. `KafkaEventHandler` never inspects a payload; the handler
  it is given does.
- No schema knowledge beyond the envelope. The payload types come from
  `@ecom/contracts`, and `buildEventEnvelope` rejects an envelope whose
  `type`/`topic`/`eventType` disagree or whose payload drifted from its schema.
