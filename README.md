# @ecom/messaging

Transactional outbox relay, the Kafka publishers, the Kafka consumer protocol
and — since `0.4.0` — the SQS task queue, shared by every E-Commerce service.
One implementation of the claim/lease/retry loop instead of one per service,
one implementation of "read a record, apply it, dead-letter it if you must, then
commit" instead of one per consumer, and one implementation of "receive a task,
do it, delete it" instead of one per worker.

`0.x` is deliberate: the API stabilises at `1.0.0` once a third service adopts
the outbox (`ord-011`). The consumer surface shipped in `0.3.0` and is now what
`cart-svc`, `catalog-svc` and `search-svc` all run; the task surface shipped in
`0.4.0` for `cms-006`.

Two stores implement one outbox port: `PostgresOutboxStore` and
`MongoOutboxStore`. Everything above the claim — the relay, the envelope builder,
the retry policy, the Kafka publisher — is shared, so a service picks a
database, not a design.

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
| throws, retryable | — | retried `1/2/4/8/16s` within `retryBudgetMs`, then committed after the dead letter | `transient-exhausted` |
| throws, `translateError` returns a reason | — | committed after the dead letter | that reason |
| malformed JSON | `invalid` | committed after the dead letter | `invalid-json` |

**The ladder must fit inside the session.** The strategy sets
`sessionTimeout` to `DEFAULT_SESSION_TIMEOUT_MS` (45s) rather than kafkajs's 30s
default, because the five retries above sleep a total of 31s: a member still
asleep when its session expires is evicted, and the coordinator then refuses both
the dead letter and the offset commit that waits on it (`UNKNOWN_MEMBER_ID`), so
the record is redelivered and retried forever. A service that wants a shorter
session can pass `retryBudgetMs` to the handler, which truncates the ladder to the
longest run of delays that fits and warns at boot about the cut.

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

## Running tasks

Kafka carries facts; SQS carries commands. A task is for exactly one worker, so
it has no fan-out and no offset — and it has to survive a redrive, which is why
`@ecom/contracts` 1.11.0 wraps every job in a `TaskMessage` with an
`idempotencyKey`.

```ts
import { createSqsTaskModule, SQS_TASK_QUEUE } from "@ecom/messaging/nest/tasks";
import type { TaskQueuePort, TaskHandler } from "@ecom/messaging";
import { TASK_QUEUES, resizeImageJobSchema, type ResizeImageJob } from "@ecom/contracts";

@Injectable()
export class MediaTasksPublisher {
  constructor(@Inject(SQS_TASK_QUEUE) private readonly tasks: TaskQueuePort) {}

  enqueue(mediaId: string) {
    return this.tasks.enqueue(TASK_QUEUES.mediaResizeImage, {
      type: "media.resize-image",
      // Identity of the effect, not of the send: a resend must not redo work.
      idempotencyKey: mediaId,
      payload: { mediaId, sourceKey: key, formats: ["webp"], widths: [320, 960] },
    });
  }
}

@Module({
  imports: [
    createSqsTaskModule<ResizeImageJob>({
      queue: TASK_QUEUES.mediaResizeImage,
      payloadSchema: resizeImageJobSchema,
      handler: async (job, delivery) => {
        await resizeAndPersist(job, delivery.message.idempotencyKey);
      },
    }),
  ],
})
export class MediaModule {}
```

The module starts and stops the poller with the Nest lifecycle, and is compiled
out of the application entirely when `SQS_TASKS_ENABLED=false` or
`NODE_ENV=test` — so a suite that never started LocalStack has no poller to
leak. An explicit `SQS_TASKS_ENABLED` wins over `NODE_ENV` in both directions,
which is how the one suite that owns a real queue opts in. `AWS_REGION`,
`AWS_ENDPOINT_URL` and the two credential variables are read by the SDK's own
chain; the module takes no secrets.

Four narrow entries exist for one rule: **an entry point must not load a
transport its service does not use.** `@ecom/messaging/nest/tasks` and
`.../nest/outbox` came first, because the `/nest` barrel also re-exports the
Kafka consumer strategy, which imports `@nestjs/microservices` eagerly — a
service that only produces (cms-svc after `cms-006`) should not need the Kafka
transport to enqueue a command. `.../nest/consumer` and `.../tasks` are the same
rule pointing the other way, added in 0.5.0 because `tasks.module.js` imports
`@aws-sdk/client-sqs` at module scope and search-svc consumes events without ever
enqueueing one: an optional peer that is not installed must not be reachable from
the entry a service does use. Each barrel still re-exports everything it did, so
an existing import path keeps working for a service that has both peers.

A handler that needs injected dependencies is passed as a `handlerToken` rather
than a function. A dynamic module cannot see the providers of the module that
imports it, so the handler is provided by a module that is listed in `imports`;
`MediaResizeModule` in cms-svc is the reference implementation.

The four rules of the poller, in the order they matter:

1. **Delete only after the handler resolves.** The delete is the ack.
2. **Never delete on failure.** The message becomes visible again when its
   visibility timeout expires, and the queue's redrive policy moves it to the
   DLQ after `maxReceiveCount`. There is no DLQ publisher here on purpose:
   `ecom-infra/aws/sqs.tf` and the LocalStack init already own that policy, and
   a second copy would drift from it.
3. **A malformed message is a failure, not a skip.** It cannot succeed on a
   retry, but deleting it would destroy the only evidence — five receives put it
   in the DLQ where a human looks.
4. **One message at a time.** Processing is sequential per pod, so a `sharp`
   worker on a 1 GB box cannot hold ten decodes at once. Scale out with pods;
   SQS hands each visible message to exactly one receiver.

`PayloadParser` is structural, not `z.ZodType`, which is why this package still
has no zod dependency: a zod schema satisfies it as-is.

## What it is not

- No scheduler, no service discovery, no routing layer. Each service decides
  which topics it subscribes to with `@EventPattern`.
- No business rules. `KafkaEventHandler` never inspects a payload; the handler
  it is given does.
- No schema knowledge beyond the envelope. The payload types come from
  `@ecom/contracts`, and `buildEventEnvelope` rejects an envelope whose
  `type`/`topic`/`eventType` disagree or whose payload drifted from its schema.
- No queue creation and no redrive configuration. Terraform and the LocalStack
  init own both; this package only reads `TASK_QUEUES` for the names and the
  visibility timeout.
