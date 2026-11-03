# Changelog

All notable changes to `@ecom/messaging` are recorded here. Versions follow
`Documentation/8.Versioning.md`; `0.x` until the third adopter (ord-011).

## 0.5.0

The retry ladder was sized against nothing. `srch-004` found a live consumer
looping on one poisoned record: the five retries sleep 1+2+4+8+16 = **31s**, the
Kafka session timeout was kafkajs's default **30s**, so the coordinator evicted
the member mid-retry, `UNKNOWN_MEMBER_ID` then refused both the dead letter and
the offset commit that waits on it, and the partition re-delivered the same
record forever — every suite green, the service down.

### Added

- `createKafkaConsumerStrategy` now sets `consumer.sessionTimeout` and
  `consumer.heartbeatInterval` instead of inheriting library defaults, with
  `DEFAULT_SESSION_TIMEOUT_MS` (45 000) and `DEFAULT_HEARTBEAT_INTERVAL_MS`
  (3 000) exported. Both are overridable per service. A heartbeat at or above the
  session timeout is rejected at boot with both numbers in the message.
- `KafkaEventHandler` option `retryBudgetMs` — an upper bound on the **total
  sleep** of the ladder. When set, the ladder is truncated at construction to the
  longest leading run of delays that fits, `maxRetries` follows it (an explicit
  `maxRetries` is capped rather than allowed to repeat the last delay and
  overshoot), the first delay is kept even if it alone exceeds the budget so a
  transient error still retries at least once, and the cut is warned at boot
  rather than applied silently.
- `test/consumer/kafka-consumer.strategy.spec.ts` asserts the emitted session and
  heartbeat pair and the boot guard; `kafka-event-handler.spec.ts` asserts the
  truncation, the one-retry floor, the `maxRetries` cap, and that the dead letter
  and commit still happen on a shortened ladder.
- `@ecom/messaging/nest/consumer` and `@ecom/messaging/tasks` — narrow entries
  for the consumer surface and the framework-free task surface. search-svc could
  not repin to 0.4.0+ at all: `nest/index.ts` re-exports `tasks.module.js`, which
  imports `@aws-sdk/client-sqs` at module scope, so a Kafka-only consumer with no
  SQS failed at boot with `ERR_MODULE_NOT_FOUND` on an optional peer it never
  calls. `src/index.ts` had the same shape from the other side.

### Changed

- The root barrel no longer re-exports `tasks/sqs-client`, `tasks/sqs-task-queue`
  or `tasks/sqs-task-worker`; those moved to `@ecom/messaging/tasks`. This is a
  removal from a public entry point and is called out rather than smoothed over.
  It breaks nothing in the platform: `task-queue.port.js` (types only, no SDK
  import) stays on the root, so cms-svc's `import type { TaskHandler,
  TaskQueuePort } from '@ecom/messaging'` is untouched, and every SQS **value**
  was already imported from `/nest/tasks`. Package-internal tests import from
  `src/` paths.

### Compatibility

Behaviour with no options passed is unchanged, and only the session timeout
moves — from 30s to 45s, which strictly _widens_ what a service can do, so no
pinned consumer can regress. `cart-svc` and `catalog-svc` stay on 0.3.0 until
their next wave; ADR 44 records the constraint they must meet when they move.

## 0.4.0

The task half. Created for `cms-006`: the platform had `TASK_QUEUES`,
`resizeImageJobSchema`, Terraform for the queues and a LocalStack init that
creates them, but **no client** — `cms-svc` published `media.tasks` on the Kafka
bus instead. This is that client.

### Added

- `TaskQueuePort` / `TaskWorkerPort` and their data types (`TaskDraft`,
  `TaskDelivery`, `TaskHandler`, `PayloadParser`, `TaskQueue`) in
  `src/tasks/task-queue.port.ts`. The ports take a `TASK_QUEUES` **entry** rather
  than a string, so a typo is a type error and the worker reads
  `visibilityTimeoutSeconds` from the same place the queue was created from.
- `SqsTaskQueue` — validates the envelope with `taskMessageSchema` before it
  sends, which catches the one mistake types cannot: a hand-built message whose
  `queue`/`type`/`idempotencyKey` disagrees with what the worker will check.
- `SqsTaskWorker` — long poll (20 s, batch 10), `delete` only after the handler
  resolves, no delete on failure. The DLQ is the queue's own redrive policy after
  `maxReceiveCount`, not a publisher here.
- `SqsQueueUrls` — `GetQueueUrl` once per queue, cached, with a `queueUrls`
  override for a deployment that forbids the call. A failed lookup is not
  cached.
- `createSqsTaskModule` (`@ecom/messaging/nest`) — starts and stops the poller
  through the Nest lifecycle, and registers no worker at all when
  `SQS_TASKS_ENABLED=false` or `NODE_ENV=test`. A handler is either a value or a
  `handlerToken` exported by one of `imports`, because a dynamic module cannot
  see the providers of the module that imports it — which is every handler that
  needs a database or a storage port.
- Narrow entry points `@ecom/messaging/nest/tasks` and
  `@ecom/messaging/nest/outbox`. The `/nest` barrel also re-exports
  `createKafkaConsumerStrategy`, which imports `@nestjs/microservices` eagerly,
  so a service that has no Kafka consumer (cms-svc after `cms-006`) could not
  import the outbox or task module without installing the Kafka transport. Both
  narrow entries are re-exported from `/nest`, so an existing import keeps
  working unchanged.
- `@aws-sdk/client-sqs` as an **optional** peer dependency, the same way
  `kafkajs` and `@nestjs/microservices` already are.

### Changed

- `SQS_TASKS_ENABLED` now wins over `NODE_ENV` in both directions. It used to be
  a kill switch only, which made the one suite that owns a real queue
  (`cms-svc/test/media-resize.e2e-spec.ts`) impossible to enable without faking
  `NODE_ENV`.
- `@ecom/contracts` dev dependency moves to `1.11.0` (the tarball that carries
  `taskMessageSchema`). The peer range `>=1.9.0 <2` already admitted it, so no
  consumer has to re-pin because of this release.

### Decisions

- **Delete-after-success, never a hand-rolled DLQ.** An SQS queue's redrive
  policy *is* the dead-letter path, and Terraform and the LocalStack init
  already configure it. A `dlqReason` publisher in the worker would be a second
  source of truth for the same five attempts. Rejected: mirroring the Kafka
  `KafkaEventHandler` model, where the transport must own the dead letter because
  Kafka has no redrive.
- **A malformed message is redelivered, not deleted.** It cannot succeed, but
  deleting it destroys the only evidence; five receives put it in the DLQ where
  a human looks. Rejected: deleting on `invalid` to keep the DLQ clean.
- **Sequential processing.** One message at a time per pod bounds memory on a
  1 GB box where each job is a `sharp` decode of a full-size image. Concurrency
  arrives by adding pods, which SQS supports natively. Rejected: an in-process
  worker pool sized by CPU count.
- **`PayloadParser` is structural, not `z.ZodType`.** This package still has no
  zod dependency, and a test can pass a three-line stub. Rejected: importing
  `zod` for one `parse` signature.

## 0.3.0

The consumer half. Created for `srch-002` (search-svc migrates its raw kafkajs
consumer to the Nest transport), `ca-009` (cart-svc consumes `order.created`)
and `cat-008` (catalog-svc consumes `product.imported`). Every consumer in the
platform now runs this protocol instead of its own copy of it.

### Added

- `createKafkaConsumerStrategy` (`@ecom/messaging/nest`) — the Nest Kafka server
  strategy, pre-configured with `postfixId: ""`, `run.autoCommit: false`,
  `parser.keepBinary: true` and a deserializer that hands the handler the raw
  JSON string. Each of those was read out of `@nestjs/microservices` 12.3.2
  rather than assumed; the three silent failure modes are documented on the
  function and pinned by tests.
- `KafkaEventHandler` (`@ecom/messaging/nest`) — `run(ctx, topic, raw, handle)`
  is the whole protocol: parse, retry `1/2/4/8/16s`, dead-letter on
  `invalid-*` / `transient-exhausted` / a service-supplied terminal reason,
  then commit. The invariant is **side effect → counter → offset**, so a record
  whose dead letter failed keeps its offset and is redelivered.
  `KafkaConsumerStalledError` is the single exception `run` throws, and only
  when the dead-letter write itself failed.
- `ProcessedEventsStore` — `claim(client, eventId, topic)` as
  `INSERT … ON CONFLICT (event_id) DO NOTHING RETURNING event_id`. Takes the
  caller's client so the claim and the domain write are one transaction.
- `KafkaDlqPublisher` + `dlqTopicNameFor` — lazy, shared producer; name from the
  registry with a `${topic}.dlq` fallback; resolves only once the broker has the
  record.
- `KafkaLagSampler` — standalone admin client, `high watermark − committed` per
  topic, unref'd 15s timer. A committed offset of `-1` means "never committed",
  not "infinitely behind". A broker that is unreachable at boot logs and leaves
  the sample missing rather than raising an unhandled rejection, because a lag
  gauge must never be a reason a process dies.
- `ConsumerMetrics` — `accepted | skipped | committed | duplicate | ignored |
  invalid | dlq | commit | stalled`, plus `recordCommit(topic, partition,
  offset)` and `lastCommit()`.

### Changed

- Peer range for `@ecom/contracts` is now `>=1.9.0 <2`: the consumer surface
  parses the 1.9.0 `product.imported` payload, which carries `variants`.
- `@nestjs/microservices ^12.0.1` is a new **optional** peer.

### Notes

- `accepted`, `skipped` and `committed` are three names for "it worked" because
  search-svc and catalog-svc already disagreed; a handler picks one via
  `{ kind: "applied", counter }`. Renaming them would have broken two dashboards
  for no gain.
- `run()` accepts a `@nestjs/microservices` `KafkaContext` or any object with
  `getMessage` / `getPartition` / `getConsumer` — the types are structural, so
  the package still does not import the framework outside `./nest`.

## 0.2.0

Second cut, created for `cat-007` (catalog-svc, `product.updated` /
`category.updated`), `cms-005` (cms-svc, `cms.updated` / `blog.updated`, MongoDB)
and `ord-011` (order-svc, `order.created` / `order.status.updated`).

### Added

- `MongoOutboxStore` — the same port for a Mongo-backed service: `write` through
  the caller's `ClientSession`, `claimBatch` as one atomic `findOneAndUpdate` per
  row (the `SKIP LOCKED` analogue), and `tryAcquireRelayLock` as a lease
  document in `<collection>_locks`, where a duplicate-key error on the upsert
  means "another worker holds it". `initialize()` creates the unique
  `event_id` index and the partial pending index.
- `createMongoOutboxModule` (`@ecom/messaging/nest`) — same relay, publisher,
  lifecycle and retry policy as `createOutboxModule`.
- `OutboxInitializer` — always registered, so `initialize()` runs even in an e2e
  suite where the relay is disabled and no lifecycle is wired.

### Changed

- `OutboxStorePort` is generic in its write context: `write(ctx, input)` where
  `Ctx` is a `Queryable` (Postgres) or a `ClientSession` (Mongo). `Ctx` defaults
  to `Queryable`, so every existing call site compiles unchanged; the relay asks
  for `OutboxStorePort<never>` because it never writes.
- `tryAcquireRelayLock(leaseSeconds?)` gained an optional lease hint. Postgres
  ignores it (advisory locks have no TTL); Mongo uses it. The relay passes four
  times its row lease so a slow batch cannot let a second worker interleave.
- Structural Mongo views (`MongoOutboxDb`, `MongoCollectionLike`,
  `MongoSessionLike`) keep the package free of `mongodb`/`mongoose`, the same
  way `PoolLike` keeps it free of `pg`.

### Fixed

- `PostgresOutboxStore` now quotes its identifiers. `order` is a reserved word,
  so `order.outbox` — the table `ord-011` needs — did not parse. Quoting is a
  no-op for `auth`, `cart` and `catalog`. The advisory-lock key stays unquoted,
  so existing lock namespaces are unchanged.
- `MongoOutboxStore` accepts both `findOneAndUpdate` result shapes. Driver 6.20
  — the one mongoose 8.24 ships — resolves to the document itself, while the
  6.x typings and earlier releases wrap it in `{ value }`. Reading only `value`
  made `claimBatch` return nothing and `markFailed` throw against a real
  server; the two integration-style tests added here pin both shapes.

## 0.1.0

First cut, created for `au-004` (auth-svc, `user.registered`) and `ca-008`
(cart-svc, `cart.updated`).

### Added

- `buildEventEnvelope` — validates the envelope and the payload against
  `@ecom/contracts` 1.8.0 and dual-writes `type`/`topic`/`eventType`.
- `PostgresOutboxStore` — `write` (caller's transaction client), `claimBatch`
  (`FOR UPDATE SKIP LOCKED` + `make_interval` lease), `markPublished`,
  `markFailed` (parked after 5 attempts) and `tryAcquireRelayLock`
  (`pg_try_advisory_lock`).
- `OutboxRelay` — sequential batch publish, per-attempt backoff, `drainOnce()`
  for tests, `start()`/`stop()` for the process lifecycle.
- `KafkaEventPublisher` — idempotent producer (`acks: -1`,
  `maxInFlightRequests: 1`) that **rejects** on failure so the relay retries.
- `RecordingPublisher`, `DiscardPublisher`, `ConsoleRelayLogger`,
  `RecordingLogger` for tests and disabled mode.
- `createOutboxModule` (`@ecom/messaging/nest`) — the whole wiring is one import
  in a service module.

### Known limits

- One relay publishes at a time per outbox; throughput is bounded by one batch
  in flight. Revisit when `ord-011` adds a third producer.
- No metrics endpoint yet (`failed` count, oldest pending age).
