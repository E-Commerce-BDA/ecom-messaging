# Changelog

All notable changes to `@ecom/messaging` are recorded here. Versions follow
`Documentation/8.Versioning.md`; `0.x` until the third adopter (ord-011).

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
