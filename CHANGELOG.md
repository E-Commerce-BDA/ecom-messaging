# Changelog

All notable changes to `@ecom/messaging` are recorded here. Versions follow
`Documentation/8.Versioning.md`; `0.x` until the third adopter (ord-011).

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
