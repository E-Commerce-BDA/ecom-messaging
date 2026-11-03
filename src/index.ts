export * from "./logger.js";
export * from "./envelope.js";

export * from "./publishers/publisher.port.js";
export * from "./publishers/kafka.publisher.js";
export * from "./publishers/recording.publisher.js";
export * from "./publishers/discard.publisher.js";

export * from "./outbox/types.js";
export * from "./outbox/postgres-outbox.store.js";
export * from "./outbox/mongo-outbox.store.js";
export * from "./outbox/outbox.relay.js";

// Consumer side. The two framework-free halves — dead letters and lag — live
// here because neither has a Nest type; the strategy and the protocol handler
// are exported from `./nest`, where the Nest peer is already assumed.
export * from "./consumer/types.js";
export * from "./consumer/dlq.publisher.js";
export * from "./consumer/lag.sampler.js";
export * from "./consumer/consumer-metrics.js";
export * from "./consumer/processed-events.store.js";

// Task side. Only the port is here: it is types and a queue-name table, so a
// service can name `TaskHandler` or `TaskQueuePort` without the SQS SDK in its
// tree. The implementations import `@aws-sdk/client-sqs` at module scope, and an
// optional peer must not be loaded by an entry point that does not use it — they
// are on `./tasks`, and `createSqsTaskModule` is on `./nest` next to the outbox
// module.
export * from "./tasks/task-queue.port.js";
