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

// Task side. SQS is the command channel: no Nest type is needed for the port or
// the two implementations, so they live here; `createSqsTaskModule` is on
// `./nest` next to the outbox module.
export * from "./tasks/task-queue.port.js";
export * from "./tasks/sqs-client.js";
export * from "./tasks/sqs-task-queue.js";
export * from "./tasks/sqs-task-worker.js";
