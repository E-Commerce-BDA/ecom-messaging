/**
 * Narrow consumer entry.                                       `srch-004` / 0.5.0
 *
 * The `/nest` barrel also re-exports the task module, and the task module
 * imports `@aws-sdk/client-sqs` eagerly — so a service with a Kafka consumer and
 * no SQS would fail at boot on an optional peer it never calls. This entry is the
 * mirror image of `./nest/outbox` and `./nest/tasks`, which exist for the same
 * reason pointing the other way: a service should load the transports it uses.
 *
 * `/nest` keeps re-exporting all of this, so an existing import path still works
 * for a service that has the SQS peer installed.
 */
export * from "../consumer/kafka-consumer.strategy.js";
export * from "../consumer/kafka-event-handler.js";
export * from "../consumer/processed-events.store.js";
export * from "../consumer/consumer-metrics.js";
export * from "../consumer/types.js";
