export * from "./outbox.module.js";

// The consumer surface. `KafkaEventHandler` and `createKafkaConsumerStrategy`
// are here rather than in the root because both only make sense next to
// `@nestjs/microservices`; the rest is re-exported so a service can import
// everything it needs from one subpath.
export * from "../consumer/kafka-consumer.strategy.js";
export * from "../consumer/kafka-event-handler.js";
export * from "../consumer/processed-events.store.js";
export * from "../consumer/consumer-metrics.js";
export * from "../consumer/types.js";
