export * from "./outbox.module.js";
export * from "./tasks.module.js";

// The consumer surface, re-exported from its own narrow entry so a service that
// has no SQS peer can import it without loading `tasks.module.js`.
export * from "./consumer.js";
