/**
 * The framework-free SQS task implementations.                                 0.5.0
 *
 * They are not on the root barrel, because `@aws-sdk/client-sqs` is an optional
 * peer: `sqs-client.ts` and `sqs-task-worker.ts` import it at module scope, so a
 * service that re-exports the root barrel would fail at boot on a transport it
 * never uses. The port (`task-queue.port.js`) stays on the root barrel — it is
 * types only, and a service may name `TaskHandler` without installing the SDK.
 *
 * `./nest/tasks` is the Nest half of the same surface, and is the entry a service
 * with SQS actually wants.
 */
export * from "./task-queue.port.js";
export * from "./sqs-client.js";
export * from "./sqs-task-queue.js";
export * from "./sqs-task-worker.js";
