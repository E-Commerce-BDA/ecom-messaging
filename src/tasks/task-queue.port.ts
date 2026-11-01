import type { TaskMessage, TASK_QUEUES } from "@ecom/contracts";

/**
 * One entry of `TASK_QUEUES` — the queue, its DLQ twin and its timing.
 *
 * Ports take this entry rather than a bare string for two reasons: the name can
 * only come from the registry (so a typo is a type error instead of a `QueueDoesNotExist`
 * at runtime), and the worker needs `visibilityTimeoutSeconds` from the same
 * place the queue was created from.
 */
export type TaskQueue = (typeof TASK_QUEUES)[keyof typeof TASK_QUEUES];

/**
 * What a producer supplies for one task.
 *
 * `id`, `enqueuedAt` and `queue` are deliberately absent: the transport mints
 * the first two, and the queue entry supplies the third. A caller that could
 * set `queue` itself could send a message to queue A whose body says B, which
 * the worker would only reject after it had already paid for the receive.
 */
export interface TaskDraft<P = unknown> {
  /** Dotted job name inside the queue, e.g. `media.resize-image`. */
  type: string;
  /**
   * Identity of the *effect*, not of the send. A re-drive and a retry keep it,
   * which is what lets the worker skip work it has already done.
   */
  idempotencyKey: string;
  payload: P;
}

/**
 * A payload parser the worker can trust.
 *
 * Structural rather than `z.ZodType` so this package does not depend on zod: a
 * zod schema satisfies it as-is, and a test can pass a three-line stub.
 */
export interface PayloadParser<P> {
  parse(input: unknown): P;
}

export interface TaskQueuePort {
  /**
   * Validate the envelope against `taskMessageSchema`, then send it.
   *
   * MUST reject when SQS did not accept the message — a resolved promise is
   * the only proof the task left the process.
   */
  enqueue<P>(queue: TaskQueue, draft: TaskDraft<P>): Promise<TaskMessage>;

  /** The queue URL, resolved once and cached. Exposed for diagnostics. */
  urlFor(queue: TaskQueue): Promise<string>;
}

/** What the handler is told about the delivery it is serving. */
export interface TaskDelivery {
  /** SQS `MessageId` — one per delivery, not per effect. */
  messageId: string;
  /** `ApproximateReceiveCount`: 1 on the first attempt, 5 on the last. */
  receiveCount: number;
  /** The validated envelope, including `idempotencyKey`. */
  message: TaskMessage;
}

/**
 * The job itself.
 *
 * Resolving means "this effect is applied" and the message is deleted. Throwing
 * means "not yet" — the message becomes visible again once the visibility
 * timeout expires, and the queue's redrive policy moves it to the DLQ after
 * `maxReceiveCount` attempts. A handler therefore must be safe to run twice.
 */
export type TaskHandler<P = unknown> = (
  payload: P,
  delivery: TaskDelivery,
) => Promise<void>;

export interface TaskWorkerPort<P = unknown> {
  /**
   * Start polling. Returns immediately; the loop runs until `stop()`.
   *
   * Calling it twice is a no-op with a warning, so a double `onModuleInit`
   * cannot open two pollers on the same queue.
   */
  run(handler: TaskHandler<P>): void;

  /** Stop polling and wait for the in-flight batch to finish. */
  stop(): Promise<void>;
}
