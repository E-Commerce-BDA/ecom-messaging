import { randomUUID } from "node:crypto";
import { SendMessageCommand } from "@aws-sdk/client-sqs";
import { taskMessageSchema, type TaskMessage } from "@ecom/contracts";
import { ConsoleRelayLogger, type RelayLogger } from "../logger.js";
import type { SqsClientLike, SqsQueueUrls } from "./sqs-client.js";
import type { TaskDraft, TaskQueue, TaskQueuePort } from "./task-queue.port.js";

export interface SqsTaskQueueOptions {
  client: SqsClientLike;
  urls: SqsQueueUrls;
  logger?: RelayLogger;
  /** Injectable so a test can assert the envelope without freezing time. */
  now?: () => Date;
  idFactory?: () => string;
}

/**
 * The producer half of the task queue.
 *
 * It validates the envelope it is about to send with `taskMessageSchema`,
 * which catches the one mistake a type system cannot: a caller that builds a
 * `TaskMessage` by hand and gets `queue`, `type` or `idempotencyKey` wrong. The
 * consumer would then have to dead-letter a record the producer already
 * believed it had sent.
 */
export class SqsTaskQueue implements TaskQueuePort {
  private readonly logger: RelayLogger;
  private readonly now: () => Date;
  private readonly idFactory: () => string;

  constructor(private readonly options: SqsTaskQueueOptions) {
    this.logger = options.logger ?? new ConsoleRelayLogger("sqs.task.queue");
    this.now = options.now ?? (() => new Date());
    this.idFactory = options.idFactory ?? randomUUID;
  }

  urlFor(queue: TaskQueue): Promise<string> {
    return this.options.urls.url(queue);
  }

  async enqueue<P>(queue: TaskQueue, draft: TaskDraft<P>): Promise<TaskMessage> {
    const message: TaskMessage = {
      id: this.idFactory(),
      queue: queue.queue,
      type: draft.type,
      idempotencyKey: draft.idempotencyKey,
      enqueuedAt: this.now().toISOString(),
      payload: draft.payload,
    };

    const parsed = taskMessageSchema.safeParse(message);
    if (!parsed.success) {
      // The producer's own bug, not a broker problem: surface it as a throw so
      // the caller fails where the mistake is, not later in a DLQ.
      throw new Error(
        `Refusing to enqueue an invalid task for ${queue.queue}: ` +
          `${parsed.error.issues.map((issue) => `${issue.path.join(".")}: ${issue.message}`).join("; ")}`,
      );
    }

    const queueUrl = await this.options.urls.url(queue);
    const output = (await this.options.client.send(
      new SendMessageCommand({
        QueueUrl: queueUrl,
        // The body IS the envelope: one JSON document, no SQS attributes to
        // drift out of step with what the worker validates.
        MessageBody: JSON.stringify(parsed.data),
        MessageAttributes: {
          // Duplicated out of the body only because a queue policy or a
          // CloudWatch alarm filters on attributes, not on body content.
          idempotencyKey: {
            DataType: "String",
            StringValue: draft.idempotencyKey,
          },
          type: { DataType: "String", StringValue: draft.type },
        },
      }),
    )) as { MessageId?: string };

    this.logger.debug(
      `enqueued ${draft.type} to ${queue.queue} ` +
        `(message ${output.MessageId ?? "unknown"}, key ${draft.idempotencyKey})`,
    );

    return parsed.data;
  }
}
