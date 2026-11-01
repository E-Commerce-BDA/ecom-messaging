import { DeleteMessageCommand, ReceiveMessageCommand } from "@aws-sdk/client-sqs";
import { taskMessageSchema, type TaskMessage } from "@ecom/contracts";
import { ConsoleRelayLogger, type RelayLogger } from "../logger.js";
import type { SqsClientLike, SqsQueueUrls } from "./sqs-client.js";
import type {
  PayloadParser,
  TaskDelivery,
  TaskHandler,
  TaskQueue,
  TaskWorkerPort,
} from "./task-queue.port.js";

export interface SqsTaskWorkerOptions<P = unknown> {
  queue: TaskQueue;
  urls: SqsQueueUrls;
  client: SqsClientLike;
  /**
   * Parses `message.payload` once the envelope has passed validation.
   *
   * Without it the handler receives `unknown`, which is what a caller that
   * already trusts its own producer wants; with it a payload that drifted from
   * `jobs.ts` is caught here, before the handler touches storage.
   */
  payloadSchema?: PayloadParser<P>;
  logger?: RelayLogger;
  /** Long poll. 20 s is the SQS maximum and the cheapest way to be idle. */
  waitTimeSeconds?: number;
  maxMessages?: number;
  /** Backoff after a failed `ReceiveMessage`. Default 1 s, doubling to 30 s. */
  errorBackoffMs?: number;
}

interface ReceivedMessage {
  MessageId?: string;
  ReceiptHandle?: string;
  Body?: string;
  Attributes?: Record<string, string>;
}

/**
 * The consumer half of the task queue.
 *
 * The protocol is four rules, and every one of them is about not losing a task:
 *
 * 1. **Delete only after the handler resolves.** A delete is the ack, so a
 *    crash mid-handler leaves the message to be redelivered.
 * 2. **Never delete on failure.** The message becomes visible again when its
 *    visibility timeout expires, and the queue's redrive policy moves it to the
 *    DLQ after `maxReceiveCount`. The DLQ publisher is deliberately *not* here:
 *    Terraform (`ecom-infra/aws/sqs.tf`) and the LocalStack init already own the
 *    redrive policy, and a second copy would drift from it.
 * 3. **A malformed message is a failure, not a skip.** It cannot succeed on a
 *    retry, but deleting it would destroy the only evidence; five receives put
 *    it in the DLQ where a human looks.
 * 4. **One message at a time.** Processing is sequential per pod, so a `sharp`
 *    worker on a 1 GB box cannot hold ten decodes at once. Scale out by adding
 *    pods — SQS hands each visible message to one receiver.
 */
export class SqsTaskWorker<P = unknown> implements TaskWorkerPort<P> {
  private readonly logger: RelayLogger;
  private readonly waitTimeSeconds: number;
  private readonly maxMessages: number;
  private readonly errorBackoffMs: number;

  private running = false;
  private loop: Promise<void> | null = null;
  private handler: TaskHandler<P> | null = null;

  constructor(private readonly options: SqsTaskWorkerOptions<P>) {
    this.logger = options.logger ?? new ConsoleRelayLogger("sqs.task.worker");
    this.waitTimeSeconds = options.waitTimeSeconds ?? 20;
    this.maxMessages = options.maxMessages ?? 10;
    this.errorBackoffMs = options.errorBackoffMs ?? 1000;
  }

  run(handler: TaskHandler<P>): void {
    if (this.running) {
      this.logger.warn(
        `worker for ${this.options.queue.queue} is already running; ignoring run()`,
      );
      return;
    }

    this.handler = handler;
    this.running = true;
    this.loop = this.poll();
    this.logger.debug(`worker for ${this.options.queue.queue} started`);
  }

  async stop(): Promise<void> {
    this.running = false;

    if (this.loop) {
      await this.loop;
      this.loop = null;
    }

    this.logger.debug(`worker for ${this.options.queue.queue} stopped`);
  }

  private async poll(): Promise<void> {
    let backoff = this.errorBackoffMs;

    while (this.running) {
      try {
        const batch = await this.receive();
        backoff = this.errorBackoffMs;

        for (const message of batch) {
          // A stop() mid-batch must not orphan the rest: they are not deleted,
          // so they simply reappear once the visibility timeout expires.
          if (!this.running) {
            break;
          }

          await this.deliver(message);
        }
      } catch (error) {
        this.logger.error(
          `receive failed on ${this.options.queue.queue}; retrying in ${backoff} ms`,
          error,
        );
        await this.sleep(backoff);
        backoff = Math.min(backoff * 2, 30_000);
      }
    }
  }

  private async receive(): Promise<ReceivedMessage[]> {
    const queue = this.options.queue;
    const queueUrl = await this.options.urls.url(queue);

    const output = (await this.options.client.send(
      new ReceiveMessageCommand({
        QueueUrl: queueUrl,
        MaxNumberOfMessages: this.maxMessages,
        WaitTimeSeconds: this.waitTimeSeconds,
        // Restated per receive rather than trusted from the queue attribute: a
        // queue edited by hand is the one case where this worker would silently
        // retry a slow job too early.
        VisibilityTimeout: queue.visibilityTimeoutSeconds,
        MessageAttributeNames: ["All"],
        AttributeNames: ["All"],
      }),
    )) as { Messages?: ReceivedMessage[] };

    return output.Messages ?? [];
  }

  private async deliver(message: ReceivedMessage): Promise<void> {
    const receiveCount = Number(
      message.Attributes?.ApproximateReceiveCount ?? "1",
    );

    let envelope: TaskMessage;
    let payload: P;
    try {
      envelope = this.parseEnvelope(message);
      payload = this.parsePayload(envelope);
    } catch (error) {
      // Rule 3: leave it for the redrive. The log line is what explains the
      // DLQ entry the operator is about to find.
      this.logger.error(
        `${(error as Error).message} — message ${message.MessageId ?? "unknown"} ` +
          `on ${this.options.queue.queue} will be redelivered ` +
          `(${receiveCount}/${this.options.queue.maxReceiveCount})`,
      );
      return;
    }

    const delivery: TaskDelivery = {
      messageId: message.MessageId ?? "unknown",
      receiveCount,
      message: envelope,
    };

    try {
      // `handler` is set by run() before the first poll, and poll() never
      // outlives it, so the optional call is only here for the compiler.
      await this.handler?.(payload, delivery);
    } catch (error) {
      this.logger.warn(
        `task ${envelope.type} (${envelope.idempotencyKey}) failed on ` +
          `${this.options.queue.queue}, receive ${receiveCount}/` +
          `${this.options.queue.maxReceiveCount}: ${(error as Error).message}`,
      );
      return;
    }

    await this.delete(message, envelope);
  }

  private parseEnvelope(message: ReceivedMessage): TaskMessage {
    let raw: unknown;
    try {
      raw = JSON.parse(message.Body ?? "");
    } catch {
      throw new Error("invalid envelope: body is not JSON");
    }

    const parsed = taskMessageSchema.safeParse(raw);
    if (!parsed.success) {
      throw new Error(
        `invalid envelope: ${parsed.error.issues
          .map((issue) => `${issue.path.join(".")}: ${issue.message}`)
          .join("; ")}`,
      );
    }

    // A message addressed to another queue means two environments share a
    // broker: refusing it (rather than processing it) is the only outcome that
    // does not silently apply another service's job.
    if (parsed.data.queue !== this.options.queue.queue) {
      throw new Error(
        `envelope names queue ${parsed.data.queue}, received on ${this.options.queue.queue}`,
      );
    }

    return parsed.data;
  }

  private parsePayload(envelope: TaskMessage): P {
    if (!this.options.payloadSchema) {
      return envelope.payload as P;
    }

    try {
      return this.options.payloadSchema.parse(envelope.payload);
    } catch (error) {
      // A drifted payload is delivered again so it reaches the DLQ with the
      // rest of its failures; the reason is in the log line above it.
      throw new Error(
        `invalid payload for ${envelope.type}: ${(error as Error).message}`,
      );
    }
  }

  private async delete(
    message: ReceivedMessage,
    envelope: TaskMessage,
  ): Promise<void> {
    if (!message.ReceiptHandle) {
      this.logger.warn(
        `SQS returned message ${message.MessageId ?? "unknown"} without a ` +
          `receipt handle; it will be redelivered`,
      );
      return;
    }

    try {
      await this.options.client.send(
        new DeleteMessageCommand({
          QueueUrl: await this.options.urls.url(this.options.queue),
          ReceiptHandle: message.ReceiptHandle,
        }),
      );
    } catch (error) {
      // The handler already ran, so the effect is applied; the redelivery is
      // absorbed by the handler's idempotency key, not by a second write here.
      this.logger.error(
        `applied ${envelope.type} (${envelope.idempotencyKey}) but could not ` +
          `delete message ${message.MessageId ?? "unknown"}; it will be redelivered`,
        error,
      );
    }
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms).unref());
  }
}
