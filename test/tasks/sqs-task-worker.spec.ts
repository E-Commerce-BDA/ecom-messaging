import {
  DeleteMessageCommand,
  GetQueueUrlCommand,
  ReceiveMessageCommand,
} from "@aws-sdk/client-sqs";
import {
  TASK_QUEUES,
  resizeImageJobSchema,
  type ResizeImageJob,
  type TaskMessage,
} from "@ecom/contracts";
import { describe, expect, it } from "vitest";
import { RecordingLogger } from "../../src/logger.js";
import { SqsQueueUrls, type SqsClientLike } from "../../src/tasks/sqs-client.js";
import { SqsTaskWorker } from "../../src/tasks/sqs-task-worker.js";
import type { PayloadParser, TaskHandler } from "../../src/tasks/task-queue.port.js";

const QUEUE = TASK_QUEUES.mediaResizeImage;
const QUEUE_URL = "http://localhost:4566/000000000000/media-resize-image";
const MEDIA_ID = "0190c9f4-0000-7000-8000-000000000003";

interface Received {
  MessageId?: string;
  ReceiptHandle?: string;
  Body?: string;
  Attributes?: Record<string, string>;
}

/**
 * Hands out the queued batches in order, then empties; `drained` resolves once
 * one receive has come back empty, which is the signal that the batch was
 * processed.
 *
 * Once drained, an empty receive costs a 1 ms timer instead of a microtask, so
 * a spinning loop cannot starve the test's own continuation before `stop()`.
 */
class StubSqs implements SqsClientLike {
  readonly deleted: string[] = [];
  readonly drained: Promise<void>;
  private markDrained!: () => void;
  private drainedFlag = false;
  private readonly batches: Received[][];

  constructor(batches: Received[][]) {
    this.batches = [...batches];
    this.drained = new Promise<void>((resolve) => {
      this.markDrained = resolve;
    });
  }

  async send(command: object): Promise<unknown> {
    if (command instanceof GetQueueUrlCommand) {
      return { QueueUrl: QUEUE_URL };
    }

    if (command instanceof ReceiveMessageCommand) {
      const batch = this.batches.shift() ?? [];
      if (batch.length === 0) {
        this.drainedFlag = true;
        this.markDrained();
        await new Promise((resolve) => setTimeout(resolve, 1));
      }

      return { Messages: batch };
    }

    if (command instanceof DeleteMessageCommand) {
      this.deleted.push(command.input.ReceiptHandle ?? "");
      return {};
    }

    return {};
  }
}

function envelope(overrides: Partial<TaskMessage> = {}): TaskMessage {
  return {
    id: "0190c9f4-0000-7000-8000-000000000001",
    queue: QUEUE.queue,
    type: "media.resize-image",
    idempotencyKey: MEDIA_ID,
    enqueuedAt: "2026-11-02T10:00:00.000Z",
    payload: {
      mediaId: MEDIA_ID,
      sourceKey: `media/2026/10/${MEDIA_ID}.png`,
      formats: ["webp"],
      widths: [320],
    },
    ...overrides,
  };
}

function message(
  body: unknown,
  options: { receiveCount?: number; receipt?: string } = {},
): Received {
  return {
    MessageId: "msg-1",
    ReceiptHandle: options.receipt ?? "receipt-1",
    Body: typeof body === "string" ? body : JSON.stringify(body),
    Attributes: {
      ApproximateReceiveCount: String(options.receiveCount ?? 1),
    },
  };
}

function build(
  client: SqsClientLike,
  options: { payloadSchema?: PayloadParser<ResizeImageJob>; logger?: RecordingLogger } = {},
) {
  const logger = options.logger ?? new RecordingLogger();
  return {
    logger,
    worker: new SqsTaskWorker<ResizeImageJob>({
      queue: QUEUE,
      client,
      urls: new SqsQueueUrls(client, { logger }),
      logger,
      payloadSchema: options.payloadSchema,
      // Long polling would make every empty batch wait 20 s.
      waitTimeSeconds: 0,
      errorBackoffMs: 1,
    }),
  };
}

async function drain(
  client: StubSqs,
  handler: TaskHandler<ResizeImageJob>,
  options: { payloadSchema?: PayloadParser<ResizeImageJob>; logger?: RecordingLogger } = {},
) {
  const { worker, logger } = build(client, options);
  worker.run(handler);
  await client.drained;
  await worker.stop();
  return logger;
}

const neverRuns: TaskHandler<ResizeImageJob> = async () => {
  throw new Error("handler must not run for this message");
};

describe("SqsTaskWorker", () => {
  it("deletes the message only after the handler resolves", async () => {
    const client = new StubSqs([[message(envelope())]]);
    const seen: { payload: ResizeImageJob; count: number }[] = [];

    await drain(client, async (payload, delivery) => {
      expect(client.deleted).toEqual([]);
      seen.push({ payload, count: delivery.receiveCount });
    });

    expect(seen).toEqual([
      {
        payload: {
          mediaId: MEDIA_ID,
          sourceKey: `media/2026/10/${MEDIA_ID}.png`,
          formats: ["webp"],
          widths: [320],
        },
        count: 1,
      },
    ]);
    expect(client.deleted).toEqual(["receipt-1"]);
  });

  it("keeps the message when the handler throws, so the redrive can act", async () => {
    const client = new StubSqs([[message(envelope())]]);

    const logger = await drain(client, async () => {
      throw new Error("sharp exploded");
    });

    expect(client.deleted).toEqual([]);
    expect(
      logger.lines.some(
        (line) =>
          line.level === "warn" &&
          line.message.includes("sharp exploded") &&
          line.message.includes(`receive 1/${QUEUE.maxReceiveCount}`),
      ),
    ).toBe(true);
  });

  it("never deletes a body that is not JSON, and names the receive count", async () => {
    const client = new StubSqs([[message("not json at all", { receiveCount: 4 })]]);

    const logger = await drain(client, neverRuns);

    expect(client.deleted).toEqual([]);
    const errors = logger.lines.filter((line) => line.level === "error");
    expect(errors[0]?.message).toMatch(/invalid envelope: body is not JSON/);
    expect(errors[0]?.message).toMatch(/4\/5/);
  });

  it("refuses an envelope that names another queue", async () => {
    const client = new StubSqs([
      [message(envelope({ queue: TASK_QUEUES.importProcessRow.queue }))],
    ]);

    const logger = await drain(client, neverRuns);

    expect(client.deleted).toEqual([]);
    expect(
      logger.lines.some((line) =>
        line.message.includes("envelope names queue import-process-row"),
      ),
    ).toBe(true);
  });

  it("validates the payload against the job schema before the handler runs", async () => {
    const client = new StubSqs([
      [message(envelope({ payload: { mediaId: "only-a-media-id" } }))],
    ]);

    let called = false;
    const logger = await drain(
      client,
      async () => {
        called = true;
      },
      { payloadSchema: resizeImageJobSchema },
    );

    expect(called).toBe(false);
    expect(client.deleted).toEqual([]);
    expect(
      logger.lines.some((line) =>
        line.message.startsWith("invalid payload for media.resize-image"),
      ),
    ).toBe(true);
  });

  it("processes a batch in order and deletes each message once", async () => {
    const client = new StubSqs([
      [
        message({ ...envelope({ id: "0190c9f4-0000-7000-8000-00000000000a" }) }, { receipt: "r-a" }),
        message({ ...envelope({ id: "0190c9f4-0000-7000-8000-00000000000b" }) }, { receipt: "r-b" }),
        message({ ...envelope({ id: "0190c9f4-0000-7000-8000-00000000000c" }) }, { receipt: "r-c" }),
      ],
    ]);

    const order: string[] = [];
    await drain(client, async (_payload, delivery) => {
      order.push(delivery.message.id);
    });

    expect(order).toEqual([
      "0190c9f4-0000-7000-8000-00000000000a",
      "0190c9f4-0000-7000-8000-00000000000b",
      "0190c9f4-0000-7000-8000-00000000000c",
    ]);
    expect(client.deleted).toEqual(["r-a", "r-b", "r-c"]);
  });

  it("passes the idempotency key through so the handler can dedupe", async () => {
    const client = new StubSqs([[message(envelope())]]);
    let key = "";

    await drain(client, async (_payload, delivery) => {
      key = delivery.message.idempotencyKey;
    });

    expect(key).toBe(MEDIA_ID);
  });

  it("stops polling after a failed delete instead of throwing out of the loop", async () => {
    class DeleteFails extends StubSqs {
      override async send(command: object): Promise<unknown> {
        if (command instanceof DeleteMessageCommand) {
          throw new Error("ReceiptHandle is invalid");
        }

        return super.send(command);
      }
    }

    const client = new DeleteFails([[message(envelope())]]);
    const logger = new RecordingLogger();
    const { worker } = build(client, { logger });

    worker.run(async () => {});
    await client.drained;
    await worker.stop();

    expect(
      logger.lines.some((line) =>
        line.message.includes("but could not delete message msg-1"),
      ),
    ).toBe(true);
  });

  it("ignores a second run() so a double init cannot open two pollers", async () => {
    const client = new StubSqs([]);
    const { worker, logger } = build(client);

    worker.run(async () => {});
    worker.run(async () => {});
    await client.drained;
    await worker.stop();

    expect(
      logger.lines.some((line) =>
        line.message.includes("is already running; ignoring run()"),
      ),
    ).toBe(true);
  });
});
