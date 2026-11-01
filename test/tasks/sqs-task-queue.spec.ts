import { GetQueueUrlCommand, SendMessageCommand } from "@aws-sdk/client-sqs";
import { TASK_QUEUES, taskMessageSchema } from "@ecom/contracts";
import { describe, expect, it } from "vitest";
import { RecordingLogger } from "../../src/logger.js";
import { SqsQueueUrls, type SqsClientLike } from "../../src/tasks/sqs-client.js";
import { SqsTaskQueue } from "../../src/tasks/sqs-task-queue.js";

const QUEUE = TASK_QUEUES.mediaResizeImage;
const QUEUE_URL = "http://localhost:4566/000000000000/media-resize-image";

class StubSqs implements SqsClientLike {
  readonly sent: { command: object; input: unknown }[] = [];

  constructor(private readonly url: string | null = QUEUE_URL) {}

  async send(command: object): Promise<unknown> {
    const input = (command as { input: unknown }).input;
    this.sent.push({ command, input });

    if (command instanceof GetQueueUrlCommand) {
      return this.url ? { QueueUrl: this.url } : {};
    }

    return { MessageId: "11111111-2222-3333-4444-555555555555" };
  }

  get sends(): SendMessageCommand[] {
    return this.sent
      .map((entry) => entry.command)
      .filter((command): command is SendMessageCommand => command instanceof SendMessageCommand);
  }
}

function build(client: SqsClientLike, logger = new RecordingLogger()) {
  const urls = new SqsQueueUrls(client, { logger });
  const queue = new SqsTaskQueue({
    client,
    urls,
    logger,
    now: () => new Date("2026-11-02T10:00:00.000Z"),
    idFactory: () => "0190c9f4-0000-7000-8000-000000000001",
  });
  return { queue, logger, urls };
}

const draft = {
  type: "media.resize-image",
  idempotencyKey: "0190c9f4-0000-7000-8000-000000000003",
  payload: {
    mediaId: "0190c9f4-0000-7000-8000-000000000003",
    sourceKey: "media/2026/10/0190c9f4-0000-7000-8000-000000000003.png",
    formats: ["webp" as const],
    widths: [320, 960],
  },
};

describe("SqsTaskQueue", () => {
  it("sends the envelope as the message body, addressed to the registry's queue", async () => {
    const client = new StubSqs();
    const { queue } = build(client);

    const sent = await queue.enqueue(QUEUE, draft);

    expect(client.sends).toHaveLength(1);
    const input = client.sends[0]!.input;
    expect(input.QueueUrl).toBe(QUEUE_URL);

    const body = JSON.parse(input.MessageBody as string) as unknown;
    expect(taskMessageSchema.parse(body)).toEqual(sent);
    expect(body).toMatchObject({
      id: "0190c9f4-0000-7000-8000-000000000001",
      queue: QUEUE.queue,
      type: draft.type,
      idempotencyKey: draft.idempotencyKey,
      enqueuedAt: "2026-11-02T10:00:00.000Z",
      payload: draft.payload,
    });
  });

  it("takes the queue from the registry, not from the caller", async () => {
    const client = new StubSqs();
    const { queue } = build(client);

    // A draft has no `queue` field at all: the type system refuses one, and the
    // body is built from TASK_QUEUES. Both halves of this test exist because a
    // mismatch is only visible to the worker after it has paid for the receive.
    const sent = await queue.enqueue(QUEUE, draft);

    expect(sent.queue).toBe("media-resize-image");
  });

  it("mirrors the identity onto SQS attributes for queue-side filtering", async () => {
    const client = new StubSqs();
    const { queue } = build(client);

    await queue.enqueue(QUEUE, draft);

    const attributes = client.sends[0]!.input.MessageAttributes!;
    expect(attributes.idempotencyKey).toEqual({
      DataType: "String",
      StringValue: draft.idempotencyKey,
    });
    expect(attributes.type).toEqual({
      DataType: "String",
      StringValue: draft.type,
    });
  });

  it("resolves the queue URL once and caches it", async () => {
    const client = new StubSqs();
    const { queue } = build(client);

    await queue.enqueue(QUEUE, draft);
    await queue.enqueue(QUEUE, draft);
    await queue.urlFor(QUEUE);

    const lookups = client.sent.filter(
      (entry) => entry.command instanceof GetQueueUrlCommand,
    );
    expect(lookups).toHaveLength(1);
    expect(client.sends).toHaveLength(2);
  });

  it("rejects rather than sending an envelope it cannot validate", async () => {
    const client = new StubSqs();
    const { queue } = build(client);

    await expect(
      queue.enqueue(QUEUE, { ...draft, type: "" }),
    ).rejects.toThrow(/Refusing to enqueue an invalid task/);
    expect(client.sends).toHaveLength(0);
  });

  it("surfaces a parent-queue failure instead of claiming success", async () => {
    const client: SqsClientLike = {
      send: async (command) => {
        if (command instanceof GetQueueUrlCommand) {
          return { QueueUrl: QUEUE_URL };
        }
        throw new Error("QueueDoesNotExist");
      },
    };
    const { queue } = build(client);

    await expect(queue.enqueue(QUEUE, draft)).rejects.toThrow(
      "QueueDoesNotExist",
    );
  });

  it("does not cache a failed URL lookup", async () => {
    let lookups = 0;
    const client: SqsClientLike = {
      send: async (command) => {
        if (!(command instanceof GetQueueUrlCommand)) {
          return { MessageId: "x" };
        }

        lookups += 1;
        return lookups === 1 ? {} : { QueueUrl: QUEUE_URL };
      },
    };
    const urls = new SqsQueueUrls(client, { logger: new RecordingLogger() });

    await expect(urls.url(QUEUE)).rejects.toThrow(/did not return a URL/);
    await expect(urls.url(QUEUE)).resolves.toBe(QUEUE_URL);
  });

  it("prefers a supplied queue URL over GetQueueUrl", async () => {
    const client = new StubSqs();
    const urls = new SqsQueueUrls(client, {
      queueUrls: { [QUEUE.queue]: "http://sqs.local/media-resize-image" },
      logger: new RecordingLogger(),
    });

    await expect(urls.url(QUEUE)).resolves.toBe("http://sqs.local/media-resize-image");
    expect(client.sent).toHaveLength(0);
  });
});
