import "reflect-metadata";

import { DeleteMessageCommand, ReceiveMessageCommand } from "@aws-sdk/client-sqs";
import { TASK_QUEUES } from "@ecom/contracts";
import { afterEach, describe, expect, it } from "vitest";
import { createSqsTaskModule, isSqsTasksEnabled, SQS_TASK_QUEUE, SQS_TASK_WORKER, SqsTaskLifecycle } from "../../src/nest/tasks.module.js";
import { RecordingLogger } from "../../src/logger.js";
import type { SqsClientLike } from "../../src/tasks/sqs-client.js";

const QUEUE = TASK_QUEUES.mediaResizeImage;

class StubSqs implements SqsClientLike {
  readonly commands: string[] = [];

  async send(command: object): Promise<unknown> {
    this.commands.push(command.constructor.name);
    if (command instanceof ReceiveMessageCommand) {
      return { Messages: [] };
    }
    return command instanceof DeleteMessageCommand ? {} : {};
  }
}

afterEach(() => {
  delete process.env.SQS_TASKS_ENABLED;
  delete process.env.NODE_ENV;
});

describe("isSqsTasksEnabled", () => {
  it("is off in NODE_ENV=test so a suite without LocalStack does not poll", () => {
    process.env.NODE_ENV = "test";
    expect(isSqsTasksEnabled()).toBe(false);
  });

  it("is on outside test, and can be switched off explicitly", () => {
    process.env.NODE_ENV = "development";
    expect(isSqsTasksEnabled()).toBe(true);

    process.env.SQS_TASKS_ENABLED = "false";
    expect(isSqsTasksEnabled()).toBe(false);
  });

  it("lets the flag win over NODE_ENV=test, so one suite can own a real queue", () => {
    process.env.NODE_ENV = "test";
    process.env.SQS_TASKS_ENABLED = "true";

    expect(isSqsTasksEnabled()).toBe(true);
  });
});

describe("createSqsTaskModule", () => {
  it("declares the producer but no poller when the worker is disabled", () => {
    const module = createSqsTaskModule({
      queue: QUEUE,
      handler: async () => {},
      client: new StubSqs(),
      enabled: () => false,
    });

    const provided = module.providers?.map((provider) =>
      typeof provider === "object" && "provide" in provider ? provider.provide : provider,
    );

    expect(provided).toContain(SQS_TASK_QUEUE);
    expect(provided).not.toContain(SQS_TASK_WORKER);
    // An e2e suite that never started LocalStack must not leave a poller open.
    expect(module.exports).toEqual([SQS_TASK_QUEUE]);
  });

  it("provides the worker and the lifecycle when enabled", () => {
    const module = createSqsTaskModule({
      queue: QUEUE,
      handler: async () => {},
      client: new StubSqs(),
      enabled: () => true,
      logger: new RecordingLogger(),
    });

    const provided = module.providers?.map((provider) =>
      typeof provider === "object" && "provide" in provider ? provider.provide : provider,
    );

    expect(provided).toContain(SQS_TASK_WORKER);
    expect(module.exports).toContain(SQS_TASK_WORKER);
  });

  it("refuses to declare an enabled poller with no job", () => {
    expect(() =>
      createSqsTaskModule({
        queue: QUEUE,
        client: new StubSqs(),
        enabled: () => true,
      }),
    ).toThrow(/neither a handler nor a handlerToken/);
  });

  it("accepts a producer-only module when the worker is disabled", () => {
    expect(() =>
      createSqsTaskModule({
        queue: QUEUE,
        client: new StubSqs(),
        enabled: () => false,
      }),
    ).not.toThrow();
  });

  it("injects a handler token from `imports` instead of taking a value", () => {
    const module = createSqsTaskModule({
      queue: QUEUE,
      handlerToken: "MEDIA_RESIZE_HANDLER",
      imports: [],
      client: new StubSqs(),
      enabled: () => true,
      logger: new RecordingLogger(),
    });

    const lifecycle = module.providers?.find(
      (provider) =>
        typeof provider === "object" &&
        "provide" in provider &&
        provider.provide === SqsTaskLifecycle,
    ) as { inject?: unknown[] } | undefined;

    expect(lifecycle?.inject).toEqual([SQS_TASK_WORKER, "MEDIA_RESIZE_HANDLER"]);
  });
});
