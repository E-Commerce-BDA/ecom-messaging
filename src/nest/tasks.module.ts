import {
  Injectable,
  Module,
  type DynamicModule,
  type InjectionToken,
  type OnModuleDestroy,
  type OnModuleInit,
  type Provider,
} from "@nestjs/common";
import { ConsoleRelayLogger, type RelayLogger } from "../logger.js";
import {
  createSqsClient,
  SqsQueueUrls,
  type SqsClientLike,
} from "../tasks/sqs-client.js";
import { SqsTaskQueue } from "../tasks/sqs-task-queue.js";
import { SqsTaskWorker, type SqsTaskWorkerOptions } from "../tasks/sqs-task-worker.js";
import type {
  PayloadParser,
  TaskHandler,
  TaskQueue,
  TaskWorkerPort,
} from "../tasks/task-queue.port.js";

export const SQS_TASK_QUEUE = Symbol("SQS_TASK_QUEUE");
export const SQS_TASK_WORKER = Symbol("SQS_TASK_WORKER");

export interface SqsTaskModuleOptions<P = unknown> {
  /** The `TASK_QUEUES` entry this module owns. One module per queue. */
  queue: TaskQueue;
  /**
   * The job, as a value. Resolving deletes the message; throwing leaves it for
   * a retry. Use `handlerToken` instead when the handler needs injected
   * dependencies.
   */
  handler?: TaskHandler<P>;
  /**
   * The job, as a provider token exported by one of `imports`.
   *
   * A dynamic module's providers cannot see the providers of the module that
   * imports it, so a handler that needs a database or a storage port is
   * published by a small module that this one imports. `MediaResizeModule` in
   * cms-svc is the working example.
   */
  handlerToken?: InjectionToken;
  /** Modules that export `handlerToken`. Ignored when `handler` is a value. */
  imports?: DynamicModule["imports"];
  /**
   * `resizeImageJobSchema`-style parser for `message.payload`. Without it the
   * handler receives `unknown`.
   */
  payloadSchema?: PayloadParser<P>;
  /** Supply one to share a client between the producer and the worker. */
  client?: SqsClientLike;
  region?: string;
  endpoint?: string;
  /** Pre-resolved queue URLs, keyed by queue name, to skip `GetQueueUrl`. */
  queueUrls?: Partial<Record<string, string>>;
  logger?: RelayLogger;
  waitTimeSeconds?: number;
  maxMessages?: number;
  /**
   * Defaults to `isSqsTasksEnabled()`.
   *
   * Evaluated once, when the module is declared — the same rule the outbox
   * relay follows, so a suite that never started LocalStack does not poll.
   */
  enabled?: () => boolean;
}

/**
 * Whether a poller should start.
 *
 * Precedence matters: the environment default is "on everywhere except a test
 * run", and an explicit `SQS_TASKS_ENABLED` overrides it in both directions —
 * `false` to keep a developer's machine quiet, `true` so the one suite that
 * owns a real queue can opt in without faking `NODE_ENV`.
 */
export function isSqsTasksEnabled(): boolean {
  if (process.env.SQS_TASKS_ENABLED === "false") {
    return false;
  }

  if (process.env.SQS_TASKS_ENABLED === "true") {
    return true;
  }

  return process.env.NODE_ENV !== "test";
}

/**
 * Starts and stops the poller with the Nest lifecycle.
 *
 * Registered only when the worker is enabled, so `app.close()` in a test has
 * nothing to stop and a disabled worker cannot hold the process open.
 */
@Injectable()
export class SqsTaskLifecycle implements OnModuleInit, OnModuleDestroy {
  constructor(
    private readonly worker: TaskWorkerPort<never>,
    private readonly handler: TaskHandler<never>,
  ) {}

  onModuleInit(): void {
    this.worker.run(this.handler);
  }

  async onModuleDestroy(): Promise<void> {
    await this.worker.stop();
  }
}

@Module({})
export class SqsTasksModule {}

/**
 * Builds the task module for one queue: a producer and a poller.
 *
 * A service that only enqueues still gets the producer (it is the same object),
 * and a service that only consumes still gets it for `urlFor`. Splitting them
 * would have produced two modules with the same options and one shared client.
 */
export function createSqsTaskModule<P>(
  options: SqsTaskModuleOptions<P>,
): DynamicModule {
  const logger = options.logger ?? new ConsoleRelayLogger(`sqs:${options.queue.queue}`);
  const enabled = (options.enabled ?? isSqsTasksEnabled)();

  // A poller with no job would receive messages and drop them, so an enabled
  // module without one is a declaration-time (boot) failure. A disabled module
  // is allowed to be producer-only.
  if (enabled && !options.handler && !options.handlerToken) {
    throw new Error(
      `createSqsTaskModule(${options.queue.queue}) is enabled but has neither a handler nor a handlerToken`,
    );
  }
  const client = options.client ?? createSqsClient({
    region: options.region,
    endpoint: options.endpoint,
  });
  const urls = new SqsQueueUrls(client, {
    queueUrls: options.queueUrls,
    logger,
  });

  const workerOptions: SqsTaskWorkerOptions<P> = {
    queue: options.queue,
    client,
    urls,
    payloadSchema: options.payloadSchema,
    logger,
    waitTimeSeconds: options.waitTimeSeconds,
    maxMessages: options.maxMessages,
  };

  const providers: Provider[] = [
    {
      provide: SQS_TASK_QUEUE,
      useFactory: () =>
        new SqsTaskQueue({ client, urls, logger }),
    },
  ];

  if (enabled) {
    providers.push({
      provide: SQS_TASK_WORKER,
      useFactory: () => new SqsTaskWorker(workerOptions),
    });

    // Two shapes because a handler either is a function or is a provider that
    // produced one. `inject` and `useFactory` must agree on the arity, so the
    // two cases are written out rather than spread conditionally.
    providers.push(
      options.handlerToken
        ? {
            provide: SqsTaskLifecycle,
            inject: [SQS_TASK_WORKER, options.handlerToken],
            useFactory: (
              worker: TaskWorkerPort<never>,
              handler: TaskHandler<never>,
            ) => new SqsTaskLifecycle(worker, handler),
          }
        : {
            provide: SqsTaskLifecycle,
            inject: [SQS_TASK_WORKER],
            useFactory: (worker: TaskWorkerPort<never>) =>
              new SqsTaskLifecycle(worker, options.handler as TaskHandler<never>),
          },
    );
  }

  return {
    module: SqsTasksModule,
    // Only the token form needs the handler's own module; the value form has
    // nothing to import.
    ...(options.handlerToken && options.imports
      ? { imports: options.imports }
      : {}),
    providers,
    exports: enabled ? [SQS_TASK_QUEUE, SQS_TASK_WORKER, SqsTaskLifecycle] : [SQS_TASK_QUEUE],
  };
}
