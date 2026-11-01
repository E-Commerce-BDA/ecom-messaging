import { GetQueueUrlCommand, SQSClient } from "@aws-sdk/client-sqs";
import { ConsoleRelayLogger, type RelayLogger } from "../logger.js";
import type { TaskQueue } from "./task-queue.port.js";

/**
 * The two `send` calls this package makes, behind one structural type.
 *
 * `SQSClient` satisfies it as-is (a class method is bivariant in its parameter),
 * so production code passes the real client and a test passes a stub that
 * records commands — no casting, and no `any` leaking into the worker.
 */
export interface SqsClientLike {
  send(command: object): Promise<unknown>;
}

export interface SqsClientOptions {
  /** Defaults to `AWS_REGION`, then `us-east-1`; read by `SQSClient` itself. */
  region?: string;
  /**
   * LocalStack. `SQSClient` reads `AWS_ENDPOINT_URL` on its own, so this is
   * only needed when the caller wants to set it explicitly.
   */
  endpoint?: string;
  logger?: RelayLogger;
}

/**
 * Builds a real `SQSClient`.
 *
 * Credentials are deliberately not parameters: the SDK's default chain
 * (`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`, then a profile, then an IAM
 * role) is what works on both LocalStack and a task role, and hard-coding them
 * here is where they would leak.
 */
export function createSqsClient(options: SqsClientOptions = {}): SQSClient {
  return new SQSClient({
    region: options.region ?? process.env.AWS_REGION ?? "us-east-1",
    ...(options.endpoint ?? process.env.AWS_ENDPOINT_URL
      ? { endpoint: options.endpoint ?? process.env.AWS_ENDPOINT_URL }
      : {}),
  });
}

/**
 * Resolves a queue entry to its URL, once.
 *
 * SQS addresses a queue by URL, and the URL embeds the account id, which this
 * package cannot know: LocalStack uses `000000000000` and AWS uses the real one.
 * `GetQueueUrl` answers it on both, so there is no per-environment template to
 * keep in sync. `queueUrls` is the escape hatch for a deployment whose policy
 * forbids that call.
 */
export class SqsQueueUrls {
  private readonly logger: RelayLogger;
  private readonly overrides: Partial<Record<string, string>>;
  private readonly cache = new Map<string, Promise<string>>();

  constructor(
    private readonly client: SqsClientLike,
    options: { queueUrls?: Partial<Record<string, string>>; logger?: RelayLogger } = {},
  ) {
    this.overrides = options.queueUrls ?? {};
    this.logger = options.logger ?? new ConsoleRelayLogger("sqs");
  }

  url(queue: TaskQueue): Promise<string> {
    const cached = this.cache.get(queue.queue);
    if (cached) {
      return cached;
    }

    const override = this.overrides[queue.queue];
    // A failed lookup must not be cached, or the worker would poll a rejected
    // promise for the rest of the process lifetime.
    const pending = override
      ? Promise.resolve(override)
      : this.lookup(queue.queue).catch((error: unknown) => {
          this.cache.delete(queue.queue);
          throw error;
        });

    this.cache.set(queue.queue, pending);
    return pending;
  }

  private async lookup(name: string): Promise<string> {
    const output = (await this.client.send(
      new GetQueueUrlCommand({ QueueName: name }),
    )) as { QueueUrl?: string };

    if (!output.QueueUrl) {
      throw new Error(`SQS did not return a URL for queue ${name}`);
    }

    this.logger.debug(`resolved queue ${name} to ${output.QueueUrl}`);
    return output.QueueUrl;
  }
}
