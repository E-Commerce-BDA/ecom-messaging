import type { PublishInput, PublisherPort } from "./publisher.port.js";

/**
 * Test double: records what would have been published and can be told to fail
 * the next N attempts so the relay's retry path is exercised without a broker.
 */
export class RecordingPublisher implements PublisherPort {
  readonly published: PublishInput[] = [];
  failNext = 0;
  failure: Error = new Error("kafka is unavailable");

  async publish(input: PublishInput): Promise<void> {
    if (this.failNext > 0) {
      this.failNext -= 1;
      throw this.failure;
    }

    this.published.push(input);
  }

  async close(): Promise<void> {
    // nothing to close
  }

  reset(): void {
    this.published.length = 0;
    this.failNext = 0;
  }
}
