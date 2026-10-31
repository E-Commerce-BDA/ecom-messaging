import { ConsoleRelayLogger, type RelayLogger } from "../logger.js";
import type { PublishInput, PublisherPort } from "./publisher.port.js";

/**
 * Publisher used when the relay is disabled (`DISABLE_OUTBOX_RELAY=true`,
 * tests, or a single-process tool). It accepts every event and drops it, so
 * the relay never runs in an environment that must not touch a broker.
 */
export class DiscardPublisher implements PublisherPort {
  private readonly logger: RelayLogger;

  constructor(logger: RelayLogger = new ConsoleRelayLogger("outbox.publisher")) {
    this.logger = logger;
  }

  async publish(input: PublishInput): Promise<void> {
    this.logger.debug(
      `relay disabled: dropped ${input.topic} ${input.envelope.id}`,
    );
  }

  async close(): Promise<void> {
    // nothing to close
  }
}
