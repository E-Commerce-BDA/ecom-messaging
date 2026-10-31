import {
  Inject,
  Injectable,
  Module,
  type DynamicModule,
  type OnModuleDestroy,
  type OnModuleInit,
  type Provider,
} from "@nestjs/common";
import { ConsoleRelayLogger, type RelayLogger } from "../logger.js";
import { MongoOutboxStore } from "../outbox/mongo-outbox.store.js";
import { OutboxRelay } from "../outbox/outbox.relay.js";
import { PostgresOutboxStore } from "../outbox/postgres-outbox.store.js";
import type { MongoOutboxDb, OutboxStorePort, PoolLike } from "../outbox/types.js";
import { DiscardPublisher } from "../publishers/discard.publisher.js";
import { KafkaEventPublisher } from "../publishers/kafka.publisher.js";
import type { PublisherPort } from "../publishers/publisher.port.js";

export const OUTBOX_STORE = Symbol("OUTBOX_STORE");
export const OUTBOX_PUBLISHER = Symbol("OUTBOX_PUBLISHER");
export const OUTBOX_RELAY = Symbol("OUTBOX_RELAY");

export interface OutboxModuleOptions {
  /** Schema that owns the outbox table: `auth`, `cart`, `catalog` or `order`. */
  schema: string;
  /** Value written into `envelope.producer`, e.g. `auth-svc`. */
  producer: string;
  clientId: string;
  pool: () => PoolLike;
  /** Defaults to `KAFKA_BROKERS` or `localhost:9092`. */
  brokers?: () => string[];
  logger?: RelayLogger;
  /**
   * Defaults to `DISABLE_OUTBOX_RELAY !== 'true' && NODE_ENV !== 'test'`.
   * Evaluated once, when the module is declared.
   */
  enabled?: () => boolean;
}

export interface MongoOutboxModuleOptions {
  /** Collection that holds the events, e.g. `cms_outbox`. */
  collection: string;
  /** Value written into `envelope.producer`, e.g. `cms-svc`. */
  producer: string;
  clientId: string;
  /** Structural access to the database, normally `() => connection.db`. */
  db: () => MongoOutboxDb;
  /** Defaults to `<collection>_locks`. */
  lockCollection?: string;
  /** How long the relay lease lasts. Default 30 s. */
  lockLeaseSeconds?: number;
  /** Defaults to `KAFKA_BROKERS` or `localhost:9092`. */
  brokers?: () => string[];
  logger?: RelayLogger;
  enabled?: () => boolean;
}

export function isOutboxRelayEnabled(): boolean {
  if (process.env.DISABLE_OUTBOX_RELAY === "true") {
    return false;
  }

  return process.env.NODE_ENV !== "test";
}

function brokersFromEnv(): string[] {
  return (process.env.KAFKA_BROKERS ?? "localhost:9092")
    .split(",")
    .map((broker) => broker.trim())
    .filter(Boolean);
}

/**
 * Starts and stops the relay with the Nest lifecycle.
 *
 * Only registered when the relay is enabled, so a test or an operator flag
 * cannot leave a background publisher running against a shared database.
 */
@Injectable()
export class OutboxLifecycle implements OnModuleInit, OnModuleDestroy {
  constructor(
    @Inject(OUTBOX_RELAY) private readonly relay: OutboxRelay,
    @Inject(OUTBOX_PUBLISHER) private readonly publisher: PublisherPort,
  ) {}

  onModuleInit(): void {
    this.relay.start();
  }

  async onModuleDestroy(): Promise<void> {
    this.relay.stop();
    await this.publisher.close();
  }
}

/**
 * Runs the store's one-time setup (the Mongo indexes) even when the relay is
 * disabled, which is the normal case for an e2e suite.
 */
@Injectable()
export class OutboxInitializer implements OnModuleInit {
  constructor(@Inject(OUTBOX_STORE) private readonly store: OutboxStorePort<never>) {}

  async onModuleInit(): Promise<void> {
    await this.store.initialize?.();
  }
}

@Module({})
export class OutboxModule {}

interface SharedModuleOptions {
  /** Log prefix, e.g. `outbox:cart`. */
  label: string;
  clientId: string;
  brokers?: () => string[];
  logger?: RelayLogger;
  enabled?: () => boolean;
}

/**
 * The relay, the publisher and the lifecycle are identical for every store;
 * only the store factory differs, so it is the single parameter here.
 */
function buildProviders(
  storeFactory: () => OutboxStorePort<never>,
  options: SharedModuleOptions,
): Provider[] {
  const logger = options.logger ?? new ConsoleRelayLogger(options.label);
  const enabled = (options.enabled ?? isOutboxRelayEnabled)();

  const providers: Provider[] = [
    { provide: OUTBOX_STORE, useFactory: storeFactory },
    {
      provide: OUTBOX_PUBLISHER,
      useFactory: (): PublisherPort => {
        if (!enabled) {
          return new DiscardPublisher(logger);
        }

        return new KafkaEventPublisher({
          brokers: options.brokers ?? brokersFromEnv,
          clientId: options.clientId,
          logger,
        });
      },
    },
    {
      provide: OUTBOX_RELAY,
      inject: [OUTBOX_STORE, OUTBOX_PUBLISHER],
      useFactory: (store: OutboxStorePort<never>, publisher: PublisherPort) =>
        new OutboxRelay({ store, publisher, logger }),
    },
    OutboxInitializer,
  ];

  if (enabled) {
    providers.push(OutboxLifecycle);
  }

  return providers;
}

/**
 * Builds the outbox module for a Postgres-backed service.
 *
 * The service supplies only its pool, schema and producer name; claim SQL,
 * leases, retries and the envelope stay in this package.
 */
export function createOutboxModule(
  options: OutboxModuleOptions,
): DynamicModule {
  return {
    module: OutboxModule,
    providers: buildProviders(
      () =>
        new PostgresOutboxStore({
          pool: options.pool,
          schema: options.schema,
          producer: options.producer,
        }),
      { ...options, label: `outbox:${options.schema}` },
    ),
    exports: [OUTBOX_STORE, OUTBOX_PUBLISHER, OUTBOX_RELAY],
  };
}

/**
 * Builds the outbox module for a Mongo-backed service.
 *
 * Same relay, same publisher, same retry policy — only the claim mechanism and
 * the lock differ, and those live in `MongoOutboxStore`.
 */
export function createMongoOutboxModule(
  options: MongoOutboxModuleOptions,
): DynamicModule {
  return {
    module: OutboxModule,
    providers: buildProviders(
      () =>
        new MongoOutboxStore({
          db: options.db,
          collection: options.collection,
          lockCollection: options.lockCollection,
          lockLeaseSeconds: options.lockLeaseSeconds,
          producer: options.producer,
        }),
      { ...options, label: `outbox:${options.collection}` },
    ),
    exports: [OUTBOX_STORE, OUTBOX_PUBLISHER, OUTBOX_RELAY],
  };
}
