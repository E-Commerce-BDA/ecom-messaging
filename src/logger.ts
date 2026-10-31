/** Minimal logger surface the relay and publishers accept. */
export interface RelayLogger {
  debug(message: string): void;
  warn(message: string): void;
  error(message: string, error?: unknown): void;
}

/** Default logger: console-backed, prefixed, no dependency on Nest. */
export class ConsoleRelayLogger implements RelayLogger {
  constructor(private readonly prefix = "outbox") {}

  debug(message: string): void {
    console.debug(`[${this.prefix}] ${message}`);
  }

  warn(message: string): void {
    console.warn(`[${this.prefix}] ${message}`);
  }

  error(message: string, error?: unknown): void {
    console.error(
      `[${this.prefix}] ${message}`,
      error instanceof Error ? error.message : (error ?? ""),
    );
  }
}

/** Collects log lines so tests can assert on them. */
export class RecordingLogger implements RelayLogger {
  readonly lines: { level: keyof RelayLogger; message: string }[] = [];

  debug(message: string): void {
    this.lines.push({ level: "debug", message });
  }

  warn(message: string): void {
    this.lines.push({ level: "warn", message });
  }

  error(message: string): void {
    this.lines.push({ level: "error", message });
  }
}
