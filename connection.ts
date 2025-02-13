import type { Backoff } from "./backoff.ts";
import { exponentialBackoff } from "./backoff.ts";
import { ErrorReplyError, isRetriableError } from "./errors.ts";
import type {
  ConnectionEventMap,
  ConnectionEventType,
  TypedEventTarget,
} from "./events.ts";
import {
  kUnstableCreateProtocol,
  kUnstablePipeline,
  kUnstableReadReply,
  kUnstableWriteCommand,
} from "./internal/symbols.ts";
import { Protocol as DenoStreamsProtocol } from "./protocol/deno_streams/mod.ts";
import type { Command, Protocol } from "./protocol/shared/protocol.ts";
import type { RedisReply, RedisValue } from "./protocol/shared/types.ts";
import { delay } from "./deps/std/async.ts";

export interface SendCommandOptions {
  /**
   * When this option is set, simple or bulk string replies are returned as `Uint8Array` type.
   *
   * @default false
   */
  returnUint8Arrays?: boolean;

  /**
   * When this option is set, the command is executed directly without queueing.
   *
   * @default false
   */
  inline?: boolean;
}

export interface Connection extends TypedEventTarget<ConnectionEventMap> {
  /** @deprecated */
  name: string | null;
  isClosed: boolean;
  isConnected: boolean;
  close(): void;
  [Symbol.dispose](): void;
  connect(): Promise<void>;
  reconnect(): Promise<void>;
  duplicate(): RedisConnection;
  sendCommand(
    command: string,
    args?: Array<RedisValue>,
    options?: SendCommandOptions,
  ): Promise<RedisReply>;
  /**
   * @private
   */
  [kUnstableReadReply](returnsUint8Arrays?: boolean): Promise<RedisReply>;
  /**
   * @private
   */
  [kUnstableWriteCommand](command: Command): Promise<void>;
  /**
   * @private
   */
  [kUnstablePipeline](
    commands: Array<Command>,
  ): Promise<Array<RedisReply | ErrorReplyError>>;
}

export interface RedisConnectionOptions {
  tls?: boolean;
  /**
   * A list of root certificates, implies {@linkcode RedisConnectionOptions.tls}
   */
  caCerts?: string[];
  db?: number;
  password?: string;
  username?: string;
  name?: string;
  /**
   * @default 10
   */
  maxRetryCount?: number;
  backoff?: Backoff;
  /**
   * When this option is set, a `PING` command is sent every specified number of seconds.
   */
  healthCheckInterval?: number;

  /**
   * @private
   */
  [kUnstableCreateProtocol]?: (conn: Deno.Conn) => Protocol;
}

export const kEmptyRedisArgs: Array<RedisValue> = [];

interface PendingCommand {
  execute: () => Promise<RedisReply>;
  resolve: (reply: RedisReply) => void;
  reject: (error: unknown) => void;
}

export function createRedisConnection(
  hostname: string,
  port: number | string | undefined,
  options: RedisConnectionOptions,
): Connection {
  return new RedisConnection(hostname, port ?? 6379, options);
}

class RedisConnection
  implements Connection, TypedEventTarget<ConnectionEventMap> {
  name: string | null = null;
  private maxRetryCount = 10;

  private readonly hostname: string;
  private readonly port: number | string;
  private _isClosed = false;
  private _isConnected = false;
  private backoff: Backoff;

  private commandQueue: PendingCommand[] = [];
  #conn!: Deno.Conn;
  #protocol!: Protocol;
  #eventTarget = new EventTarget();

  get isClosed(): boolean {
    return this._isClosed;
  }

  get isConnected(): boolean {
    return this._isConnected;
  }

  get isRetriable(): boolean {
    return this.maxRetryCount > 0;
  }

  constructor(
    hostname: string,
    port: number | string,
    private options: RedisConnectionOptions,
  ) {
    this.hostname = hostname;
    this.port = port;
    if (options.name) {
      this.name = options.name;
    }
    if (options.maxRetryCount != null) {
      this.maxRetryCount = options.maxRetryCount;
    }
    this.backoff = options.backoff ?? exponentialBackoff();
  }

  duplicate(): RedisConnection {
    return new RedisConnection(this.hostname, this.port, this.options);
  }

  private async authenticate(
    username: string | undefined,
    password: string,
  ): Promise<void> {
    try {
      password && username
        ? await this.sendCommand("AUTH", [username, password], { inline: true })
        : await this.sendCommand("AUTH", [password], { inline: true });
    } catch (error) {
      if (error instanceof ErrorReplyError) {
        const authError = new AuthenticationError("Authentication failed", {
          cause: error,
        });
        this.#dispatchEvent("error", { error: authError });
        throw authError;
      } else {
        this.#dispatchEvent("error", { error });
        throw error;
      }
    }
  }

  private async selectDb(
    db: number | undefined = this.options.db,
  ): Promise<void> {
    if (!db) throw new Error("The database index is undefined.");
    await this.sendCommand("SELECT", [db], { inline: true });
  }

  private enqueueCommand(
    command: PendingCommand,
  ) {
    this.commandQueue.push(command);
    if (this.commandQueue.length === 1) {
      this.processCommandQueue();
    }
  }

  sendCommand(
    command: string,
    args?: Array<RedisValue>,
    options?: SendCommandOptions,
  ): Promise<RedisReply> {
    const execute = () =>
      this.#protocol.sendCommand(
        command,
        args ?? kEmptyRedisArgs,
        options?.returnUint8Arrays,
      );
    if (options?.inline) {
      return execute();
    }
    const { promise, resolve, reject } = Promise.withResolvers<RedisReply>();
    this.enqueueCommand({ execute, resolve, reject });

    return promise;
  }

  addEventListener<K extends keyof ConnectionEventMap>(
    type: K,
    callback: (event: CustomEvent<ConnectionEventMap[K]>) => void,
    options?: AddEventListenerOptions | boolean,
  ): void {
    return this.#eventTarget.addEventListener(
      type,
      callback as (event: Event) => void,
      options,
    );
  }

  removeEventListener<K extends keyof ConnectionEventMap>(
    type: K,
    callback: (event: CustomEvent<ConnectionEventMap[K]>) => void,
    options?: EventListenerOptions | boolean,
  ): void {
    return this.#eventTarget.removeEventListener(
      type,
      callback as (event: Event) => void,
      options,
    );
  }

  #dispatchEvent<K extends ConnectionEventType>(
    type: K,
    detail: ConnectionEventMap[K],
  ): boolean {
    return this.#eventTarget.dispatchEvent(new CustomEvent(type, { detail }));
  }

  [kUnstableReadReply](returnsUint8Arrays?: boolean): Promise<RedisReply> {
    return this.#protocol.readReply(returnsUint8Arrays);
  }

  [kUnstablePipeline](commands: Array<Command>): Promise<RedisReply[]> {
    const { promise, resolve, reject } = Promise.withResolvers<
      RedisReply[]
    >();
    const execute = () => this.#protocol.pipeline(commands);
    this.enqueueCommand({ execute, resolve, reject } as PendingCommand);
    return promise;
  }

  [kUnstableWriteCommand](command: Command): Promise<void> {
    return this.#protocol.writeCommand(command);
  }

  /**
   * Connect to Redis server
   */
  async connect(): Promise<void> {
    await this.#connect(0);
  }

  async #connect(retryCount: number) {
    try {
      const dialOpts: Deno.ConnectOptions = {
        hostname: this.hostname,
        port: parsePortLike(this.port),
      };
      const conn: Deno.Conn = this.options?.tls || this.options?.caCerts != null
        ? await Deno.connectTls({ ...dialOpts, caCerts: this.options?.caCerts })
        : await Deno.connect(dialOpts);

      this.#conn = conn;
      this.#protocol = this.options?.[kUnstableCreateProtocol]?.(conn) ??
        new DenoStreamsProtocol(conn);

      this._isClosed = false;
      this._isConnected = true;
      this.#dispatchEvent("connect", undefined);

      try {
        if (this.options.password != null) {
          await this.authenticate(this.options.username, this.options.password);
        }
        if (this.options.db) {
          await this.selectDb(this.options.db);
        }
      } catch (error) {
        this.#close();
        throw error;
      }

      this.#dispatchEvent("ready", undefined);

      this.#enableHealthCheckIfNeeded();
    } catch (error) {
      if (error instanceof AuthenticationError) {
        this.#dispatchEvent("error", { error });
        this.#dispatchEvent("end", undefined);
        throw (error.cause ?? error);
      }

      const backoff = this.backoff(retryCount);
      retryCount++;
      if (retryCount >= this.maxRetryCount) {
        this.#dispatchEvent("error", { error: error as Error });
        this.#dispatchEvent("end", undefined);
        throw error;
      }
      this.#dispatchEvent("reconnecting", { delay: backoff });
      await delay(backoff);
      await this.#connect(retryCount);
    }
  }

  close() {
    return this[Symbol.dispose]();
  }

  [Symbol.dispose](): void {
    return this.#close(false);
  }

  #close(canReconnect = false) {
    const isClosedAlready = this._isClosed;

    this._isClosed = true;
    this._isConnected = false;
    try {
      this.#conn!.close();
    } catch (error) {
      if (!(error instanceof Deno.errors.BadResource)) {
        this.#dispatchEvent("error", { error: error as Error });
        throw error;
      }
    } finally {
      if (!isClosedAlready) {
        this.#dispatchEvent("close", undefined);

        if (!canReconnect) {
          this.#dispatchEvent("end", undefined);
        }
      }
    }
  }

  async reconnect(): Promise<void> {
    try {
      await this.sendCommand("PING");
      this._isConnected = true;
    } catch (error) {
      this.#dispatchEvent("error", { error });
      this.#close(true);
      await this.connect();
      await this.sendCommand("PING");
    }
  }

  private async processCommandQueue() {
    const [command] = this.commandQueue;
    if (!command) return;

    try {
      const reply = await command.execute();
      command.resolve(reply);
    } catch (error) {
      if (
        !isRetriableError(error) ||
        this.isManuallyClosedByUser()
      ) {
        this.#dispatchEvent("error", { error });
        return command.reject(error);
      }

      let backoff = 0;
      for (let i = 0; i < this.maxRetryCount; i++) {
        // Try to reconnect to the server and retry the command
        this.#close(true);
        try {
          this.#dispatchEvent("reconnecting", { delay: backoff });
          await this.connect();
          const reply = await command.execute();
          return command.resolve(reply);
        } catch (error) {
          this.#dispatchEvent("error", { error }); // TODO: use `AggregateError`?
          backoff = this.backoff(i);
          await delay(backoff);
        }
      }

      this.#dispatchEvent("error", { error });
      command.reject(error);
    } finally {
      this.commandQueue.shift();
      this.processCommandQueue();
    }
  }

  private isManuallyClosedByUser(): boolean {
    return this._isClosed && !this._isConnected;
  }

  #enableHealthCheckIfNeeded() {
    const { healthCheckInterval } = this.options;
    if (healthCheckInterval == null) {
      return;
    }

    const ping = async () => {
      if (this.isManuallyClosedByUser()) {
        return;
      }

      try {
        await this.sendCommand("PING");
        this._isConnected = true;
      } catch (_error) {
        // TODO: notify the user of an error
        this._isConnected = false;
      } finally {
        setTimeout(ping, healthCheckInterval);
      }
    };

    setTimeout(ping, healthCheckInterval);
  }
}

class AuthenticationError extends Error {}

function parsePortLike(port: string | number | undefined): number {
  let parsedPort: number;
  if (typeof port === "string") {
    parsedPort = parseInt(port);
  } else if (typeof port === "number") {
    parsedPort = port;
  } else {
    parsedPort = 6379;
  }
  if (!Number.isSafeInteger(parsedPort)) {
    throw new Error("Port is invalid");
  }
  return parsedPort;
}
