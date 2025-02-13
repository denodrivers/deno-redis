import type { CommandExecutor } from "./executor.ts";
import { isRetriableError } from "./errors.ts";
import { kUnstableReadReply } from "./internal/symbols.ts";

export interface RedisMonitorLog {
  timestamp: string;
  args: string[];
  source: string;
  database: string;
}

export interface RedisMonitor {
  readonly isConnected: boolean;
  readonly isClosed: boolean;
  receive(): AsyncIterableIterator<RedisMonitorLog>;
  close(): void;
}

export class RedisMonitorImpl implements RedisMonitor {
  get isConnected(): boolean {
    return this.executor.connection.isConnected;
  }

  get isClosed(): boolean {
    return this.executor.connection.isClosed;
  }

  constructor(private executor: CommandExecutor) {}

  receive(): AsyncIterableIterator<RedisMonitorLog> {
    return this.#receive();
  }

  /**
   * Non-standard return value. Dumps the received commands in an infinite flow.
   * @see https://redis.io/docs/latest/commands/monitor
   */
  async *#receive(): AsyncIterableIterator<RedisMonitorLog> {
    let forceReconnect = false;
    const connection = this.executor.connection;
    while (this.isConnected) {
      try {
        let reply: string;
        try {
          reply = await connection[kUnstableReadReply]() as typeof reply;
        } catch (err) {
          if (this.isClosed) {
            // Connection already closed by the user.
            break;
          }
          throw err; // Connection may have been unintentionally closed.
        }

        // Reply example: 1735135615.9063666 [0 127.0.0.1:52848] "XRANGE" "foo" "-" "+" "COUNT" "3"
        const len = reply.indexOf(" ");
        const timestamp = reply.slice(0, len);
        const argIndex = reply.indexOf('"');
        const args = reply
          .slice(argIndex + 1, -1)
          .split('" "')
          .map((elem) => elem.replace(/\\"/g, '"'));
        const [database, source] = reply.slice(len + 2, argIndex - 2).split(
          " ",
        );

        yield { timestamp, args, source, database };
      } catch (error) {
        if (isRetriableError(error)) {
          forceReconnect = true;
        } else throw error;
      } finally {
        if ((!this.isClosed && !this.isConnected) || forceReconnect) {
          forceReconnect = false;
          await connection.reconnect();
        }
      }
    }
  }

  close() {
    this.executor.connection.close();
  }
}
