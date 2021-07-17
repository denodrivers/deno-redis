import { nextPorts, startRedisCluster, stopRedisCluster } from "./test_util.ts";
import { TestSuite } from "../test_util.ts";
import { connect as connectToCluster } from "../../experimental/cluster/mod.ts";
import {
  assert,
  assertArrayIncludes,
  assertEquals,
  assertThrowsAsync,
} from "../../vendor/https/deno.land/std/testing/asserts.ts";
import sample from "../../vendor/https/cdn.skypack.dev/lodash-es/sample.js";
import calculateSlot from "../../vendor/https/cdn.skypack.dev/cluster-key-slot/lib/index.js";
import { ErrorReplyError } from "../../errors.ts";
import { connect, create } from "../../redis.ts";
import type { CommandExecutor } from "../../executor.ts";

const suite = new TestSuite("cluster/client");
const ports = nextPorts(6);
const cluster = await startRedisCluster(ports);
const nodes = ports.map((port) => ({
  hostname: "127.0.0.1",
  port,
}));
const client = await connectToCluster({ nodes });

suite.afterAll(() => {
  stopRedisCluster(cluster);
});

suite.afterEach(() => {
  client.close();
});

suite.test("del multiple keys in the same hash slot", async () => {
  await client.set("{hoge}foo", "a");
  await client.set("{hoge}bar", "b");
  const r = await client.del("{hoge}foo", "{hoge}bar");
  assertEquals(r, 2);
});

suite.test("del multiple keys in different hash slots", async () => {
  await client.set("foo", "a");
  await client.set("bar", "b");
  await assertThrowsAsync(
    async () => {
      await client.del("foo", "bar");
    },
    ErrorReplyError,
    "-CROSSSLOT Keys in request don't hash to the same slot",
  );
});

suite.test("handle a -MOVED redirection error", async () => {
  let redirected = false;
  let manuallyRedirectedPort!: number;
  const portsSent = new Set<number>();
  const client = await connectToCluster({
    nodes,
    async newRedis(opts) {
      const redis = await connect(opts);
      assert(opts.port != null);
      const proxyExecutor = {
        get connection() {
          return redis.executor.connection;
        },
        async exec(cmd, ...args) {
          if (cmd === "GET" && !redirected) {
            // Manually cause a -MOVED redirection error
            const [key] = args;
            assert(typeof key === "string");
            const slot = calculateSlot(key);
            manuallyRedirectedPort = sample(
              ports.filter((x) => x !== opts.port),
            );
            const error = new ErrorReplyError(
              `-MOVED ${slot} ${opts.hostname}:${manuallyRedirectedPort}`,
            );
            redirected = true;
            throw error;
          } else {
            assert(opts.port);
            portsSent.add(Number(opts.port));
            const reply = await redis.executor.exec(cmd, ...args);
            return reply;
          }
        },
      } as CommandExecutor;
      return create(proxyExecutor);
    },
  });

  try {
    await client.set("foo", "bar");
    const r = await client.get("foo");
    assertEquals(r, "bar");
    // Check if a cluster client correctly handles a -MOVED error
    assert(redirected);
    assertArrayIncludes<number>([...portsSent], [manuallyRedirectedPort]);
  } finally {
    client.close();
  }
});

suite.test("handle a -ASK redirection error", async () => {
  let redirected = false;
  let manuallyRedirectedPort!: number;
  const portsSent = new Set<number>();
  const commandsSent = new Set<string>();
  const client = await connectToCluster({
    nodes,
    async newRedis(opts) {
      const redis = await connect(opts);
      assert(opts.port != null);
      const proxyExecutor = {
        get connection() {
          return redis.executor.connection;
        },
        async exec(cmd, ...args) {
          commandsSent.add(cmd);
          if (cmd === "GET" && !redirected) {
            // Manually cause a -ASK redirection error
            const [key] = args;
            assert(typeof key === "string");
            const slot = calculateSlot(key);
            manuallyRedirectedPort = sample(
              ports.filter((x) => x !== opts.port),
            );
            const error = new ErrorReplyError(
              `-ASK ${slot} ${opts.hostname}:${manuallyRedirectedPort}`,
            );
            redirected = true;
            throw error;
          } else {
            assert(opts.port);
            portsSent.add(Number(opts.port));
            const reply = await redis.executor.exec(cmd, ...args);
            return reply;
          }
        },
      } as CommandExecutor;
      return create(proxyExecutor);
    },
  });
  try {
    await client.set("hoge", "piyo");
    const r = await client.get("hoge");
    assertEquals(r, "piyo");
    // Check if a cluster client correctly handles a -ASK error
    assert(redirected);
    assertArrayIncludes<number>([...portsSent], [manuallyRedirectedPort]);
    assertArrayIncludes<string>([...commandsSent], ["ASKING"]);
  } finally {
    client.close();
  }
});

suite.test("properly handle too many redirections", async () => {
  const client = await connectToCluster({
    nodes,
    async newRedis(opts) {
      const redis = await connect(opts);
      assert(opts.port != null);
      const proxyExecutor = {
        get connection() {
          return redis.executor.connection;
        },
        async exec(cmd, ...args) {
          if (cmd === "GET") {
            // Manually cause a -MOVED redirection error
            const [key] = args;
            assert(typeof key === "string");
            const slot = calculateSlot(key);
            const randomPort = sample(
              ports.filter((x) => x !== opts.port),
            );
            const error = new ErrorReplyError(
              `-MOVED ${slot} ${opts.hostname}:${randomPort}`,
            );
            throw error;
          } else {
            const reply = await redis.executor.exec(cmd, ...args);
            return reply;
          }
        },
      } as CommandExecutor;
      return create(proxyExecutor);
    },
  });
  try {
    await assertThrowsAsync(
      () => client.get("foo"),
      Error,
      "Too many Cluster redirections?",
    );
  } finally {
    client.close();
  }
});

suite.runTests();
