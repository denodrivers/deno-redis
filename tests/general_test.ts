import {
  BulkReply,
  createLazyClient,
  ErrorReplyError,
  parseURL,
  replyTypes,
} from "../mod.ts";
import {
  assert,
  assertEquals,
  assertNotEquals,
  assertRejects,
} from "../vendor/https/deno.land/std/testing/asserts.ts";
import {
  newClient,
  nextPort,
  startRedis,
  stopRedis,
  TestSuite,
} from "./test_util.ts";

const suite = new TestSuite("general");
const port = nextPort();
const server = await startRedis({ port });
const opts = { hostname: "127.0.0.1", port };
const client = await newClient(opts);

suite.afterAll(() => {
  stopRedis(server);
  client.close();
});

suite.test("conccurent", async () => {
  let promises: Promise<string | undefined>[] = [];
  for (const key of ["a", "b", "c"]) {
    promises.push(client.set(key, key));
  }
  await Promise.all(promises);
  promises = [];
  for (const key of ["a", "b", "c"]) {
    promises.push(client.get(key));
  }
  const [a, b, c] = await Promise.all(promises);
  assertEquals(a, "a");
  assertEquals(b, "b");
  assertEquals(c, "c");
});

suite.test("db0", async () => {
  const key = "exists";
  const client1 = await newClient({ ...opts, db: 0 });
  await client1.set(key, "aaa");
  const exists1 = await client1.exists(key);
  assertEquals(exists1, 1);
  const client2 = await newClient({ ...opts, db: 0 });
  const exists2 = await client2.exists(key);
  assertEquals(exists2, 1);
  client1.close();
  client2.close();
});

suite.test("connect with wrong password", async () => {
  await assertRejects(async () => {
    await newClient({
      hostname: "127.0.0.1",
      port,
      password: "wrong_password",
    });
  }, ErrorReplyError);
});

suite.test("connect with empty password", async () => {
  // In Redis, authentication with an empty password will always fail.
  await assertRejects(async () => {
    await newClient({
      hostname: "127.0.0.1",
      port,
      password: "",
    });
  }, ErrorReplyError);
});

suite.test("exists", async () => {
  const key = "exists";
  const client1 = await newClient({ ...opts, db: 0 });
  await client1.set(key, "aaa");
  const exists1 = await client1.exists(key);
  assertEquals(exists1, 1);
  const client2 = await newClient({ ...opts, db: 1 });
  const exists2 = await client2.exists(key);
  assertEquals(exists2, 0);
  client1.close();
  client2.close();
});

[Infinity, NaN, "", "port"].forEach((v) => {
  suite.test(`invalid port: ${v}`, async () => {
    await assertRejects(
      async () => {
        await newClient({ hostname: "127.0.0.1", port: v });
      },
      Error,
      "invalid",
    );
  });
});

suite.test("sendCommand - simple types", async () => {
  // simple string
  {
    const reply = await client.sendCommand("SET", "key", "a");
    assertEquals(reply.type, replyTypes.SimpleString);
    assertEquals(reply.value(), "OK");
  }

  // bulk string
  {
    const reply = await client.sendCommand("GET", "key");
    assertEquals(reply.type, replyTypes.BulkString);
    assertEquals(reply.value(), "a");
  }

  // integer
  {
    const reply = await client.sendCommand("EXISTS", "key");
    assertEquals(reply.type, replyTypes.Integer);
    assertEquals(reply.value(), 1);
  }
});

suite.test("sendCommand - get the raw data as Uint8Array", async () => {
  const encoder = new TextEncoder();
  await client.set("key", encoder.encode("hello"));
  const reply = await client.sendCommand("GET", "key");
  assertEquals(reply.type, replyTypes.BulkString);
  assertEquals((reply as BulkReply).buffer(), encoder.encode("hello"));
});

suite.test("lazy client", async () => {
  const resources = Deno.resources();
  const client = createLazyClient(opts);
  assert(!client.isConnected);
  assertEquals(resources, Deno.resources());
  try {
    await client.get("foo");
    assert(client.isConnected);
    assertNotEquals(resources, Deno.resources());
  } finally {
    client.close();
  }
});

suite.test("parse basic URL", () => {
  const options = parseURL("redis://127.0.0.1:7003");
  assertEquals(options.hostname, "127.0.0.1");
  assertEquals(options.port, 7003);
  assertEquals(options.tls, false);
  assertEquals(options.db, undefined);
  assertEquals(options.name, undefined);
  assertEquals(options.password, undefined);
});

suite.test("parse complex URL", () => {
  const options = parseURL("rediss://username:password@127.0.0.1:7003/1");
  assertEquals(options.hostname, "127.0.0.1");
  assertEquals(options.port, 7003);
  assertEquals(options.tls, true);
  assertEquals(options.db, 1);
  assertEquals(options.name, "username");
  assertEquals(options.password, "password");
});

suite.test("parse URL with search options", () => {
  const options = parseURL(
    "redis://127.0.0.1:7003/?db=2&password=password&ssl=true",
  );
  assertEquals(options.hostname, "127.0.0.1");
  assertEquals(options.port, 7003);
  assertEquals(options.tls, true);
  assertEquals(options.db, 2);
  assertEquals(options.name, undefined);
  assertEquals(options.password, "password");
});

suite.test("Check parameter parsing priority", () => {
  const options = parseURL(
    "rediss://username:password@127.0.0.1:7003/1?db=2&password=password2&ssl=false",
  );
  assertEquals(options.hostname, "127.0.0.1");
  assertEquals(options.port, 7003);
  assertEquals(options.tls, true);
  assertEquals(options.db, 1);
  assertEquals(options.name, "username");
  assertEquals(options.password, "password");
});

suite.runTests();
