import { ErrorReplyError } from "../mod.ts";
import type {
  ArrayReply,
  BulkReply,
  IntegerReply,
  StatusReply,
} from "../protocol/mod.ts";
import {
  assert,
  assertEquals,
} from "../vendor/https/deno.land/std/testing/asserts.ts";
import { newClient, startRedis, stopRedis, TestSuite } from "./test_util.ts";

const suite = new TestSuite("pipeline");
const server = await startRedis({ port: 7014 });
const opts = { hostname: "127.0.0.1", port: 7014 };

suite.afterAll(() => {
  stopRedis(server);
});

suite.test("testPipeline", async () => {
  const client = await newClient(opts);
  const pl = client.pipeline();
  await Promise.all([
    pl.ping(),
    pl.ping(),
    pl.set("set1", "value1"),
    pl.set("set2", "value2"),
    pl.mget("set1", "set2"),
    pl.del("set1"),
    pl.del("set2"),
  ]);
  const ret = await pl.flush();
  assertEquals(ret.length, 7);
  assertEquals((ret[0] as StatusReply).type, "status");
  assertEquals((ret[0] as StatusReply).value(), "PONG");
  assertEquals((ret[1] as StatusReply).type, "status");
  assertEquals((ret[1] as StatusReply).value(), "PONG");
  assertEquals((ret[2] as StatusReply).type, "status");
  assertEquals((ret[2] as StatusReply).value(), "OK");
  assertEquals((ret[3] as StatusReply).type, "status");
  assertEquals((ret[3] as StatusReply).value(), "OK");
  assertEquals((ret[4] as ArrayReply).type, "array");
  assertEquals((ret[4] as ArrayReply).value(), ["value1", "value2"]);
  assertEquals((ret[5] as IntegerReply).type, "integer");
  assertEquals((ret[5] as IntegerReply).value(), 1);
  assertEquals((ret[6] as IntegerReply).type, "integer");
  assertEquals((ret[6] as IntegerReply).value(), 1);
  client.close();
});

suite.test("testTx", async () => {
  const client = await newClient(opts);
  const tx1 = client.tx();
  const tx2 = client.tx();
  const tx3 = client.tx();
  await client.del("key");
  await Promise.all<unknown>([
    tx1.get("key"),
    tx1.incr("key"),
    tx1.incr("key"),
    tx1.incr("key"),
    tx1.get("key"),
    //
    tx2.get("key"),
    tx2.incr("key"),
    tx2.incr("key"),
    tx2.incr("key"),
    tx2.get("key"),
    //
    tx3.get("key"),
    tx3.incr("key"),
    tx3.incr("key"),
    tx3.incr("key"),
    tx3.get("key"),
  ]);
  const rep1 = await tx1.flush() as [
    BulkReply,
    IntegerReply,
    IntegerReply,
    IntegerReply,
    BulkReply,
  ];
  const rep2 = await tx2.flush() as [
    BulkReply,
    IntegerReply,
    IntegerReply,
    IntegerReply,
    BulkReply,
  ];
  const rep3 = await tx3.flush() as [
    BulkReply,
    IntegerReply,
    IntegerReply,
    IntegerReply,
    BulkReply,
  ];
  assertEquals(
    parseInt(rep1[4].value()!),
    parseInt(rep1[0].value()!) + 3,
  );
  assertEquals(
    parseInt(rep2[4].value()!),
    parseInt(rep2[0].value()!) + 3,
  );
  assertEquals(
    parseInt(rep3[4].value()!),
    parseInt(rep3[0].value()!) + 3,
  );
  client.close();
});

suite.test("pipeline in concurrent", async () => {
  {
    const client = await newClient(opts);
    const tx = client.pipeline();
    const promises: Promise<unknown>[] = [];
    await client.del("a", "b", "c");
    for (const key of ["a", "b", "c"]) {
      promises.push(tx.set(key, key));
    }
    promises.push(tx.flush());
    for (const key of ["a", "b", "c"]) {
      promises.push(tx.get(key));
    }
    promises.push(tx.flush());
    const res = await Promise.all(promises) as [
      string,
      string,
      string,
      [StatusReply, StatusReply, StatusReply],
      string,
      string,
      string,
      [BulkReply, BulkReply, BulkReply],
    ];

    assertEquals(res.length, 8);
    assertEquals(res[0], "OK"); // set(a)
    assertEquals(res[1], "OK"); // set(b)
    assertEquals(res[2], "OK"); // set(c)

    // flush()
    assertEquals(res[3].length, 3);
    assertEquals(res[3][0].type, "status");
    assertEquals(res[3][0].value(), "OK");
    assertEquals(res[3][1].type, "status");
    assertEquals(res[3][1].value(), "OK");
    assertEquals(res[3][2].type, "status");
    assertEquals(res[3][2].value(), "OK");

    assertEquals(res[4], "OK"); // get(a)
    assertEquals(res[5], "OK"); // get(b)
    assertEquals(res[6], "OK"); // get(c)

    // flush()
    assertEquals(res[7].length, 3);
    assertEquals(res[7][0].type, "string");
    assertEquals(res[7][0].value(), "a");
    assertEquals(res[7][1].type, "string");
    assertEquals(res[7][1].value(), "b");
    assertEquals(res[7][2].type, "string");
    assertEquals(res[7][2].value(), "c");

    client.close();
  }
});

suite.test("error while pipeline", async () => {
  const client = await newClient(opts);
  const tx = client.pipeline();
  tx.set("a", "a");
  tx.eval("var", ["k"], ["v"]);
  tx.get("a");
  const resp = await tx.flush();
  assertEquals(resp.length, 3);
  assertEquals((resp[0] as StatusReply).type, "status");
  assertEquals((resp[0] as StatusReply).value(), "OK");
  assert(resp[1] instanceof ErrorReplyError);
  assertEquals((resp[2] as BulkReply).type, "string");
  assertEquals((resp[2] as BulkReply).value(), "a");
  client.close();
});

suite.runTests();
