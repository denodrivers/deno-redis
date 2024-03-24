import { assertEquals } from "../vendor/https/deno.land/std/assert/mod.ts";
import {
  beforeAll,
  describe,
  it,
} from "../vendor/https/deno.land/std/testing/bdd.ts";
import { newClient, nextPort, startRedis, stopRedis } from "./test_util.ts";

describe("reconnect", () => {
  let port!: number;
  beforeAll(() => {
    port = nextPort();
  });

  it("auto reconnect", async () => {
    let server = await startRedis({ port });
    const client = await newClient({ hostname: "127.0.0.1", port });
    assertEquals(await client.ping(), "PONG");
    await stopRedis(server);
    server = await startRedis({ port });
    assertEquals(await client.ping(), "PONG");
    client.close();
    await stopRedis(server);
  });

  it("auto reconnect, with db spec", async () => {
    let server = await startRedis({ port });
    const client = await newClient({ hostname: "127.0.0.1", port, db: 1 });
    assertEquals(await client.ping(), "PONG");
    await stopRedis(server);
    server = await startRedis({ port });
    assertEquals(await client.ping(), "PONG");
    client.close();
    await stopRedis(server);
  });
});
