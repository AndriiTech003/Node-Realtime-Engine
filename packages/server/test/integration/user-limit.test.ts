import { afterAll, beforeAll, describe, expect, it } from "vitest";
import type { RealtimeServer } from "@ashamrai/realtime-server";
import { eventually, purgePrefix, startNode, TestClient, uniquePrefix } from "../../../../test-support/helpers.js";

const prefix = uniquePrefix();
const LIMIT = 4;
let a: RealtimeServer;
let b: RealtimeServer;
const extra: RealtimeServer[] = [];

beforeAll(async () => {
  a = await startNode(prefix, { nodeId: "ul-a", maxConnectionsPerUser: LIMIT });
  b = await startNode(prefix, { nodeId: "ul-b", maxConnectionsPerUser: LIMIT });
});

afterAll(async () => {
  for (const node of [a, b, ...extra]) await node.stop().catch(() => undefined);
  await purgePrefix(prefix);
});

async function counts(uid: string): Promise<Record<string, number>> {
  return a.store.userConnections(uid);
}

async function total(uid: string): Promise<number> {
  return Object.values(await counts(uid)).reduce((x, y) => x + y, 0);
}

async function open(node: RealtimeServer, uid: string): Promise<TestClient> {
  const client = await TestClient.connect(node, uid);
  expect(client.closeCode).toBeNull();
  return client;
}

async function rejected(node: RealtimeServer, uid: string): Promise<number> {
  const client = await TestClient.connect(node, uid);
  return client.waitClose();
}

function cid(client: TestClient): string {
  const hello = client.frames.find((f) => f.t === "hello");
  if (hello === undefined || hello.t !== "hello") throw new Error("no hello");
  return hello.cid;
}

describe("cluster-wide per-user connection limit", () => {
  it("counts one user's connections across nodes and rejects the next one on any node with 4029", async () => {
    const clients = [await open(a, "u1"), await open(a, "u1"), await open(b, "u1"), await open(b, "u1")];
    expect(await counts("u1")).toEqual({ "ul-a": 2, "ul-b": 2 });
    expect(await rejected(b, "u1")).toBe(4029);
    expect(await rejected(a, "u1")).toBe(4029);
    expect(await counts("u1")).toEqual({ "ul-a": 2, "ul-b": 2 });
    for (const c of clients) expect(c.closeCode).toBeNull();
    expect(a.userLimiter.localCount("u1")).toBe(2);
    expect(b.userLimiter.localCount("u1")).toBe(2);
    for (const c of clients) c.close();
    await eventually(async () => (await total("u1")) === 0);
    expect(await a.store.nodeUserIds("ul-a")).not.toContain("u1");
    expect(await a.store.nodeUserIds("ul-b")).not.toContain("u1");
  });

  it("a normal close on node A frees a slot that node B can use", async () => {
    const clients = [await open(a, "u2"), await open(a, "u2"), await open(b, "u2"), await open(b, "u2")];
    expect(await rejected(b, "u2")).toBe(4029);
    clients[0]?.close();
    await eventually(async () => (await counts("u2"))["ul-a"] === 1);
    const replacement = await open(b, "u2");
    expect(await counts("u2")).toEqual({ "ul-a": 1, "ul-b": 3 });
    for (const c of [...clients.slice(1), replacement]) c.close();
    await eventually(async () => (await total("u2")) === 0);
  });

  it("an abrupt client terminate and a server-side terminate both decrement", async () => {
    const clients = [await open(a, "u3"), await open(a, "u3"), await open(b, "u3"), await open(b, "u3")];
    clients[0]?.terminate();
    await eventually(async () => (await total("u3")) === 3);
    const serverSide = b.connection(cid(clients[2] as TestClient));
    expect(serverSide).toBeDefined();
    serverSide?.terminate();
    await eventually(async () => (await total("u3")) === 2);
    expect(await counts("u3")).toEqual({ "ul-a": 1, "ul-b": 1 });
    const more = [await open(a, "u3"), await open(b, "u3")];
    expect(await rejected(a, "u3")).toBe(4029);
    for (const c of [...clients, ...more]) c.close();
    await eventually(async () => (await total("u3")) === 0);
  });

  it("concurrent connects through two nodes admit exactly the limit", async () => {
    const attempts = await Promise.all(Array.from({ length: 10 }, (_, i) => TestClient.connect(i % 2 === 0 ? a : b, "u4")));
    await eventually(() => attempts.filter((c) => c.closeCode === 4029).length === 6);
    const open4 = attempts.filter((c) => c.closeCode === null);
    expect(open4).toHaveLength(LIMIT);
    expect(await total("u4")).toBe(LIMIT);
    for (const c of attempts) c.close();
    await eventually(async () => (await total("u4")) === 0);
    expect(a.userLimiter.localCount("u4") + b.userLimiter.localCount("u4")).toBe(0);
  });

  it("connections of a crashed node are released by a surviving node's sweeper", async () => {
    const c = await startNode(prefix, { nodeId: "ul-c", maxConnectionsPerUser: LIMIT });
    extra.push(c);
    const onC = [await open(c, "u5"), await open(c, "u5")];
    const onA = [await open(a, "u5"), await open(a, "u5")];
    expect(await rejected(b, "u5")).toBe(4029);
    expect(await counts("u5")).toEqual({ "ul-a": 2, "ul-c": 2 });
    await c.crash();
    await eventually(async () => (await counts("u5"))["ul-c"] === undefined, 10000);
    expect(await a.store.nodeUserIds("ul-c")).toEqual([]);
    const after = [await open(b, "u5"), await open(b, "u5")];
    expect(await counts("u5")).toEqual({ "ul-a": 2, "ul-b": 2 });
    expect(await rejected(a, "u5")).toBe(4029);
    for (const client of [...onC, ...onA, ...after]) client.close();
    await eventually(async () => (await total("u5")) === 0);
  });

  it("a graceful stop removes the node's counts", async () => {
    const d = await startNode(prefix, { nodeId: "ul-d", maxConnectionsPerUser: LIMIT });
    extra.push(d);
    const clients = [await open(d, "u6"), await open(d, "u6"), await open(a, "u6")];
    expect(await counts("u6")).toEqual({ "ul-a": 1, "ul-d": 2 });
    await d.stop();
    expect(await counts("u6")).toEqual({ "ul-a": 1 });
    for (const client of clients) client.close();
    await eventually(async () => (await total("u6")) === 0);
  });

  it("a node restarted under the same id clears the counts of its previous incarnation", async () => {
    const first = await startNode(prefix, { nodeId: "ul-e", maxConnectionsPerUser: LIMIT, nodeAliveTtlMs: 60000 });
    extra.push(first);
    const clients = [await open(first, "u7"), await open(first, "u7"), await open(first, "u7")];
    await first.crash();
    expect(await counts("u7")).toEqual({ "ul-e": 3 });
    const second = await startNode(prefix, { nodeId: "ul-e", maxConnectionsPerUser: LIMIT });
    extra.push(second);
    expect(await counts("u7")).toEqual({});
    const fresh = [await open(second, "u7"), await open(second, "u7"), await open(second, "u7"), await open(second, "u7")];
    expect(await rejected(a, "u7")).toBe(4029);
    for (const client of [...clients, ...fresh]) client.close();
    await eventually(async () => (await total("u7")) === 0);
  });

  it("a node that was swept as dead while still alive restores its counts", async () => {
    const clients = [await open(b, "u8"), await open(b, "u8"), await open(b, "u8")];
    expect(await counts("u8")).toEqual({ "ul-b": 3 });
    await a.store.reapNode("ul-b");
    expect(await counts("u8")).toEqual({});
    await eventually(async () => (await counts("u8"))["ul-b"] === 3, 5000);
    expect(await a.store.nodeUserIds("ul-b")).toContain("u8");
    const last = await open(a, "u8");
    expect(await rejected(a, "u8")).toBe(4029);
    for (const client of [...clients, last]) client.close();
    await eventually(async () => (await total("u8")) === 0);
  });

  it("presence of a node that was swept as dead while still alive comes back", async () => {
    const watcher = await open(a, "w9");
    await watcher.sub("room:rejoin");
    const member = await open(b, "u9");
    await member.sub("room:rejoin");
    await member.request({ t: "pres", ch: "room:rejoin", meta: { status: "typing" } });
    await watcher.waitFor((f) => f.t === "pu" && f.uid === "u9");
    const mark = watcher.frames.length;
    await a.store.reapNode("ul-b");
    await watcher.waitFor((f) => f.t === "pl" && f.uid === "u9", 5000, mark);
    const back = await watcher.waitFor((f) => f.t === "pj" && f.uid === "u9", 5000, mark);
    expect(back.t === "pj" ? back.meta : undefined).toEqual({ status: "typing" });
    const list = await a.store.presenceList("room:rejoin", 10);
    expect(list.entries.map((e) => e.uid).sort()).toEqual(["u9", "w9"]);
    await eventually(async () => (await counts("u9"))["ul-b"] === 1);
    watcher.close();
    member.close();
  });
});
