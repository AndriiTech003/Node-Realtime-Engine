import { afterAll, beforeAll, describe, expect, it } from "vitest";
import WebSocket from "ws";
import type { RealtimeServer } from "@ashamrai/realtime-server";
import { RealtimeClient, type Channel, type ChannelMessage, type WebSocketConstructor } from "@ashamrai/realtime-client";
import { eventually, getTicket, purgePrefix, serverPublish, sleep, startNode, uniquePrefix } from "../../../../test-support/helpers.js";

const prefix = uniquePrefix();
let a: RealtimeServer;
let b: RealtimeServer;
const clients: RealtimeClient[] = [];

beforeAll(async () => {
  a = await startNode(prefix, { nodeId: "cli-a" });
  b = await startNode(prefix, { nodeId: "cli-b" });
});

afterAll(async () => {
  for (const c of clients) c.close();
  await a.stop().catch(() => undefined);
  await b.stop().catch(() => undefined);
  await purgePrefix(prefix);
});

let nodeIndex = 0;

function client(uid: string, options: Partial<ConstructorParameters<typeof RealtimeClient>[0]> & { node?: () => RealtimeServer } = {}): RealtimeClient {
  const pick = options.node ?? (() => (nodeIndex++ % 2 === 0 ? a : b));
  let target = pick();
  const c = new RealtimeClient({
    url: "ws://127.0.0.1:0/v1/connect",
    getTicket: async () => {
      target = pick();
      return getTicket(target, uid);
    },
    WebSocket: class extends WebSocket {
      constructor(url: string, protocols?: string | string[]) {
        super(url.replace("127.0.0.1:0", `127.0.0.1:${target.address().port}`), protocols);
      }
    } as unknown as WebSocketConstructor,
    reconnect: { baseMs: 50, maxMs: 500, jitter: "full" },
    ...options,
  });
  clients.push(c);
  return c;
}

function subscribed(channel: Channel): Promise<void> {
  if (channel.state === "subscribed") return Promise.resolve();
  return new Promise((resolve) => channel.once("subscribed", () => resolve()));
}

function opened(c: RealtimeClient): Promise<void> {
  if (c.state === "open") return Promise.resolve();
  return new Promise((resolve) => c.once("open", () => resolve()));
}

describe("client against a real server (Node)", () => {
  it("publishes and receives in order through two nodes", async () => {
    const alice = client("alice", { node: () => a });
    const bob = client("bob", { node: () => b, codec: "msgpack" });
    const roomA = alice.subscribe("room:sdk");
    const roomB = bob.subscribe("room:sdk");
    const got: number[] = [];
    roomB.on("message", (m) => got.push((m.d as { n: number }).n));
    await Promise.all([opened(alice), opened(bob)]);
    await subscribed(roomB);
    for (let n = 1; n <= 10; n++) await roomA.publish({ n });
    await eventually(() => got.length === 10);
    expect(got).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    expect(bob.codecName).toBe("msgpack");
  });

  it("works with the built-in Node WebSocket implementation", async () => {
    const c = new RealtimeClient({
      url: `ws://127.0.0.1:${a.address().port}/v1/connect`,
      getTicket: () => getTicket(a, "native"),
    });
    clients.push(c);
    const room = c.subscribe("room:native");
    await subscribed(room);
    const ack = await room.publish({ hi: true });
    expect(ack.seq).toBe(1);
  });

  it("resumes after the connection is killed and fills the gap without duplicates", async () => {
    const reader = client("reader");
    const writer = client("writer");
    const room = reader.subscribe("room:resume-sdk");
    const writerRoom = writer.subscribe("room:resume-sdk");
    const got: ChannelMessage[] = [];
    room.on("message", (m) => got.push(m));
    await subscribed(room);
    await subscribed(writerRoom);
    await writerRoom.publish({ n: 1 });
    await eventually(() => got.length === 1);
    reader.kill();
    for (let n = 2; n <= 8; n++) await writerRoom.publish({ n });
    await eventually(() => got.length === 8, 5000);
    expect(got.map((m) => m.seq)).toEqual([1, 2, 3, 4, 5, 6, 7, 8]);
    expect(got.slice(1).some((m) => m.resumed)).toBe(true);
    expect(reader.stats.reconnects).toBeGreaterThanOrEqual(1);
    expect(reader.stats.duplicates).toBe(0);
  });

  it("queues publishes while offline and delivers them exactly once after reconnect", async () => {
    const offline = client("offline", { node: () => a });
    const watcher = client("watcher-off", { node: () => b });
    const room = offline.subscribe("room:offline");
    const watch = watcher.subscribe("room:offline");
    const got: unknown[] = [];
    watch.on("message", (m) => got.push(m.d));
    await subscribed(room);
    await subscribed(watch);
    offline.disconnect();
    const pending = [room.publish({ text: "one" }), room.publish({ text: "two" })];
    expect(offline.stats.queued).toBe(2);
    await sleep(100);
    expect(got).toEqual([]);
    offline.connect();
    const acks = await Promise.all(pending);
    expect(acks.map((x) => x.seq)).toEqual([1, 2]);
    await eventually(() => got.length === 2);
    await sleep(100);
    expect(got).toEqual([{ text: "one" }, { text: "two" }]);
  });

  it("reconnects to another node on drain and keeps the stream gapless", async () => {
    const c = await startNode(prefix, { nodeId: "cli-drain", drainMaxDelayMs: 300, drainCloseAfterMs: 2000 });
    let useDrainNode = true;
    const reader = client("drain-reader", { node: () => (useDrainNode ? c : a) });
    const room = reader.subscribe("room:drain-sdk");
    const got: number[] = [];
    room.on("message", (m) => got.push(m.seq));
    await subscribed(room);
    expect(reader.node).toBe("cli-drain");
    const drains: number[] = [];
    reader.on("drain", (d) => drains.push(d.after));
    useDrainNode = false;
    const publishing = (async () => {
      for (let i = 0; i < 20; i++) {
        await serverPublish(b, "room:drain-sdk", { i });
        await sleep(20);
      }
    })();
    await c.drain();
    await publishing;
    await eventually(() => got.length === 20, 5000);
    expect(drains).toHaveLength(1);
    expect(reader.node).toBe("cli-a");
    expect(got).toEqual(Array.from({ length: 20 }, (_, i) => i + 1));
  });

  it("recovers from a node crash by reconnecting elsewhere and resuming", async () => {
    const doomed = await startNode(prefix, { nodeId: "cli-doomed" });
    let alive = true;
    const reader = client("crash-reader", { node: () => (alive ? doomed : b) });
    const room = reader.subscribe("room:crash-sdk");
    const got: number[] = [];
    room.on("message", (m) => got.push(m.seq));
    await subscribed(room);
    await serverPublish(a, "room:crash-sdk", 1);
    await eventually(() => got.length === 1);
    alive = false;
    await doomed.crash();
    for (let i = 2; i <= 6; i++) await serverPublish(a, "room:crash-sdk", i);
    await eventually(() => got.length === 6, 5000);
    expect(got).toEqual([1, 2, 3, 4, 5, 6]);
    expect(reader.node).toBe("cli-b");
  });

  it("presence API reflects joins, meta updates and leaves", async () => {
    const one = client("p-one");
    const two = client("p-two");
    const r1 = one.subscribe("room:presence-sdk");
    await subscribed(r1);
    const r2 = two.subscribe("room:presence-sdk");
    await subscribed(r2);
    await eventually(() => r1.members.has("p-two"));
    await r2.setPresence({ name: "Two", status: "typing" });
    await eventually(() => JSON.stringify(r1.members.get("p-two")) === JSON.stringify({ name: "Two", status: "typing" }));
    two.close();
    await eventually(() => !r1.members.has("p-two"));
    expect(r1.memberList().map((m) => m.uid)).toEqual(["p-one"]);
  });

  it("receives ephemeral messages from others, throttled", async () => {
    const one = client("e-one");
    const two = client("e-two");
    const r1 = one.subscribe("room:eph-sdk");
    const r2 = two.subscribe("room:eph-sdk");
    await subscribed(r1);
    await subscribed(r2);
    const seen: unknown[] = [];
    r2.on("ephemeral", (e) => seen.push(e.d));
    for (let x = 0; x <= 30; x++) {
      r1.sendEphemeral({ x });
      await sleep(5);
    }
    await eventually(() => JSON.stringify(seen[seen.length - 1]) === JSON.stringify({ x: 30 }));
    expect(seen.length).toBeLessThan(10);
  });

  it("emits reset and continues live when history is gone", async () => {
    const tiny = await startNode(prefix, { nodeId: "cli-tiny", resumeLimit: 3 });
    const reader = client("reset-reader", { node: () => tiny });
    const room = reader.subscribe("room:reset-sdk", { from: 0 });
    for (let i = 0; i < 10; i++) await serverPublish(tiny, "room:reset-sdk", i);
    const resets: number[] = [];
    room.on("reset", (r) => resets.push(r.seq));
    await eventually(() => resets.length === 1 || (room.lastSeq ?? 0) >= 10);
    reader.close();
    const again = client("reset-reader-2", { node: () => tiny });
    const room2 = again.subscribe("room:reset-sdk", { from: 1 });
    const resets2: number[] = [];
    room2.on("reset", (r) => resets2.push(r.seq));
    await eventually(() => resets2.length === 1);
    expect(resets2).toEqual([10]);
    const got: number[] = [];
    room2.on("message", (m) => got.push(m.seq));
    await serverPublish(tiny, "room:reset-sdk", "after");
    await eventually(() => got.length === 1);
    expect(got).toEqual([11]);
    again.close();
    await tiny.stop();
  });

  it("is disconnected with 4008 when too slow and resumes immediately", async () => {
    const bp = await startNode(prefix, {
      nodeId: "cli-bp",
      backpressure: { lowBytes: 4096, highBytes: 16384, hardBytes: 10 * 1024 * 1024, maxPending: 10, lagTimeoutMs: 30000 },
    });
    class TrackedSocket extends WebSocket {
      static latest: WebSocket | null = null;
      constructor(url: string, protocols?: string | string[]) {
        super(url, protocols);
        TrackedSocket.latest = this;
      }
    }
    const reader = new RealtimeClient({
      url: `ws://127.0.0.1:${bp.address().port}/v1/connect`,
      getTicket: () => getTicket(bp, "slowpoke"),
      WebSocket: TrackedSocket as unknown as WebSocketConstructor,
      reconnect: { baseMs: 50, maxMs: 500, jitter: "full" },
    });
    clients.push(reader);
    const room = reader.subscribe("room:slow-sdk");
    const got: number[] = [];
    room.on("message", (m) => got.push(m.seq));
    await subscribed(room);
    const closes: number[] = [];
    reader.on("close", (c) => closes.push(c.code));
    const raw = (TrackedSocket.latest as unknown as { _socket: { pause(): void; resume(): void } })._socket;
    raw.pause();
    const blob = "z".repeat(16 * 1024);
    for (let i = 0; i < 100; i++) bp.publishDurable("room:slow-sdk", { i, blob }, `s-${i}`, "server").catch(() => undefined);
    await eventually(async () => ((await bp.metrics.slowConsumerDisconnects.get()).values[0]?.value ?? 0) > 0, 10000);
    raw.resume();
    await eventually(() => got.length === 100, 15000);
    expect(closes).toContain(4008);
    expect(got).toEqual(Array.from({ length: 100 }, (_, i) => i + 1));
    reader.close();
    await bp.stop();
  });
});
