import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { RealtimeClient, type ChannelMessage } from "../../src/index.js";
import { FakeSocket } from "./fake-socket.js";

let tickets = 0;

function makeClient(extra: Partial<ConstructorParameters<typeof RealtimeClient>[0]> = {}): RealtimeClient {
  return new RealtimeClient({
    url: "ws://test/v1/connect",
    getTicket: () => Promise.resolve(`t${++tickets}`),
    WebSocket: FakeSocket,
    reconnect: { baseMs: 1000, maxMs: 8000, jitter: "none" },
    ...extra,
  });
}

async function flush(): Promise<void> {
  await vi.advanceTimersByTimeAsync(0);
}

function msg(seq: number, d: unknown = seq): Record<string, unknown> {
  return { t: "msg", ch: "room:1", seq, mid: `m${seq}`, d, ts: 1, from: "u" };
}

beforeEach(() => {
  vi.useFakeTimers();
  FakeSocket.reset();
  tickets = 0;
});

afterEach(() => {
  vi.useRealTimers();
});

describe("RealtimeClient connection lifecycle", () => {
  it("goes connecting -> open and sends a fresh ticket in the URL", async () => {
    const client = makeClient();
    const states: string[] = [];
    client.on("state", (s) => states.push(s.state));
    await flush();
    const socket = FakeSocket.last();
    expect(socket.url).toBe("ws://test/v1/connect?ticket=t1");
    expect(socket.protocols).toEqual(["pulse.v1.json"]);
    socket.open();
    expect(client.state).toBe("open");
    expect(client.cid).toBe("c1");
    expect(states).toEqual(["open"]);
    client.close();
    expect(client.state).toBe("closed");
  });

  it("does not reconnect after 1000, 4003 or 4400", async () => {
    for (const code of [1000, 4003, 4400]) {
      FakeSocket.reset();
      const client = makeClient();
      await flush();
      FakeSocket.last().open();
      FakeSocket.last().serverClose(code);
      expect(client.state).toBe("closed");
      await vi.advanceTimersByTimeAsync(60000);
      expect(FakeSocket.instances).toHaveLength(1);
    }
  });

  it("reconnects immediately with a new ticket after 4001", async () => {
    makeClient();
    await flush();
    FakeSocket.last().open();
    FakeSocket.last().serverClose(4001);
    await flush();
    expect(FakeSocket.instances).toHaveLength(2);
    expect(FakeSocket.last().url).toContain("ticket=t2");
  });

  it("reconnects immediately after 4008 and resumes from lastSeq", async () => {
    const client = makeClient();
    const room = client.subscribe("room:1");
    await flush();
    const first = FakeSocket.last();
    first.open();
    expect(first.sentOf("sub")[0]).toMatchObject({ ch: "room:1" });
    first.receive({ t: "ok", id: first.sentOf("sub")[0]?.["id"], seq: 5 });
    first.receive(msg(6));
    first.serverClose(4008);
    await flush();
    const second = FakeSocket.last();
    expect(second).not.toBe(first);
    second.open("c2");
    expect(second.sentOf("sub")[0]).toMatchObject({ ch: "room:1", from: 6 });
    expect(room.lastSeq).toBe(6);
    expect(client.stats.reconnects).toBe(1);
  });

  it("backs off exponentially on abnormal closes and 4029", async () => {
    const client = makeClient();
    const delays: number[] = [];
    client.on("reconnecting", (e) => delays.push(e.delay));
    await flush();
    FakeSocket.last().serverClose(1006);
    await vi.advanceTimersByTimeAsync(1000);
    FakeSocket.last().serverClose(4029);
    await vi.advanceTimersByTimeAsync(2000);
    FakeSocket.last().serverClose(1006);
    expect(delays).toEqual([1000, 2000, 4000]);
    await vi.advanceTimersByTimeAsync(4000);
    FakeSocket.last().open();
    FakeSocket.last().serverClose(1006);
    expect(delays[3]).toBe(1000);
  });

  it("handles drain by reconnecting after the server-provided delay", async () => {
    const client = makeClient();
    await flush();
    const first = FakeSocket.last();
    first.open();
    first.receive({ t: "drain", after: 3000 });
    first.serverClose(1001);
    await vi.advanceTimersByTimeAsync(2999);
    expect(FakeSocket.instances).toHaveLength(1);
    await vi.advanceTimersByTimeAsync(2);
    expect(FakeSocket.instances).toHaveLength(2);
    FakeSocket.last().open("c2");
    expect(client.state).toBe("open");
  });

  it("closes the old socket itself when the drain delay elapses first", async () => {
    makeClient();
    await flush();
    const first = FakeSocket.last();
    first.open();
    first.receive({ t: "drain", after: 100 });
    await vi.advanceTimersByTimeAsync(101);
    expect(first.closedWith?.code).toBe(1000);
    expect(FakeSocket.instances).toHaveLength(2);
  });

  it("treats a missing app-level pong as a dead connection", async () => {
    const client = makeClient({ pingIntervalMs: 1000, pongTimeoutMs: 500 });
    await flush();
    FakeSocket.last().open();
    await vi.advanceTimersByTimeAsync(1000);
    expect(FakeSocket.last().sentOf("ping")).toHaveLength(1);
    await vi.advanceTimersByTimeAsync(501);
    expect(client.state).toBe("reconnecting");
  });

  it("keeps the connection when pongs arrive", async () => {
    const client = makeClient({ pingIntervalMs: 1000, pongTimeoutMs: 500 });
    await flush();
    const socket = FakeSocket.last();
    socket.open();
    for (let i = 0; i < 5; i++) {
      await vi.advanceTimersByTimeAsync(1000);
      socket.receive({ t: "pong", ts: 0 });
    }
    expect(client.state).toBe("open");
  });

  it("retries when the ticket request fails", async () => {
    let fail = true;
    const client = makeClient({
      getTicket: () => (fail ? Promise.reject(new Error("down")) : Promise.resolve("ok")),
    });
    const errors: string[] = [];
    client.on("error", (e) => errors.push(e.message));
    await flush();
    expect(FakeSocket.instances).toHaveLength(0);
    fail = false;
    await vi.advanceTimersByTimeAsync(1000);
    expect(FakeSocket.instances).toHaveLength(1);
    expect(errors[0]).toContain("ticket request failed");
  });
});

describe("channels: dedupe, gaps, resume", () => {
  it("drops duplicates and resubscribes from lastSeq on a gap", async () => {
    const client = makeClient();
    const room = client.subscribe("room:1");
    const got: number[] = [];
    const gaps: unknown[] = [];
    room.on("message", (m) => got.push(m.seq));
    room.on("gap", (g) => gaps.push(g));
    await flush();
    const socket = FakeSocket.last();
    socket.open();
    socket.receive({ t: "ok", id: socket.sentOf("sub")[0]?.["id"], seq: 0 });
    socket.receive(msg(1));
    socket.receive(msg(2));
    socket.receive(msg(2));
    socket.receive(msg(1));
    socket.receive(msg(4));
    expect(got).toEqual([1, 2]);
    expect(client.stats.duplicates).toBe(2);
    expect(gaps).toEqual([{ expected: 3, got: 4 }]);
    const resub = socket.sentOf("sub")[1];
    expect(resub).toMatchObject({ ch: "room:1", from: 2 });
    socket.receive(msg(5));
    socket.receive(msg(3));
    socket.receive(msg(4));
    socket.receive({ t: "ok", id: resub?.["id"], seq: 4 });
    socket.receive(msg(5));
    expect(got).toEqual([1, 2, 3, 4, 5]);
  });

  it("marks messages delivered during a resume as resumed", async () => {
    const client = makeClient();
    const room = client.subscribe("room:1", { from: 10 });
    const got: ChannelMessage[] = [];
    room.on("message", (m) => got.push(m));
    await flush();
    const socket = FakeSocket.last();
    socket.open();
    const sub = socket.sentOf("sub")[0];
    expect(sub).toMatchObject({ from: 10 });
    socket.receive(msg(11));
    socket.receive(msg(12));
    socket.receive({ t: "ok", id: sub?.["id"], seq: 12 });
    socket.receive(msg(13));
    expect(got.map((m) => [m.seq, m.resumed])).toEqual([
      [11, true],
      [12, true],
      [13, false],
    ]);
    expect(client.stats.resumedMessages).toBe(2);
  });

  it("applies reset by moving lastSeq and emitting reset", async () => {
    const client = makeClient();
    const room = client.subscribe("room:1", { from: 3 });
    const resets: unknown[] = [];
    room.on("reset", (r) => resets.push(r));
    await flush();
    const socket = FakeSocket.last();
    socket.open();
    socket.receive({ t: "reset", ch: "room:1", seq: 900 });
    socket.receive({ t: "ok", id: socket.sentOf("sub")[0]?.["id"], seq: 900 });
    socket.receive(msg(901));
    expect(resets).toEqual([{ seq: 900 }]);
    expect(room.lastSeq).toBe(901);
  });

  it("requests history only on the first subscribe", async () => {
    const client = makeClient();
    client.subscribe("room:1", { history: 20 });
    await flush();
    const socket = FakeSocket.last();
    socket.open();
    expect(socket.sentOf("sub")[0]).toMatchObject({ history: 20 });
    socket.receive({ t: "ok", id: socket.sentOf("sub")[0]?.["id"], seq: 0 });
    socket.serverClose(1006);
    await vi.advanceTimersByTimeAsync(1000);
    FakeSocket.last().open();
    expect(FakeSocket.last().sentOf("sub")[0]).toMatchObject({ from: 0 });
    expect(FakeSocket.last().sentOf("sub")[0]?.["history"]).toBeUndefined();
  });

  it("tracks presence by user", async () => {
    const client = makeClient();
    const room = client.subscribe("room:1");
    await flush();
    const socket = FakeSocket.last();
    socket.open();
    socket.receive({ t: "ok", id: socket.sentOf("sub")[0]?.["id"], seq: 0, presence: [{ uid: "a", meta: { name: "A" } }], pn: 1 });
    socket.receive({ t: "pj", ch: "room:1", uid: "b" });
    socket.receive({ t: "pu", ch: "room:1", uid: "a", meta: { status: "away" } });
    expect(room.memberList()).toEqual([{ uid: "a", meta: { status: "away" } }, { uid: "b" }]);
    socket.receive({ t: "pl", ch: "room:1", uid: "a" });
    expect(room.memberList()).toEqual([{ uid: "b" }]);
  });

  it("ignores unknown frame types for forward compatibility", async () => {
    const client = makeClient();
    await flush();
    const socket = FakeSocket.last();
    socket.open();
    socket.receive({ t: "v2-feature", x: 1 });
    expect(client.state).toBe("open");
  });
});

describe("offline publish queue", () => {
  it("queues publishes while offline and sends them once with the same cmid after reconnect", async () => {
    const client = makeClient();
    await flush();
    const first = FakeSocket.last();
    first.open();
    const p1 = client.publish("room:1", { text: "a" });
    const sent = first.sentOf("pub")[0];
    first.serverClose(1006);
    const p2 = client.publish("room:1", { text: "b" });
    expect(client.stats.queued).toBe(2);
    await vi.advanceTimersByTimeAsync(1000);
    const second = FakeSocket.last();
    second.open("c2");
    const pubs = second.sentOf("pub");
    expect(pubs.map((p) => p["d"])).toEqual([{ text: "a" }, { text: "b" }]);
    expect(pubs[0]?.["cmid"]).toBe(sent?.["cmid"]);
    second.receive({ t: "ok", id: pubs[0]?.["id"], seq: 7, mid: "m7", dup: true });
    second.receive({ t: "ok", id: pubs[1]?.["id"], seq: 8, mid: "m8" });
    await expect(p1).resolves.toEqual({ seq: 7, mid: "m7", dup: true });
    await expect(p2).resolves.toEqual({ seq: 8, mid: "m8", dup: false });
    expect(client.stats.queued).toBe(0);
  });

  it("rejects when the queue is full and on server errors", async () => {
    const client = makeClient({ offlineQueueLimit: 1 });
    const first = client.publish("room:1", 1);
    await expect(client.publish("room:1", 2)).rejects.toMatchObject({ code: "QUEUE_FULL" });
    await flush();
    const socket = FakeSocket.last();
    socket.open();
    const pub = socket.sentOf("pub")[0];
    socket.receive({ t: "err", id: pub?.["id"], code: "FORBIDDEN", msg: "no" });
    await expect(first).rejects.toMatchObject({ code: "FORBIDDEN" });
  });
});

describe("ephemeral throttle", () => {
  it("sends at most one ephemeral per window per channel, keeping the latest value", async () => {
    const client = makeClient({ ephemeralThrottleMs: 50 });
    const room = client.subscribe("room:1");
    await flush();
    const socket = FakeSocket.last();
    socket.open();
    for (let x = 0; x < 10; x++) room.sendEphemeral({ x });
    expect(socket.sentOf("eph")).toHaveLength(1);
    await vi.advanceTimersByTimeAsync(50);
    expect(socket.sentOf("eph").map((f) => f["d"])).toEqual([{ x: 0 }, { x: 9 }]);
  });

  it("drops ephemeral messages while disconnected", async () => {
    const client = makeClient();
    const room = client.subscribe("room:1");
    room.sendEphemeral({ x: 1 });
    await flush();
    const socket = FakeSocket.last();
    socket.open();
    expect(socket.sentOf("eph")).toHaveLength(0);
  });
});
