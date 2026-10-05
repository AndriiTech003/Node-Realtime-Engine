import { afterAll, beforeAll, describe, expect, it } from "vitest";
import WebSocket from "ws";
import type { RealtimeServer } from "@ashamrai/realtime-server";
import {
  baseUrl,
  getTicket,
  msgpackCodec,
  purgePrefix,
  serverPublish,
  sleep,
  startNode,
  TestClient,
  tokenFor,
  uniquePrefix,
} from "../../../../test-support/helpers.js";

const prefix = uniquePrefix();
let node: RealtimeServer;

beforeAll(async () => {
  node = await startNode(prefix, { rate: { violationsToClose: 5 } });
});

afterAll(async () => {
  await node.stop();
  await purgePrefix(prefix);
});

function rawUpgrade(path: string, headers: Record<string, string> = {}): Promise<number> {
  return new Promise((resolve) => {
    const ws = new WebSocket(`ws://127.0.0.1:${node.address().port}${path}`, "pulse.v1.json", { headers });
    ws.on("unexpected-response", (_req, res) => {
      resolve(res.statusCode ?? 0);
      ws.terminate();
    });
    ws.on("open", () => {
      resolve(101);
      ws.close();
    });
    ws.on("error", () => undefined);
  });
}

describe("single node pub/sub", () => {
  it("two clients in one room exchange messages in seq order", async () => {
    const a = await TestClient.connect(node, "ann");
    const b = await TestClient.connect(node, "bob");
    const okA = await a.sub("room:chat");
    const okB = await b.sub("room:chat");
    expect(okA).toMatchObject({ t: "ok", seq: 0 });
    expect(okB).toMatchObject({ t: "ok", seq: 0 });
    for (let i = 1; i <= 5; i++) {
      const ok = await a.pub("room:chat", { text: `m${i}` });
      expect(ok).toMatchObject({ t: "ok", seq: i });
    }
    await b.waitForSeq("room:chat", 5);
    expect(b.messages("room:chat")).toEqual([1, 2, 3, 4, 5].map((i) => ({ seq: i, d: { text: `m${i}` }, from: "ann" })));
    a.close();
    b.close();
  });

  it("serves JSON and MessagePack clients from the same fan-out", async () => {
    const j = await TestClient.connect(node, "json-user");
    const m = await TestClient.connect(node, "mp-user", { codec: msgpackCodec });
    expect(m.ws.protocol).toBe("pulse.v1.msgpack");
    await j.sub("room:codec");
    await m.sub("room:codec");
    await j.pub("room:codec", { n: 1, nested: { list: [1, 2, 3] } });
    await m.waitForSeq("room:codec", 1);
    await j.waitForSeq("room:codec", 1);
    expect(m.messages("room:codec")).toEqual(j.messages("room:codec"));
    j.close();
    m.close();
  });

  it("returns history on sub with history: n", async () => {
    const a = await TestClient.connect(node, "hist");
    for (let i = 0; i < 5; i++) await a.pub("room:hist", { i });
    const b = await TestClient.connect(node, "hist2");
    const ok = await b.sub("room:hist", { history: 3 });
    expect(ok).toMatchObject({ seq: 5 });
    expect(b.messages("room:hist").map((m) => m.seq)).toEqual([3, 4, 5]);
    a.close();
    b.close();
  });

  it("stops delivery after unsub and reports NOT_SUBSCRIBED", async () => {
    const a = await TestClient.connect(node, "u1");
    const b = await TestClient.connect(node, "u2");
    await a.sub("room:unsub");
    await b.sub("room:unsub");
    expect(await b.request({ t: "unsub", ch: "room:unsub" })).toMatchObject({ t: "ok" });
    expect(await b.request({ t: "unsub", ch: "room:unsub" })).toMatchObject({ t: "err", code: "NOT_SUBSCRIBED" });
    await a.pub("room:unsub", "x");
    await a.waitForSeq("room:unsub", 1);
    await sleep(100);
    expect(b.messages("room:unsub")).toHaveLength(0);
    a.close();
    b.close();
  });

  it("enforces channel types and permissions", async () => {
    const a = await TestClient.connect(node, "perm");
    expect(await a.sub("queue:1")).toMatchObject({ t: "err", code: "UNKNOWN_CHANNEL_TYPE" });
    expect(await a.sub("user:someone-else")).toMatchObject({ t: "err", code: "FORBIDDEN" });
    expect(await a.sub("user:perm")).toMatchObject({ t: "ok" });
    expect(await a.pub("user:perm", "x")).toMatchObject({ t: "err", code: "FORBIDDEN" });
    expect(await a.sub("broadcast:news")).toMatchObject({ t: "ok" });
    expect(await a.pub("broadcast:news", "x")).toMatchObject({ t: "err", code: "FORBIDDEN" });
    a.close();
  });

  it("limits subscriptions per connection to 100", async () => {
    const a = await TestClient.connect(node, "many-subs");
    const results = await Promise.all(Array.from({ length: 100 }, (_, i) => a.sub(`room:lim-${i}`)));
    expect(results.every((r) => r.t === "ok")).toBe(true);
    expect(await a.sub("room:lim-100")).toMatchObject({ t: "err", code: "TOO_MANY_SUBSCRIPTIONS" });
    a.close();
  });

  it("server publish API reaches private user channels and needs the server key", async () => {
    const a = await TestClient.connect(node, "inbox");
    await a.sub("user:inbox");
    const result = await serverPublish(node, "user:inbox", { notice: "order shipped" });
    expect(result.seq).toBe(1);
    await a.waitForSeq("user:inbox", 1);
    expect(a.messages("user:inbox")[0]).toEqual({ seq: 1, d: { notice: "order shipped" }, from: "server" });
    const denied = await fetch(`${baseUrl(node)}/v1/publish`, {
      method: "POST",
      headers: { "x-api-key": "wrong", "content-type": "application/json" },
      body: JSON.stringify({ ch: "user:inbox", d: 1 }),
    });
    expect(denied.status).toBe(401);
    const bad = await fetch(`${baseUrl(node)}/v1/publish`, {
      method: "POST",
      headers: { authorization: "Bearer test-server-key", "content-type": "application/json" },
      body: JSON.stringify({ ch: "nope:1", d: 1 }),
    });
    expect(bad.status).toBe(400);
    a.close();
  });

  it("serves history over HTTP for reset recovery", async () => {
    await serverPublish(node, "room:http-hist", { a: 1 });
    await serverPublish(node, "room:http-hist", { a: 2 });
    const res = await fetch(`${baseUrl(node)}/v1/history?ch=room:http-hist&limit=10`, {
      headers: { authorization: `Bearer ${tokenFor("reader")}` },
    });
    expect(res.status).toBe(200);
    const body = (await res.json()) as { seq: number; messages: { seq: number; d: unknown }[] };
    expect(body.seq).toBe(2);
    expect(body.messages.map((m) => m.d)).toEqual([{ a: 1 }, { a: 2 }]);
    const forbidden = await fetch(`${baseUrl(node)}/v1/history?ch=user:other`, {
      headers: { authorization: `Bearer ${tokenFor("reader")}` },
    });
    expect(forbidden.status).toBe(403);
  });

  it("delivers ephemeral messages coalesced to the latest value, not echoed to the sender", async () => {
    const a = await TestClient.connect(node, "cursor-a");
    const b = await TestClient.connect(node, "cursor-b");
    await a.sub("room:eph");
    await b.sub("room:eph");
    for (let x = 0; x < 20; x++) a.send({ t: "eph", ch: "room:eph", d: { x } });
    await b.waitFor((f) => f.t === "eph" && (f.d as { x: number }).x === 19);
    const ephs = b.frames.filter((f) => f.t === "eph");
    expect(ephs.length).toBeLessThanOrEqual(3);
    expect(ephs[ephs.length - 1]).toMatchObject({ t: "eph", ch: "room:eph", from: "cursor-a", d: { x: 19 } });
    await sleep(100);
    expect(a.frames.some((f) => f.t === "eph")).toBe(false);
    a.send({ t: "eph", ch: "room:not-subscribed", d: 1 });
    await a.waitFor((f) => f.t === "err" && f.code === "NOT_SUBSCRIBED");
    a.close();
    b.close();
  });

  it("answers app-level ping", async () => {
    const a = await TestClient.connect(node, "pinger");
    a.send({ t: "ping", ts: 123 });
    expect(await a.waitFor((f) => f.t === "pong")).toEqual({ t: "pong", ts: 123 });
    a.close();
  });
});

describe("upgrade and connection guards", () => {
  it("rejects missing, unknown and reused tickets with HTTP 401 before upgrade", async () => {
    expect(await rawUpgrade("/v1/connect")).toBe(401);
    expect(await rawUpgrade("/v1/connect?ticket=nope")).toBe(401);
    const ticket = await getTicket(node, "once");
    expect(await rawUpgrade(`/v1/connect?ticket=${ticket}`)).toBe(101);
    expect(await rawUpgrade(`/v1/connect?ticket=${ticket}`)).toBe(401);
    expect(await rawUpgrade("/other")).toBe(404);
  });

  it("checks Origin against the allowlist", async () => {
    expect(await rawUpgrade(`/v1/connect?ticket=${await getTicket(node, "o1")}`, { origin: "http://evil.test" })).toBe(403);
    expect(await rawUpgrade(`/v1/connect?ticket=${await getTicket(node, "o2")}`, { origin: "http://allowed.test" })).toBe(101);
  });

  it("rejects ticket requests without a valid JWT", async () => {
    const res = await fetch(`${baseUrl(node)}/v1/tickets`, { method: "POST", headers: { authorization: "Bearer bad.token.x" } });
    expect(res.status).toBe(401);
  });

  it("closes with 4400 on unknown frame types and undecodable frames", async () => {
    const a = await TestClient.connect(node, "bad1");
    a.send({ t: "explode" });
    expect(await a.waitClose()).toBe(4400);
    const b = await TestClient.connect(node, "bad2");
    b.ws.send("{not json");
    expect(await b.waitClose()).toBe(4400);
  });

  it("answers BAD_REQUEST for a known frame with invalid fields", async () => {
    const a = await TestClient.connect(node, "bad3");
    a.send({ t: "sub", id: 9, ch: "" });
    expect(await a.waitFor((f) => f.t === "err")).toMatchObject({ id: 9, code: "BAD_REQUEST" });
    a.close();
  });

  it("closes with 1009 on frames above maxPayload", async () => {
    const a = await TestClient.connect(node, "big");
    a.send({ t: "pub", id: 1, ch: "room:big", cmid: "c1", d: "x".repeat(70 * 1024) });
    expect(await a.waitClose()).toBe(1009);
  });

  it("rate limits publishes and closes with 4029 on systematic abuse", async () => {
    const a = await TestClient.connect(node, "spammer");
    for (let i = 0; i < 60; i++) a.send({ t: "pub", id: i + 1, ch: "room:spam", cmid: `c${i}`, d: i });
    await a.waitFor((f) => f.t === "err" && f.code === "RATE_LIMITED");
    expect(await a.waitClose()).toBe(4029);
  });

  it("limits connections per user with 4029", async () => {
    const clients: TestClient[] = [];
    for (let i = 0; i < 10; i++) clients.push(await TestClient.connect(node, "tabby"));
    const extra = await TestClient.connect(node, "tabby");
    expect(await extra.waitClose()).toBe(4029);
    for (const c of clients) expect(c.closeCode).toBeNull();
    for (const c of clients) c.close();
  });

  it("exposes health and Prometheus metrics", async () => {
    expect((await fetch(`${baseUrl(node)}/health/live`)).status).toBe(200);
    expect((await fetch(`${baseUrl(node)}/health/ready`)).status).toBe(200);
    const metrics = await (await fetch(`${baseUrl(node)}/metrics`)).text();
    for (const name of [
      "rt_connections",
      "rt_subscriptions",
      "rt_channels_local",
      "rt_messages_in_total",
      "rt_messages_out_total",
      "rt_fanout_duration_seconds",
      "rt_ws_buffered_bytes",
      "rt_slow_consumer_disconnects_total",
      "rt_ephemeral_dropped_total",
      "rt_resume_messages",
      "rt_redis_command_seconds",
      "nodejs_eventloop_lag_p99_seconds",
      "rt_event_loop_utilization",
      "nodejs_heap_size_used_bytes",
      "rt_gc_pause_seconds",
    ]) {
      expect(metrics, name).toContain(`# TYPE ${name}`);
    }
  });

  it("toggles debug logging for one connection through the admin endpoint", async () => {
    const a = await TestClient.connect(node, "debuggee");
    const hello = a.frames[0] as { cid: string };
    const res = await fetch(`${baseUrl(node)}/admin/debug`, {
      method: "POST",
      headers: { "x-api-key": "test-server-key", "content-type": "application/json" },
      body: JSON.stringify({ cid: hello.cid, enabled: true }),
    });
    expect(await res.json()).toEqual({ cid: hello.cid, debug: true });
    expect(node.connection(hello.cid)?.log.level).toBe("debug");
    const unauthorized = await fetch(`${baseUrl(node)}/admin/debug`, { method: "POST", body: "{}" });
    expect(unauthorized.status).toBe(401);
    a.close();
  });
});

describe("load shedding", () => {
  it("answers 503 on upgrade when the node is at its connection limit", async () => {
    const small = await startNode(uniquePrefix(), { maxConnections: 2 });
    const a = await TestClient.connect(small, "s1");
    const b = await TestClient.connect(small, "s2");
    await expect(TestClient.connect(small, "s3")).rejects.toThrow(/503/);
    a.close();
    b.close();
    await small.stop();
  });
});
