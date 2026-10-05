import { afterAll, beforeAll, describe, expect, it } from "vitest";
import fc from "fast-check";
import type { RealtimeServer } from "@ashamrai/realtime-server";
import { eventually, purgePrefix, sleep, startNode, TestClient, uniquePrefix } from "../../../../test-support/helpers.js";

const prefix = uniquePrefix();
let nodes: RealtimeServer[] = [];

beforeAll(async () => {
  nodes = [
    await startNode(prefix, { nodeId: "c-1" }),
    await startNode(prefix, { nodeId: "c-2" }),
    await startNode(prefix, { nodeId: "c-3" }),
  ];
});

afterAll(async () => {
  for (const node of nodes) await node.stop().catch(() => undefined);
  await purgePrefix(prefix);
});

function node(i: number): RealtimeServer {
  return nodes[i % nodes.length] as RealtimeServer;
}

describe("cross-node ordering", () => {
  it("a publish on node A arrives in the same order on nodes B and C", async () => {
    const subs = await Promise.all([TestClient.connect(node(1), "b"), TestClient.connect(node(2), "c")]);
    for (const s of subs) await s.sub("room:order");
    const pub = await TestClient.connect(node(0), "a");
    for (let i = 1; i <= 30; i++) pub.send({ t: "pub", id: i, ch: "room:order", cmid: `o-${i}`, d: i });
    for (const s of subs) await s.waitForSeq("room:order", 30);
    expect(subs[0]?.messages("room:order")).toEqual(subs[1]?.messages("room:order"));
    expect(subs[0]?.messages("room:order").map((m) => m.seq)).toEqual(Array.from({ length: 30 }, (_, i) => i + 1));
    for (const c of [...subs, pub]) c.close();
  });

  it("property: N concurrent publishers on different nodes produce one identical gapless sequence for every subscriber", async () => {
    let run = 0;
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 2, max: 6 }),
        fc.array(fc.integer({ min: 1, max: 15 }), { minLength: 2, maxLength: 6 }),
        fc.integer({ min: 2, max: 6 }),
        async (subscriberCount, perPublisher, codecSeed) => {
          run++;
          const ch = `room:prop-${run}`;
          const subscribers: TestClient[] = [];
          for (let i = 0; i < subscriberCount; i++) {
            const client = await TestClient.connect(node(i), `sub-${run}-${i}`);
            await client.sub(ch, { presence: (i + codecSeed) % 2 === 0 });
            subscribers.push(client);
          }
          const publishers: TestClient[] = [];
          for (let p = 0; p < perPublisher.length; p++) publishers.push(await TestClient.connect(node(p + 1), `pub-${run}-${p}`));
          await Promise.all(
            publishers.map(async (publisher, p) => {
              const count = perPublisher[p] as number;
              const acks: Promise<unknown>[] = [];
              for (let k = 0; k < count; k++) acks.push(publisher.pub(ch, { p, k }));
              await Promise.all(acks);
            }),
          );
          const total = perPublisher.reduce((a, b) => a + b, 0);
          for (const s of subscribers) await s.waitForSeq(ch, total, 10000);
          const reference = subscribers[0]?.messages(ch) ?? [];
          const ok =
            reference.length === total &&
            reference.every((m, i) => m.seq === i + 1) &&
            subscribers.every((s) => JSON.stringify(s.messages(ch)) === JSON.stringify(reference));
          const perPublisherOrder = perPublisher.every((count, p) => {
            const ks = reference.filter((m) => (m.d as { p: number }).p === p).map((m) => (m.d as { k: number }).k);
            return ks.length === count;
          });
          for (const c of [...subscribers, ...publishers]) c.close();
          return ok && perPublisherOrder;
        },
      ),
      { numRuns: 12 },
    );
  });
});

describe("presence", () => {
  it("is per user: several tabs show up once, leave fires after the last tab", async () => {
    const observer = await TestClient.connect(node(0), "observer");
    await observer.sub("room:tabs");
    const tab1 = await TestClient.connect(node(1), "multi");
    const ok1 = await tab1.sub("room:tabs");
    const tab2 = await TestClient.connect(node(2), "multi");
    const ok2 = await tab2.sub("room:tabs");
    expect(ok1.t === "ok" && ok1.presence?.map((m) => m.uid).sort()).toEqual(["multi", "observer"]);
    expect(ok2.t === "ok" && ok2.pn).toBe(2);
    await observer.waitFor((f) => f.t === "pj" && f.uid === "multi");
    await sleep(200);
    expect(observer.frames.filter((f) => f.t === "pj" && f.uid === "multi")).toHaveLength(1);
    tab1.close();
    await sleep(300);
    expect(observer.frames.some((f) => f.t === "pl")).toBe(false);
    tab2.close();
    await observer.waitFor((f) => f.t === "pl" && f.uid === "multi");
    observer.close();
  });

  it("broadcasts presence meta updates and supports opting out", async () => {
    const a = await TestClient.connect(node(0), "meta-a", { extraClaims: { meta: { color: "#f00" } } });
    const ok = await a.sub("room:meta");
    expect(ok.t === "ok" && ok.presence).toEqual([{ uid: "meta-a", meta: { name: "meta-a", color: "#f00" } }]);
    const ghost = await TestClient.connect(node(1), "ghost");
    const ghostOk = await ghost.sub("room:meta", { presence: false });
    expect(ghostOk.t === "ok" && ghostOk.presence).toBeUndefined();
    const b = await TestClient.connect(node(1), "meta-b");
    await b.sub("room:meta");
    expect(await a.request({ t: "pres", ch: "room:meta", meta: { status: "away" } })).toMatchObject({ t: "ok" });
    expect(await b.waitFor((f) => f.t === "pu")).toEqual({ t: "pu", ch: "room:meta", uid: "meta-a", meta: { status: "away" } });
    const late = await TestClient.connect(node(2), "meta-c");
    const lateOk = await late.sub("room:meta");
    const members = lateOk.t === "ok" ? lateOk.presence ?? [] : [];
    expect(members.find((m) => m.uid === "meta-a")?.meta).toEqual({ status: "away" });
    expect(members.some((m) => m.uid === "ghost")).toBe(false);
    await sleep(100);
    expect(ghost.frames.some((f) => f.t === "pj" || f.t === "pu")).toBe(false);
    for (const c of [a, b, ghost, late]) c.close();
  });

  it("removes presence of a node that died without cleanup", async () => {
    const doomed = await startNode(prefix, { nodeId: "doomed" });
    const observer = await TestClient.connect(node(0), "watcher");
    await observer.sub("room:crash");
    const victim = await TestClient.connect(doomed, "victim");
    await victim.sub("room:crash");
    await observer.waitFor((f) => f.t === "pj" && f.uid === "victim");
    const started = Date.now();
    await doomed.crash();
    await observer.waitFor((f) => f.t === "pl" && f.uid === "victim", 10000);
    const elapsed = Date.now() - started;
    expect(elapsed).toBeLessThan(6000);
    const fresh = await TestClient.connect(node(1), "fresh");
    const ok = await fresh.sub("room:crash");
    expect(ok.t === "ok" && ok.presence?.map((m) => m.uid).sort()).toEqual(["fresh", "watcher"]);
    observer.close();
    fresh.close();
  });

  it("expires presence entries whose heartbeat stopped even if the node record is alive", async () => {
    const shortTtl = await startNode(prefix, { nodeId: "short-ttl", presenceTtlMs: 800, presenceRefreshMs: 60000 });
    const observer = await TestClient.connect(node(0), "w-exp");
    await observer.sub("room:exp");
    const stale = await TestClient.connect(shortTtl, "stale");
    await stale.sub("room:exp");
    await observer.waitFor((f) => f.t === "pj" && f.uid === "stale");
    await observer.waitFor((f) => f.t === "pl" && f.uid === "stale", 8000);
    stale.close();
    observer.close();
    await shortTtl.stop();
  });
});

describe("slow consumers", () => {
  it("a slow client does not affect a fast one and is disconnected with 4008, then resumes without loss", async () => {
    const bpNode = await startNode(prefix, {
      nodeId: "bp",
      backpressure: { lowBytes: 16 * 1024, highBytes: 64 * 1024, hardBytes: 256 * 1024, maxPending: 200, lagTimeoutMs: 30000 },
      rate: { pubPerSec: 10000, pubBurst: 10000 },
    });
    const fast = await TestClient.connect(bpNode, "fast");
    const slow = await TestClient.connect(bpNode, "slow");
    const pub = await TestClient.connect(bpNode, "pub");
    await fast.sub("room:bp");
    await slow.sub("room:bp");
    slow.pauseReading();
    const blob = "x".repeat(8 * 1024);
    const latencies: number[] = [];
    let n = 0;
    const deadline = Date.now() + 15000;
    while (((await bpNode.metrics.slowConsumerDisconnects.get()).values[0]?.value ?? 0) === 0) {
      if (Date.now() > deadline) throw new Error("slow consumer was never disconnected");
      n++;
      const sentAt = Date.now();
      await pub.pub("room:bp", { n, blob });
      await fast.waitForSeq("room:bp", n);
      latencies.push(Date.now() - sentAt);
    }
    for (let i = 0; i < 20; i++) {
      n++;
      const sentAt = Date.now();
      await pub.pub("room:bp", { n, blob });
      await fast.waitForSeq("room:bp", n);
      latencies.push(Date.now() - sentAt);
    }
    const sorted = [...latencies].sort((a, b) => a - b);
    const p99 = sorted[Math.floor(sorted.length * 0.99)] ?? 0;
    expect(p99).toBeLessThan(200);
    expect(fast.messages("room:bp").map((m) => m.seq)).toEqual(Array.from({ length: n }, (_, i) => i + 1));
    expect(fast.closeCode).toBeNull();
    const heapPending = Array.from(bpNode.allConnections()).reduce((acc, c) => acc + c.pendingCount, 0);
    expect(heapPending).toBeLessThanOrEqual(200);
    slow.resumeReading();
    const code = await slow.waitClose(10000);
    expect([4008, 1006]).toContain(code);
    const got = slow.messages("room:bp");
    const lastSeq = got.length === 0 ? 0 : (got[got.length - 1]?.seq ?? 0);
    expect(got.map((m) => m.seq)).toEqual(Array.from({ length: lastSeq }, (_, i) => i + 1));
    const back = await TestClient.connect(bpNode, "slow");
    await back.sub("room:bp", { from: lastSeq });
    await back.waitForSeq("room:bp", n, 10000);
    const resumed = back.messages("room:bp").map((m) => m.seq);
    expect([...got.map((m) => m.seq), ...resumed]).toEqual(Array.from({ length: n }, (_, i) => i + 1));
    for (const c of [fast, pub, back]) c.close();
    await bpNode.stop();
  });

  it("the slow client receives close code 4008 when it reads its backlog in time", async () => {
    const bpNode = await startNode(prefix, {
      nodeId: "bp2",
      backpressure: { lowBytes: 8 * 1024, highBytes: 32 * 1024, hardBytes: 10 * 1024 * 1024, maxPending: 20, lagTimeoutMs: 30000 },
    });
    const slow = await TestClient.connect(bpNode, "slow2");
    await slow.sub("room:bp2");
    slow.pauseReading();
    const blob = "y".repeat(16 * 1024);
    for (let i = 0; i < 200; i++) {
      bpNode.publishDurable("room:bp2", { i, blob }, `c-${i}`, "server").catch(() => undefined);
    }
    await eventually(async () => ((await bpNode.metrics.slowConsumerDisconnects.get()).values[0]?.value ?? 0) > 0, 10000);
    slow.resumeReading();
    expect(await slow.waitClose(5000)).toBe(4008);
    await bpNode.stop();
  });
});

describe("drain", () => {
  it("marks the node not ready, sends jittered drain frames and closes stragglers with 1001", async () => {
    const draining = await startNode(prefix, { nodeId: "drainer", drainMaxDelayMs: 2000, drainCloseAfterMs: 1500 });
    const clients: TestClient[] = [];
    for (let i = 0; i < 40; i++) clients.push(await TestClient.connect(draining, `d-${i}`));
    const done = draining.drain();
    await eventually(() => clients.every((c) => c.frames.some((f) => f.t === "drain")), 3000);
    const ready = await fetch(`http://127.0.0.1:${draining.address().port}/health/ready`);
    expect(ready.status).toBe(503);
    await expect(TestClient.connect(draining, "latecomer")).rejects.toThrow(/503/);
    const afters = clients.map((c) => {
      const f = c.frames.find((x) => x.t === "drain");
      return f !== undefined && f.t === "drain" ? f.after : -1;
    });
    expect(afters.every((a) => a >= 0 && a < 2000)).toBe(true);
    expect(new Set(afters).size).toBeGreaterThan(30);
    const mean = afters.reduce((a, b) => a + b, 0) / afters.length;
    expect(mean).toBeGreaterThan(500);
    expect(mean).toBeLessThan(1500);
    for (const c of clients.slice(0, 20)) c.close();
    await done;
    const codes = await Promise.all(clients.slice(20).map((c) => c.waitClose()));
    expect(codes.every((code) => code === 1001)).toBe(true);
  });
});
