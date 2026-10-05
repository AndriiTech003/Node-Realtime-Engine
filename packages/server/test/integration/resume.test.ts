import { afterAll, beforeAll, describe, expect, it } from "vitest";
import type { RealtimeServer } from "@ashamrai/realtime-server";
import { purgePrefix, serverPublish, sleep, startNode, TestClient, uniquePrefix } from "../../../../test-support/helpers.js";

const prefix = uniquePrefix();
let nodeA: RealtimeServer;
let nodeB: RealtimeServer;

beforeAll(async () => {
  nodeA = await startNode(prefix, { nodeId: "resume-a" });
  nodeB = await startNode(prefix, { nodeId: "resume-b" });
});

afterAll(async () => {
  await nodeA.stop();
  await nodeB.stop();
  await purgePrefix(prefix);
});

describe("resume after reconnect", () => {
  it("a disconnected client catches up with history, then ok, then live", async () => {
    const writer = await TestClient.connect(nodeA, "writer");
    const reader = await TestClient.connect(nodeA, "reader");
    await writer.sub("room:resume");
    await reader.sub("room:resume");
    await writer.pub("room:resume", { n: 1 });
    await reader.waitForSeq("room:resume", 1);
    reader.close();
    for (let n = 2; n <= 6; n++) await writer.pub("room:resume", { n });
    const again = await TestClient.connect(nodeA, "reader");
    const ok = await again.sub("room:resume", { from: 1 });
    expect(ok).toMatchObject({ t: "ok", seq: 6 });
    const okIndex = again.frames.indexOf(ok);
    const msgsBeforeOk = again.frames.slice(0, okIndex).filter((f) => f.t === "msg");
    expect(msgsBeforeOk.map((f) => (f.t === "msg" ? f.seq : 0))).toEqual([2, 3, 4, 5, 6]);
    await writer.pub("room:resume", { n: 7 });
    await again.waitForSeq("room:resume", 7);
    expect(again.messages("room:resume").map((m) => m.seq)).toEqual([2, 3, 4, 5, 6, 7]);
    writer.close();
    again.close();
  });

  it("resumes on a different node than the one it was connected to", async () => {
    const writer = await TestClient.connect(nodeA, "w2");
    await writer.sub("room:any-node");
    for (let i = 0; i < 10; i++) await writer.pub("room:any-node", { i });
    const reader = await TestClient.connect(nodeB, "r2");
    await reader.sub("room:any-node", { from: 4 });
    expect(reader.messages("room:any-node").map((m) => m.seq)).toEqual([5, 6, 7, 8, 9, 10]);
    writer.close();
    reader.close();
  });

  it("merges history and live without gaps or duplicates while publishing continues", async () => {
    const writer = await TestClient.connect(nodeA, "w3");
    await writer.sub("room:race");
    for (let i = 0; i < 20; i++) await serverPublish(nodeA, "room:race", { i });
    let publishing = true;
    const pump = (async () => {
      let i = 0;
      while (publishing) {
        await serverPublish(nodeB, "room:race", { live: i++ });
      }
    })();
    await sleep(20);
    const reader = await TestClient.connect(nodeB, "r3");
    await reader.sub("room:race", { from: 3 });
    await sleep(150);
    publishing = false;
    await pump;
    const head = (await serverPublish(nodeA, "room:race", { final: true })).seq;
    await reader.waitForSeq("room:race", head);
    const seqs = reader.messages("room:race").map((m) => m.seq);
    expect(seqs).toEqual(Array.from({ length: head - 3 }, (_, i) => i + 4));
    writer.close();
    reader.close();
  });

  it("sends reset when the history before from was trimmed", async () => {
    const trimNode = await startNode(prefix, { nodeId: "trim", historyMaxLen: 10 });
    const writer = await TestClient.connect(trimNode, "w4");
    for (let i = 0; i < 400; i++) await serverPublish(trimNode, "room:trimmed", { i });
    const reader = await TestClient.connect(trimNode, "r4");
    const ok = await reader.sub("room:trimmed", { from: 2 });
    const reset = reader.frames.find((f) => f.t === "reset");
    expect(reset).toEqual({ t: "reset", ch: "room:trimmed", seq: 400 });
    expect(ok).toMatchObject({ seq: 400 });
    expect(reader.messages("room:trimmed")).toHaveLength(0);
    await serverPublish(trimNode, "room:trimmed", { after: true });
    await reader.waitForSeq("room:trimmed", 401);
    writer.close();
    reader.close();
    await trimNode.stop();
  });

  it("sends reset when the gap is larger than the resume limit", async () => {
    const limited = await startNode(prefix, { nodeId: "limited", resumeLimit: 5 });
    for (let i = 0; i < 10; i++) await serverPublish(limited, "room:gap", { i });
    const reader = await TestClient.connect(limited, "r5");
    await reader.sub("room:gap", { from: 5 });
    expect(reader.frames.find((f) => f.t === "reset")).toBeUndefined();
    expect(reader.messages("room:gap").map((m) => m.seq)).toEqual([6, 7, 8, 9, 10]);
    const far = await TestClient.connect(limited, "r6");
    await far.sub("room:gap", { from: 1 });
    expect(far.frames.find((f) => f.t === "reset")).toEqual({ t: "reset", ch: "room:gap", seq: 10 });
    expect(far.messages("room:gap")).toHaveLength(0);
    reader.close();
    far.close();
    await limited.stop();
  });

  it("sends reset when the client is ahead of the server", async () => {
    const reader = await TestClient.connect(nodeA, "r7");
    await reader.sub("room:ahead-empty", { from: 50 });
    expect(reader.frames.find((f) => f.t === "reset")).toEqual({ t: "reset", ch: "room:ahead-empty", seq: 0 });
    reader.close();
  });
});

describe("cmid idempotency", () => {
  it("returns the original seq and mid for a repeated cmid and stores one message", async () => {
    const a = await TestClient.connect(nodeA, "idem");
    const watcher = await TestClient.connect(nodeB, "watch");
    await a.sub("room:idem");
    await watcher.sub("room:idem");
    const first = await a.pub("room:idem", { text: "once" }, "cmid-1");
    const second = await a.pub("room:idem", { text: "once" }, "cmid-1");
    expect(first).toMatchObject({ t: "ok", seq: 1 });
    expect(second).toMatchObject({ t: "ok", seq: 1, dup: true });
    expect(first.t === "ok" && second.t === "ok" && first.mid === second.mid).toBe(true);
    a.close();
    const reconnected = await TestClient.connect(nodeB, "idem");
    const third = await reconnected.pub("room:idem", { text: "once" }, "cmid-1");
    expect(third).toMatchObject({ seq: 1, dup: true });
    await reconnected.pub("room:idem", { text: "next" }, "cmid-2");
    await watcher.waitForSeq("room:idem", 2);
    expect(watcher.messages("room:idem").map((m) => m.d)).toEqual([{ text: "once" }, { text: "next" }]);
    const fresh = await TestClient.connect(nodeA, "late");
    await fresh.sub("room:idem", { from: 0 });
    expect(fresh.messages("room:idem")).toHaveLength(2);
    reconnected.close();
    watcher.close();
    fresh.close();
  });

  it("deduplicates concurrent retries of the same cmid across nodes", async () => {
    const a = await TestClient.connect(nodeA, "c1");
    const b = await TestClient.connect(nodeB, "c1");
    const results = await Promise.all([
      a.pub("room:idem-race", 1, "same"),
      b.pub("room:idem-race", 1, "same"),
      a.pub("room:idem-race", 1, "same"),
      b.pub("room:idem-race", 1, "same"),
    ]);
    const seqs = new Set(results.map((r) => (r.t === "ok" ? r.seq : -1)));
    expect(seqs).toEqual(new Set([1]));
    expect(results.filter((r) => r.t === "ok" && r.dup === true)).toHaveLength(3);
    a.close();
    b.close();
  });
});
