import { describe, expect, it } from "vitest";
import { pino } from "pino";
import type { RedisStore } from "../../src/redis.js";
import { UserConnectionLimiter } from "../../src/user-limit.js";

class FakeStore {
  readonly calls: string[] = [];
  readonly counts = new Map<string, number>();
  pending: (() => void)[] = [];
  holdAcquire = false;
  fail = false;
  resyncs: Map<string, number>[] = [];

  trackNodeUser(_node: string, uid: string): void {
    this.calls.push(`sadd ${uid}`);
  }

  untrackNodeUser(_node: string, uid: string): void {
    this.calls.push(`srem ${uid}`);
  }

  async userAcquire(uid: string, _node: string, limit: number): Promise<{ ok: boolean; total: number }> {
    this.calls.push(`acquire ${uid}`);
    if (this.holdAcquire) await new Promise<void>((resolve) => this.pending.push(resolve));
    if (this.fail) throw new Error("redis down");
    const current = this.counts.get(uid) ?? 0;
    if (current >= limit) return { ok: false, total: current };
    this.counts.set(uid, current + 1);
    return { ok: true, total: current + 1 };
  }

  async userRelease(uid: string): Promise<number> {
    this.calls.push(`release ${uid}`);
    if (this.fail) throw new Error("redis down");
    const next = (this.counts.get(uid) ?? 0) - 1;
    this.counts.set(uid, Math.max(0, next));
    return Math.max(0, next);
  }

  async resyncNodeUsers(_node: string, snapshot: () => Map<string, number> | null): Promise<boolean> {
    const counts = snapshot();
    if (counts === null) return false;
    this.resyncs.push(counts);
    return true;
  }
}

function limiter(store: FakeStore, limit = 2): UserConnectionLimiter {
  return new UserConnectionLimiter(store as unknown as RedisStore, "n1", limit, pino({ level: "silent" }));
}

describe("UserConnectionLimiter", () => {
  it("indexes the user on the node before the first acquire and unindexes after the last release", async () => {
    const store = new FakeStore();
    const l = limiter(store);
    expect(await l.acquire("u")).toBe(true);
    expect(await l.acquire("u")).toBe(true);
    l.release("u");
    l.release("u");
    await Promise.resolve();
    expect(store.calls).toEqual(["sadd u", "acquire u", "acquire u", "release u", "release u", "srem u"]);
    expect(l.localCount("u")).toBe(0);
  });

  it("does not hold a local slot for a rejected connection", async () => {
    const store = new FakeStore();
    const l = limiter(store, 1);
    expect(await l.acquire("u")).toBe(true);
    expect(await l.acquire("u")).toBe(false);
    expect(l.localCount("u")).toBe(1);
    expect(store.calls.filter((c) => c.startsWith("release"))).toEqual([]);
  });

  it("skips a resync while an acquire is in flight and retries later", async () => {
    const store = new FakeStore();
    const l = limiter(store);
    store.holdAcquire = true;
    const pending = l.acquire("u");
    expect(await l.resync()).toBe(false);
    expect(l.needsResync).toBe(true);
    for (const resolve of store.pending) resolve();
    expect(await pending).toBe(true);
    expect(await l.resync()).toBe(true);
    expect(l.needsResync).toBe(false);
    expect(store.resyncs).toEqual([new Map([["u", 1]])]);
  });

  it("marks itself dirty when Redis fails so the next tick resyncs", async () => {
    const store = new FakeStore();
    const l = limiter(store);
    expect(await l.acquire("u")).toBe(true);
    store.fail = true;
    l.release("u");
    await new Promise((r) => setTimeout(r, 0));
    expect(l.needsResync).toBe(true);
    await expect(l.acquire("v")).rejects.toThrow("redis down");
    expect(l.localCount("v")).toBe(0);
  });

  it("is a no-op when the limit is disabled", async () => {
    const store = new FakeStore();
    const l = limiter(store, 0);
    expect(await l.acquire("u")).toBe(true);
    l.release("u");
    expect(store.calls).toEqual([]);
  });
});
