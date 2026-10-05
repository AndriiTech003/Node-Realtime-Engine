import { describe, expect, it } from "vitest";
import fc from "fast-check";
import { checkPage, mergeLive, planResume } from "../../src/resume.js";

describe("planResume", () => {
  it("goes live without from", () => {
    expect(planResume(undefined, undefined, 10, 5000)).toEqual({ kind: "live", seq: 10 });
  });
  it("replays a bounded tail for history", () => {
    expect(planResume(undefined, 3, 10, 5000)).toEqual({ kind: "replay", from: 7, to: 10, strict: false });
    expect(planResume(undefined, 30, 10, 5000)).toEqual({ kind: "replay", from: 0, to: 10, strict: false });
  });
  it("replays the gap when it fits", () => {
    expect(planResume(1180, undefined, 1182, 5000)).toEqual({ kind: "replay", from: 1180, to: 1182, strict: true });
  });
  it("is live when the client is caught up", () => {
    expect(planResume(10, undefined, 10, 5000)).toEqual({ kind: "live", seq: 10 });
  });
  it("resets when the gap exceeds the resume limit", () => {
    expect(planResume(0, undefined, 5001, 5000)).toEqual({ kind: "reset", seq: 5001, reason: "gap" });
    expect(planResume(1, undefined, 5001, 5000).kind).toBe("replay");
  });
  it("resets when the client is ahead of the server", () => {
    expect(planResume(20, undefined, 10, 5000)).toEqual({ kind: "reset", seq: 10, reason: "ahead" });
  });
});

describe("checkPage", () => {
  it("accepts contiguous pages", () => {
    expect(checkPage(5, [{ seq: 5 }, { seq: 6 }], 6)).toEqual({ ok: true });
  });
  it("detects trimmed history", () => {
    expect(checkPage(5, [{ seq: 8 }, { seq: 9 }], 9)).toEqual({ ok: false, firstSeq: 8 });
    expect(checkPage(5, [], 9)).toEqual({ ok: false, firstSeq: null });
  });
  it("detects holes in the middle", () => {
    expect(checkPage(1, [{ seq: 1 }, { seq: 3 }], 3).ok).toBe(false);
  });
});

describe("mergeLive (history + live race)", () => {
  it("drops live messages already covered by history and keeps order", () => {
    const buffered = [{ seq: 9 }, { seq: 10 }, { seq: 11 }, { seq: 12 }];
    expect(mergeLive(10, buffered)).toEqual({ items: [{ seq: 11 }, { seq: 12 }], lastSeq: 12 });
  });
  it("drops duplicates inside the live buffer", () => {
    const buffered = [{ seq: 11 }, { seq: 11 }, { seq: 12 }, { seq: 12 }];
    expect(mergeLive(10, buffered).items.map((i) => i.seq)).toEqual([11, 12]);
  });
  it("passes unsequenced presence items through in place", () => {
    const buffered = [{ seq: 10 }, { tag: "pj" }, { seq: 11 }];
    expect(mergeLive(10, buffered).items).toEqual([{ tag: "pj" }, { seq: 11 }]);
  });
  it("history + merged live is always gapless and duplicate free", () => {
    fc.assert(
      fc.property(fc.integer({ min: 0, max: 50 }), fc.integer({ min: 0, max: 50 }), fc.integer({ min: 0, max: 50 }), (from, historyLen, overlap) => {
        const head = from + historyLen;
        const history = Array.from({ length: historyLen }, (_, i) => from + 1 + i);
        const liveStart = Math.max(1, head - overlap + 1);
        const live = Array.from({ length: overlap + 10 }, (_, i) => ({ seq: liveStart + i }));
        const merged = mergeLive(head, live).items.map((x) => x.seq);
        const all = [...history, ...merged];
        for (let i = 0; i < all.length; i++) {
          if (all[i] !== from + 1 + i) return false;
        }
        return true;
      }),
    );
  });
});
