import { describe, expect, it } from "vitest";
import { backoffDelay } from "../../src/backoff.js";
import { SeqTracker } from "../../src/seq-tracker.js";

describe("backoffDelay", () => {
  const options = { baseMs: 500, maxMs: 30000, jitter: "full" as const };
  it("uses full jitter in [0, base * 2^attempt)", () => {
    expect(backoffDelay(0, options, () => 0)).toBe(0);
    expect(backoffDelay(0, options, () => 0.999)).toBe(499);
    expect(backoffDelay(3, options, () => 0.5)).toBe(2000);
  });
  it("caps at maxMs", () => {
    expect(backoffDelay(20, options, () => 0.999)).toBeLessThan(30000);
    expect(backoffDelay(20, { ...options, jitter: "none" })).toBe(30000);
  });
  it("without jitter every client waits exactly the same", () => {
    const delays = new Set(Array.from({ length: 100 }, () => backoffDelay(2, { ...options, jitter: "none" })));
    expect(delays).toEqual(new Set([2000]));
  });
  it("with jitter delays are spread uniformly", () => {
    const delays = Array.from({ length: 2000 }, () => backoffDelay(2, options));
    const mean = delays.reduce((a, b) => a + b, 0) / delays.length;
    expect(mean).toBeGreaterThan(850);
    expect(mean).toBeLessThan(1150);
    expect(Math.max(...delays)).toBeLessThan(2000);
  });
});

describe("SeqTracker", () => {
  it("accepts the first message as a baseline, then only contiguous seqs", () => {
    const t = new SeqTracker();
    expect(t.check(10)).toBe("accept");
    t.commit(10);
    expect(t.check(10)).toBe("duplicate");
    expect(t.check(9)).toBe("duplicate");
    expect(t.check(11)).toBe("accept");
    expect(t.check(13)).toBe("gap");
  });
  it("moves the baseline forward only", () => {
    const t = new SeqTracker(5);
    t.baseline(3);
    expect(t.last).toBe(5);
    t.baseline(8);
    expect(t.last).toBe(8);
    t.reset(2);
    expect(t.last).toBe(2);
  });
});
