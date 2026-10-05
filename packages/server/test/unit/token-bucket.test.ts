import { describe, expect, it } from "vitest";
import { TokenBucket, ViolationCounter } from "../../src/token-bucket.js";

describe("TokenBucket", () => {
  it("allows a burst then refills at the configured rate", () => {
    const bucket = new TokenBucket(20, 40, 0);
    let allowed = 0;
    for (let i = 0; i < 100; i++) if (bucket.take(1, 0)) allowed++;
    expect(allowed).toBe(40);
    expect(bucket.take(1, 0)).toBe(false);
    expect(bucket.take(1, 50)).toBe(true);
    expect(bucket.take(1, 50)).toBe(false);
    let later = 0;
    for (let i = 0; i < 100; i++) if (bucket.take(1, 1050)) later++;
    expect(later).toBe(20);
  });

  it("never exceeds the burst", () => {
    const bucket = new TokenBucket(60, 60, 0);
    expect(bucket.available(1_000_000)).toBe(60);
  });

  it("sustains exactly the rate over a long window", () => {
    const bucket = new TokenBucket(20, 40, 0);
    let allowed = 0;
    for (let t = 0; t <= 10_000; t += 5) if (bucket.take(1, t)) allowed++;
    expect(allowed).toBeGreaterThanOrEqual(239);
    expect(allowed).toBeLessThanOrEqual(241);
  });
});

describe("ViolationCounter", () => {
  it("trips after the limit inside the window and resets after it", () => {
    const counter = new ViolationCounter(3, 1000, 0);
    expect(counter.hit(0)).toBe(false);
    expect(counter.hit(10)).toBe(false);
    expect(counter.hit(20)).toBe(true);
    expect(counter.hit(2000)).toBe(false);
  });
});
