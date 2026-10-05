import { describe, expect, it } from "vitest";
import { Histogram } from "../../src/histogram.js";
import { histogramBuckets, parsePrometheus, pick, quantileFromBuckets } from "../../src/prom.js";

describe("Histogram", () => {
  it("estimates percentiles within 2%", () => {
    const h = new Histogram();
    for (let i = 1; i <= 10000; i++) h.record(i / 10);
    expect(h.count).toBe(10000);
    expect(Math.abs(h.percentile(50) - 500) / 500).toBeLessThan(0.02);
    expect(Math.abs(h.percentile(99) - 990) / 990).toBeLessThan(0.02);
    expect(h.percentile(100)).toBe(1000);
    expect(h.mean()).toBeCloseTo(500.05, 1);
  });

  it("merges worker histograms", () => {
    const a = new Histogram();
    const b = new Histogram();
    for (let i = 0; i < 100; i++) a.record(1);
    for (let i = 0; i < 100; i++) b.record(100);
    const merged = Histogram.from(a.toJSON());
    merged.merge(b.toJSON());
    expect(merged.count).toBe(200);
    expect(merged.percentile(25)).toBeLessThan(1.05);
    expect(merged.percentile(99)).toBeGreaterThan(95);
    expect(merged.min).toBeLessThanOrEqual(1);
    expect(merged.max).toBe(100);
  });

  it("handles empty histograms and tiny values", () => {
    const h = new Histogram();
    expect(h.percentile(99)).toBe(0);
    h.record(0);
    h.record(-3);
    expect(h.percentile(50)).toBe(0);
  });
});

describe("Prometheus parsing", () => {
  const text = [
    "# HELP rt_connections x",
    "# TYPE rt_connections gauge",
    'rt_connections{state="active",node="n1"} 10',
    'rt_connections{state="lagging",node="n1"} 2',
    'rt_fanout_duration_seconds_bucket{le="0.001",node="n1"} 50',
    'rt_fanout_duration_seconds_bucket{le="0.01",node="n1"} 90',
    'rt_fanout_duration_seconds_bucket{le="+Inf",node="n1"} 100',
    "process_cpu_seconds_total 1.5",
  ].join("\n");

  it("parses samples and sums by labels", () => {
    const samples = parsePrometheus(text);
    expect(pick(samples, "rt_connections")).toBe(12);
    expect(pick(samples, "rt_connections", { state: "lagging" })).toBe(2);
    expect(pick(samples, "process_cpu_seconds_total")).toBe(1.5);
    expect(Number.isNaN(pick(samples, "missing"))).toBe(true);
  });

  it("computes quantiles from histogram bucket deltas", () => {
    const after = histogramBuckets(parsePrometheus(text), "rt_fanout_duration_seconds");
    const before = new Map<number, number>();
    expect(quantileFromBuckets(before, after, 0.5)).toBeCloseTo(0.001, 5);
    expect(quantileFromBuckets(before, after, 0.9)).toBeCloseTo(0.01, 5);
    expect(quantileFromBuckets(after, after, 0.5)).toBe(0);
  });
});
