import { describe, expect, it } from "vitest";
import { Metrics } from "../../src/metrics.js";

describe("Metrics", () => {
  it("exports labelled counters at 0 before the first event so rate() sees the first burst", async () => {
    const metrics = new Metrics(() => ({ active: 0, lagging: 0, subscriptions: 0, channels: 0, pending: 0 }), { node: "n1" });
    try {
      const text = await metrics.registry.metrics();
      expect(text).toContain('rt_upgrades_total{result="ok",node="n1"} 0');
      expect(text).toContain('rt_upgrades_total{result="user_limit",node="n1"} 0');
      expect(text).toContain('rt_messages_in_total{t="pub",node="n1"} 0');
      expect(text).toContain('rt_messages_out_total{kind="durable",node="n1"} 0');
      metrics.upgrades.inc({ result: "ok" });
      expect(await metrics.registry.metrics()).toContain('rt_upgrades_total{result="ok",node="n1"} 1');
    } finally {
      metrics.stop();
    }
  });
});
