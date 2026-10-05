import { describe, expect, it } from "vitest";
import { pino } from "pino";
import type { WebSocket } from "ws";
import { CloseCode, jsonCodec } from "@ashamrai/realtime-protocol";
import { Connection } from "../../src/connection.js";
import { resolveConfig } from "../../src/config.js";
import { Metrics } from "../../src/metrics.js";

class FakeWs {
  bufferedAmount = 0;
  sent: Buffer[] = [];
  closed: { code: number; reason: string } | null = null;
  terminated = false;
  send(data: Buffer): void {
    this.sent.push(data);
  }
  close(code: number, reason: string): void {
    this.closed = { code, reason };
  }
  terminate(): void {
    this.terminated = true;
  }
}

function setup(overrides: Partial<ReturnType<typeof resolveConfig>["backpressure"]> = {}) {
  const config = resolveConfig({ backpressure: { lowBytes: 100, highBytes: 1000, hardBytes: 4000, maxPending: 5, lagTimeoutMs: 1000, ...overrides } });
  const metrics = new Metrics(() => ({ active: 0, lagging: 0, subscriptions: 0, channels: 0, pending: 0 }), {});
  const ws = new FakeWs();
  const lagging: Connection[] = [];
  const slow: string[] = [];
  const conn = new Connection(
    "c1",
    ws as unknown as WebSocket,
    { id: "u", name: "u", claims: { sub: "u" }, expiresAt: null },
    jsonCodec,
    pino({ level: "silent" }),
    metrics,
    config.backpressure,
    config.rate,
    { onLagging: (c) => lagging.push(c), onSlowClose: (_c, reason) => slow.push(reason) },
  );
  return { conn, ws, lagging, slow, metrics };
}

const bytes = (s: string) => Buffer.from(s);

describe("Connection backpressure", () => {
  it("sends directly below the high watermark", () => {
    const { conn, ws } = setup();
    expect(conn.sendBytes(bytes("a"), "durable")).toBe(true);
    expect(ws.sent).toHaveLength(1);
    expect(conn.state).toBe("active");
  });

  it("queues durable and drops ephemeral above the high watermark, with one lag frame per channel", () => {
    const { conn, ws, lagging, metrics } = setup();
    ws.bufferedAmount = 2000;
    expect(conn.sendBytes(bytes("d1"), "durable", "room:1")).toBe(true);
    expect(conn.state).toBe("lagging");
    expect(lagging).toHaveLength(1);
    expect(conn.sendBytes(bytes("e1"), "ephemeral", "room:1")).toBe(false);
    expect(conn.sendBytes(bytes("e2"), "ephemeral", "room:1")).toBe(false);
    expect(ws.sent).toHaveLength(0);
    expect(conn.pendingCount).toBe(2);
    expect(JSON.parse(conn.pending[1]?.toString() ?? "")).toEqual({ t: "lag", ch: "room:1" });
    return metrics.ephemeralDropped.get().then((v) => expect(v.values[0]?.value).toBe(2));
  });

  it("keeps order: new durable frames queue behind pending even after the buffer drains", () => {
    const { conn, ws } = setup();
    ws.bufferedAmount = 2000;
    conn.sendBytes(bytes("1"), "durable");
    ws.bufferedAmount = 0;
    conn.sendBytes(bytes("2"), "durable");
    expect(ws.sent).toHaveLength(0);
    expect(conn.flushPending(Date.now())).toBe("active");
    expect(ws.sent.map((b) => b.toString())).toEqual(["1", "2"]);
  });

  it("flushes only while below the low watermark", () => {
    const { conn, ws } = setup();
    ws.bufferedAmount = 2000;
    conn.sendBytes(bytes("1"), "durable");
    conn.sendBytes(bytes("2"), "durable");
    ws.bufferedAmount = 500;
    expect(conn.flushPending(Date.now())).toBe("lagging");
    expect(ws.sent).toHaveLength(0);
    ws.bufferedAmount = 50;
    const originalSend = ws.send.bind(ws);
    ws.send = (data: Buffer) => {
      originalSend(data);
      ws.bufferedAmount += 60;
    };
    expect(conn.flushPending(Date.now())).toBe("lagging");
    expect(ws.sent).toHaveLength(1);
  });

  it("closes with 4008 when pending overflows", () => {
    const { conn, ws, slow } = setup();
    ws.bufferedAmount = 2000;
    for (let i = 0; i < 6; i++) conn.sendBytes(bytes(String(i)), "durable");
    expect(ws.closed?.code).toBe(CloseCode.SlowConsumer);
    expect(slow).toEqual(["pending overflow"]);
    expect(conn.state).toBe("closing");
    expect(conn.sendBytes(bytes("x"), "durable")).toBe(false);
  });

  it("closes with 4008 above the hard watermark", () => {
    const { conn, ws, slow } = setup();
    ws.bufferedAmount = 5000;
    conn.sendBytes(bytes("x"), "durable");
    expect(ws.closed?.code).toBe(CloseCode.SlowConsumer);
    expect(slow).toEqual(["hard watermark"]);
  });

  it("closes with 4008 when lagging longer than the timeout", () => {
    const { conn, ws } = setup();
    ws.bufferedAmount = 2000;
    conn.sendBytes(bytes("x"), "durable");
    expect(conn.flushPending(conn.lagSince + 2000)).toBe("closed");
    expect(ws.closed?.code).toBe(CloseCode.SlowConsumer);
  });
});
