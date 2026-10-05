import { afterAll, describe, expect, it } from "vitest";
import WebSocket from "ws";
import type { RealtimeServer } from "@ashamrai/realtime-server";
import { getTicket, purgePrefix, startNode, TestClient, uniquePrefix } from "../../../../test-support/helpers.js";

const prefix = uniquePrefix();
const started: RealtimeServer[] = [];

afterAll(async () => {
  for (const s of started) await s.stop().catch(() => undefined);
  await purgePrefix(prefix);
});

async function silentClient(server: RealtimeServer): Promise<{ ws: WebSocket; closed: Promise<number> }> {
  const ticket = await getTicket(server, `silent-${Math.random()}`);
  const ws = new WebSocket(`ws://127.0.0.1:${server.address().port}/v1/connect?ticket=${ticket}`, "pulse.v1.json", { autoPong: false });
  const closed = new Promise<number>((resolve) => ws.on("close", (code) => resolve(code)));
  await new Promise((resolve) => ws.once("open", resolve));
  return { ws, closed };
}

describe("heartbeat", () => {
  for (const mode of ["shared", "per-connection"] as const) {
    it(`terminates connections that stop answering pings (${mode} timer)`, async () => {
      const server = await startNode(prefix, { heartbeatMode: mode, heartbeatIntervalMs: 300, heartbeatTimeoutMs: 300 });
      started.push(server);
      const healthy = await TestClient.connect(server, "healthy");
      const silent = await silentClient(server);
      const t0 = Date.now();
      const code = await silent.closed;
      expect(code).toBe(1006);
      expect(Date.now() - t0).toBeLessThan(2000);
      expect(healthy.closeCode).toBeNull();
      const value = (await server.metrics.heartbeatTerminations.get()).values[0]?.value ?? 0;
      expect(value).toBeGreaterThanOrEqual(1);
      healthy.close();
    });
  }

  it("closes with 4001 when the session behind the ticket expires", async () => {
    const server = await startNode(prefix, { heartbeatIntervalMs: 200, heartbeatTimeoutMs: 200 });
    started.push(server);
    const client = await TestClient.connect(server, "expiring", { extraClaims: { exp: Math.floor(Date.now() / 1000) + 1 } });
    expect(await client.waitClose(5000)).toBe(4001);
  });
});
