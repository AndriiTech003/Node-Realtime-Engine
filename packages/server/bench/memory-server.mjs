import { createServer } from "node:http";
import { writeHeapSnapshot } from "node:v8";
import { WebSocketServer } from "ws";
import { RealtimeServer } from "../dist/index.js";

const mode = process.argv[2];
const port = Number(process.argv[3]);
const prefix = process.argv[4] ?? "rt:bench:";

function report() {
  const cpu = process.cpuUsage();
  globalThis.gc?.();
  globalThis.gc?.();
  const mem = process.memoryUsage();
  return { heapUsed: mem.heapUsed, rss: mem.rss, external: mem.external, arrayBuffers: mem.arrayBuffers, cpuUser: cpu.user, cpuSystem: cpu.system };
}

if (mode === "bare") {
  const http = createServer();
  const wss = new WebSocketServer({ noServer: true, perMessageDeflate: false });
  const sockets = new Set();
  http.on("upgrade", (req, socket, head) => wss.handleUpgrade(req, socket, head, (ws) => {
    sockets.add(ws);
    ws.on("close", () => sockets.delete(ws));
  }));
  http.listen(port, "127.0.0.1", () => process.send?.({ ready: true }));
} else {
  const heartbeatMode = mode === "engine-per-connection-timers" ? "per-connection" : "shared";
  const server = new RealtimeServer({
    config: {
      nodeId: `bench-${mode}`,
      host: "127.0.0.1",
      port,
      redisPrefix: prefix,
      jwtSecret: "bench-secret",
      logLevel: "silent",
      perMessageDeflate: mode === "engine-deflate",
      heartbeatMode,
      heartbeatIntervalMs: Number(process.env.BENCH_HB_MS ?? 25000),
      heartbeatTimeoutMs: 10000,
      maxConnections: 100000,
    },
  });
  await server.start();
  process.send?.({ ready: true });
}

process.on("message", (m) => {
  if (m === "report") process.send?.({ report: report() });
  if (m === "exit") process.exit(0);
  if (typeof m === "object" && m !== null && typeof m.snapshot === "string") {
    for (let i = 0; i < 4; i++) globalThis.gc?.();
    const file = writeHeapSnapshot(m.snapshot);
    process.send?.({ snapshotWritten: file, heapUsed: process.memoryUsage().heapUsed });
  }
});
