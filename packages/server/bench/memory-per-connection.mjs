import { fork } from "node:child_process";
import { writeFileSync, mkdirSync } from "node:fs";
import WebSocket from "ws";
import { Redis } from "ioredis";
import { signJwt } from "../dist/index.js";

const N = Number(process.env.BENCH_CONNECTIONS ?? 4000);
const port = 4380;
const prefix = "rt:bench:mem:";
const modes = (process.env.BENCH_MODES ?? "bare,engine,engine-subscribed,engine-deflate,engine-per-connection-timers").split(",");

function startServer(mode, env = {}) {
  const serverMode = mode === "engine-subscribed" ? "engine" : mode;
  const child = fork(new URL("./memory-server.mjs", import.meta.url), [serverMode, String(port), prefix], {
    execArgv: ["--expose-gc"],
    env: { ...process.env, ...env },
  });
  return new Promise((resolve) => child.on("message", (m) => m.ready && resolve(child)));
}

function ask(child) {
  return new Promise((resolve) => {
    const handler = (m) => {
      if (m.report !== undefined) {
        child.off("message", handler);
        resolve(m.report);
      }
    };
    child.on("message", handler);
    child.send("report");
  });
}

async function ticketFor(uid) {
  const res = await fetch(`http://127.0.0.1:${port}/v1/tickets`, {
    method: "POST",
    headers: { authorization: `Bearer ${signJwt({ sub: uid }, "bench-secret")}` },
  });
  return (await res.json()).ticket;
}

async function connect(mode, i) {
  const engine = mode !== "bare";
  const url = engine ? `ws://127.0.0.1:${port}/v1/connect?ticket=${await ticketFor(`u${i}`)}` : `ws://127.0.0.1:${port}/`;
  const ws = new WebSocket(url, engine ? "pulse.v1.json" : undefined, { perMessageDeflate: mode === "engine-deflate" });
  await new Promise((resolve, reject) => {
    ws.once("open", resolve);
    ws.once("error", reject);
  });
  if (mode === "engine-subscribed") ws.send(JSON.stringify({ t: "sub", id: 1, ch: `user:u${i}`, presence: false }));
  return ws;
}

async function purge() {
  const redis = new Redis("redis://127.0.0.1:6379/3");
  const keys = await redis.keys(`${prefix}*`);
  if (keys.length > 0) await redis.del(...keys);
  redis.disconnect();
}

const results = [];
for (const mode of modes) {
  const env = mode === "engine-per-connection-timers" || mode === "engine" ? { BENCH_HB_MS: process.env.BENCH_HB_MS ?? "25000" } : {};
  const server = await startServer(mode, env);
  await new Promise((r) => setTimeout(r, 500));
  const before = await ask(server);
  const sockets = [];
  const started = performance.now();
  for (let i = 0; i < N; i += 50) {
    const batch = [];
    for (let k = i; k < Math.min(N, i + 50); k++) batch.push(connect(mode, k));
    sockets.push(...(await Promise.all(batch)));
  }
  const connectMs = performance.now() - started;
  await new Promise((r) => setTimeout(r, 2000));
  const after = await ask(server);
  const holdMs = Number(process.env.BENCH_HOLD_MS ?? 30000);
  const cpuBefore = await ask(server);
  await new Promise((r) => setTimeout(r, holdMs));
  const cpuAfter = await ask(server);
  const kb = (v) => Math.round((v / N / 1024) * 100) / 100;
  const result = {
    mode,
    connections: N,
    heapPerConnKb: kb(after.heapUsed - before.heapUsed),
    rssPerConnKb: kb(after.rss - before.rss),
    externalPerConnKb: kb(after.external - before.external),
    connectMs: Math.round(connectMs),
    idleCpuMsPerSec: Math.round(((cpuAfter.cpuUser + cpuAfter.cpuSystem - cpuBefore.cpuUser - cpuBefore.cpuSystem) / 1000 / (holdMs / 1000)) * 100) / 100,
    holdSeconds: holdMs / 1000,
  };
  console.log(JSON.stringify(result));
  results.push(result);
  for (const ws of sockets) ws.terminate();
  server.send("exit");
  await new Promise((r) => setTimeout(r, 1000));
  await purge();
}
mkdirSync("results", { recursive: true });
writeFileSync(process.env.BENCH_OUT ?? "results/bench-memory.json", JSON.stringify({ at: new Date().toISOString(), node: process.version, results }, null, 2));
