import { fork } from "node:child_process";
import { mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import WebSocket from "ws";
import { Redis } from "ioredis";
import { signJwt } from "../dist/index.js";

const N = Number(process.env.BENCH_CONNECTIONS ?? 2000);
const ROOM_SIZE = 20;
const port = Number(process.env.BENCH_PORT ?? 4380);
const prefix = "rt:bench:heap:";
const dir = process.env.BENCH_SNAPSHOT_DIR ?? ".run/heap";
const out = process.env.BENCH_OUT ?? "results/heap-snapshot-diff.json";
const keep = process.env.BENCH_KEEP_SNAPSHOTS === "1";
const watched = ["Connection", "WebSocket", "Socket", "Sender", "Receiver", "TokenBucket", "ViolationCounter", "TCP", "WriteWrap", "Timeout"];

mkdirSync(dir, { recursive: true });
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const server = fork(new URL("./memory-server.mjs", import.meta.url), ["engine", String(port), prefix], {
  execArgv: ["--expose-gc"],
  env: { ...process.env },
});
await new Promise((resolve) => server.on("message", (m) => m.ready && resolve()));

function snapshot(name) {
  const file = join(dir, `${name}.heapsnapshot`);
  return new Promise((resolve) => {
    const handler = (m) => {
      if (m.snapshotWritten !== undefined) {
        server.off("message", handler);
        resolve({ file: m.snapshotWritten, heapUsed: m.heapUsed });
      }
    };
    server.on("message", handler);
    server.send({ snapshot: file });
  });
}

async function serverConnections() {
  const res = await fetch(`http://127.0.0.1:${port}/health/ready`);
  return (await res.json()).connections;
}

async function ticketFor(uid) {
  const res = await fetch(`http://127.0.0.1:${port}/v1/tickets`, {
    method: "POST",
    headers: { authorization: `Bearer ${signJwt({ sub: uid, name: uid }, "bench-secret")}` },
  });
  return (await res.json()).ticket;
}

async function client(i, cycle) {
  const uid = `heap-${cycle}-${i}`;
  const ws = new WebSocket(`ws://127.0.0.1:${port}/v1/connect?ticket=${await ticketFor(uid)}`, "pulse.v1.json");
  const acks = new Map();
  ws.on("message", (raw) => {
    const frame = JSON.parse(raw.toString());
    if ((frame.t === "ok" || frame.t === "err") && acks.has(frame.id)) acks.get(frame.id)(frame);
  });
  await new Promise((resolve, reject) => {
    ws.once("open", resolve);
    ws.once("error", reject);
  });
  const request = (frame) =>
    new Promise((resolve) => {
      acks.set(frame.id, resolve);
      ws.send(JSON.stringify(frame));
    });
  await request({ t: "sub", id: 1, ch: `room:heap-${cycle}-${Math.floor(i / ROOM_SIZE)}` });
  await request({ t: "sub", id: 2, ch: `user:${uid}`, presence: false });
  await request({ t: "pub", id: 3, ch: `room:heap-${cycle}-${Math.floor(i / ROOM_SIZE)}`, cmid: `c-${uid}`, d: { text: `hello from ${uid}` } });
  return ws;
}

async function openMany(count, cycle) {
  const sockets = [];
  for (let i = 0; i < count; i += 50) {
    const batch = [];
    for (let k = i; k < Math.min(count, i + 50); k++) batch.push(client(k, cycle));
    sockets.push(...(await Promise.all(batch)));
  }
  return sockets;
}

async function closeAll(sockets, abrupt = false) {
  await Promise.all(
    sockets.map(
      (ws) =>
        new Promise((resolve) => {
          ws.once("close", resolve);
          if (abrupt) ws.terminate();
          else ws.close();
        }),
    ),
  );
  const deadline = Date.now() + 20000;
  while ((await serverConnections()) > 0 && Date.now() < deadline) await sleep(100);
  await sleep(1500);
}

function classify(snapshotFile) {
  const raw = JSON.parse(readFileSync(snapshotFile, "utf8"));
  const meta = raw.snapshot.meta;
  const fields = meta.node_fields;
  const types = meta.node_types[0];
  const width = fields.length;
  const iType = fields.indexOf("type");
  const iName = fields.indexOf("name");
  const iId = fields.indexOf("id");
  const iSize = fields.indexOf("self_size");
  const { nodes, strings } = raw;
  const classes = new Map();
  const ids = new Map();
  let total = 0;
  for (let i = 0; i < nodes.length; i += width) {
    const type = types[nodes[i + iType]];
    const name = strings[nodes[i + iName]];
    const size = nodes[i + iSize];
    const key = type === "object" || type === "closure" ? (type === "closure" ? `${name}()` : name) : `(${type})`;
    let entry = classes.get(key);
    if (entry === undefined) {
      entry = { count: 0, size: 0 };
      classes.set(key, entry);
    }
    entry.count++;
    entry.size += size;
    ids.set(nodes[i + iId], key);
    total += size;
  }
  return { classes, ids, total, nodeCount: nodes.length / width };
}

function diff(a, b, limit = 15) {
  const keys = new Set([...a.classes.keys(), ...b.classes.keys()]);
  const rows = [];
  for (const key of keys) {
    const x = a.classes.get(key) ?? { count: 0, size: 0 };
    const y = b.classes.get(key) ?? { count: 0, size: 0 };
    rows.push({ class: key, countDelta: y.count - x.count, sizeDelta: y.size - x.size });
  }
  rows.sort((p, q) => q.sizeDelta - p.sizeDelta);
  return rows.slice(0, limit);
}

function survivors(a, c, limit = 15) {
  const byClass = new Map();
  let count = 0;
  for (const [id, key] of c.ids) {
    if (a.ids.has(id)) continue;
    count++;
    byClass.set(key, (byClass.get(key) ?? 0) + 1);
  }
  return { count, top: [...byClass].sort((p, q) => q[1] - p[1]).slice(0, limit).map(([k, n]) => ({ class: k, count: n })) };
}

function watchCounts(s) {
  const out = {};
  for (const name of watched) out[name] = s.classes.get(name)?.count ?? 0;
  return out;
}

const warm = await openMany(200, "warm");
await closeAll(warm);
const snapA = await snapshot("a-baseline");
const sockets = await openMany(N, "main");
await sleep(2000);
const openOnServer = await serverConnections();
const snapB = await snapshot("b-open");
await closeAll(sockets);
const snapC = await snapshot("c-closed");
const again = await openMany(N, "again");
await sleep(2000);
await closeAll(again, true);
const snapD = await snapshot("d-second-cycle-closed");
server.send("exit");

const [A, B, C, D] = [snapA, snapB, snapC, snapD].map((s) => classify(s.file));
const kb = (bytes) => Math.round((bytes / 1024) * 10) / 10;
const result = {
  at: new Date().toISOString(),
  node: process.version,
  connections: N,
  openOnServer,
  perConnection: "room:{20 users} with presence + user:{uid} + one pub each",
  closePaths: { firstCycle: "client close handshake", secondCycle: "client terminate (no close frame)" },
  totals: {
    baselineKb: kb(A.total),
    openKb: kb(B.total),
    closedKb: kb(C.total),
    secondCycleClosedKb: kb(D.total),
    heapUsedMb: [snapA, snapB, snapC, snapD].map((s) => Math.round((s.heapUsed / 1024 / 1024) * 10) / 10),
    nodes: [A, B, C, D].map((s) => s.nodeCount),
  },
  perConnectionBytes: Math.round((B.total - A.total) / N),
  leftoverAfterCloseKb: kb(C.total - A.total),
  growthSecondCycleKb: kb(D.total - C.total),
  watched: { baseline: watchCounts(A), open: watchCounts(B), closed: watchCounts(C), secondCycleClosed: watchCounts(D) },
  growthOpen: diff(A, B),
  leftoverClosed: diff(A, C),
  growthSecondCycle: diff(C, D),
  survivorsAtoC: survivors(A, C),
  survivorsCtoD: survivors(C, D),
};
mkdirSync("results", { recursive: true });
writeFileSync(out, `${JSON.stringify(result, null, 2)}\n`);
console.log(JSON.stringify({ ...result, growthOpen: result.growthOpen.slice(0, 8), leftoverClosed: result.leftoverClosed.slice(0, 8), growthSecondCycle: result.growthSecondCycle.slice(0, 8) }, null, 2));

const redis = new Redis("redis://127.0.0.1:6379/3");
let cursor = "0";
do {
  const [next, keys] = await redis.scan(cursor, "MATCH", `${prefix}*`, "COUNT", 1000);
  cursor = next;
  if (keys.length > 0) await redis.unlink(...keys);
} while (cursor !== "0");
redis.disconnect();
if (!keep) for (const s of [snapA, snapB, snapC, snapD]) rmSync(s.file, { force: true });
