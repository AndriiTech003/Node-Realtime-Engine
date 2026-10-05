#!/usr/bin/env node
import { exec } from "node:child_process";
import { mkdirSync, writeFileSync } from "node:fs";
import { cpus, totalmem } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";
import { Worker } from "node:worker_threads";
import { Histogram } from "./histogram.js";
import { histogramBuckets, pick, quantileFromBuckets, scrape, type Sample } from "./prom.js";
import type { ClientSpec, MainToWorker, Target, TimelineBucket, WorkerConfig, WorkerReport, WorkerToMain } from "./types.js";

const execAsync = promisify(exec);
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

export interface Options {
  scenario: string;
  label: string;
  out: string;
  via: "direct" | "haproxy";
  nodes: string[];
  haproxy: string;
  workers: number;
  codec: "json" | "msgpack";
  deflate: boolean;
  jitter: boolean;
  connections: number;
  rooms: number;
  duration: number;
  warmup: number;
  rate: number;
  payload: number;
  slowPercent: number;
  cursorHz: number;
  probes: number;
  steps: number;
  subscribe: "user" | "none";
  killCmd: string;
  restartCmd: string;
  jwtSecret: string;
  serverKey: string;
  connectRate: number;
  ticketConcurrency: number;
  backoffBaseMs: number;
  backoffMaxMs: number;
}

function parseArgs(argv: string[]): Options {
  const get = (name: string, fallback: string): string => {
    const i = argv.indexOf(`--${name}`);
    return i >= 0 && argv[i + 1] !== undefined ? (argv[i + 1] as string) : fallback;
  };
  const has = (name: string) => argv.includes(`--${name}`);
  const scenario = argv[0] ?? "help";
  const defaults: Record<string, Record<string, string>> = {
    idle: { connections: "9000", duration: "60", steps: "3" },
    "fanout-big-room": { connections: "6000", duration: "60", rate: "5" },
    "many-rooms": { connections: "6000", duration: "60", rate: "0.1", cursorHz: "10" },
    "slow-consumers": { connections: "6000", duration: "90", rate: "0.1", cursorHz: "10", slowPercent: "5" },
    "reconnect-storm": { connections: "4500", duration: "45", rate: "1", via: "haproxy" },
    resume: { connections: "3000", duration: "120", rate: "5", rooms: "100" },
  };
  const d = defaults[scenario] ?? {};
  const value = (name: string, fallback: string) => get(name, d[name] ?? fallback);
  return {
    scenario,
    label: get("label", `${scenario}-${new Date().toISOString().replace(/[:.]/g, "-")}`),
    out: get("out", "results"),
    via: value("via", "direct") === "haproxy" ? "haproxy" : "direct",
    nodes: get("nodes", "http://127.0.0.1:4301,http://127.0.0.1:4302,http://127.0.0.1:4303").split(","),
    haproxy: get("haproxy", "http://127.0.0.1:4300"),
    workers: Number(get("workers", "4")),
    codec: get("codec", "json") === "msgpack" ? "msgpack" : "json",
    deflate: has("deflate"),
    jitter: get("jitter", "on") !== "off",
    connections: Number(value("connections", "1000")),
    rooms: Number(value("rooms", "0")),
    duration: Number(value("duration", "60")),
    warmup: Number(get("warmup", "5")),
    rate: Number(value("rate", "5")),
    payload: Number(get("payload", "0")),
    slowPercent: Number(value("slowPercent", "0")),
    cursorHz: Number(value("cursorHz", "0")),
    probes: Number(get("probes", "0")),
    steps: Number(value("steps", "3")),
    subscribe: get("subscribe", "user") === "none" ? "none" : "user",
    killCmd: get("kill-cmd", "node scripts/cluster.mjs kill 2"),
    restartCmd: get("restart-cmd", "node scripts/cluster.mjs start-node 2"),
    jwtSecret: get("jwt-secret", process.env["JWT_SECRET"] ?? "dev-jwt-secret-change-me"),
    serverKey: get("server-key", process.env["SERVER_API_KEY"] ?? "dev-server-key-change-me"),
    connectRate: Number(get("connect-rate", "1000")),
    ticketConcurrency: Number(get("ticket-concurrency", "128")),
    backoffBaseMs: Number(get("backoff-base", "1000")),
    backoffMaxMs: Number(get("backoff-max", "30000")),
  };
}

function targetsFor(options: Options): Target[] {
  const toWs = (http: string) => `${http.replace(/^http/, "ws")}/v1/connect`;
  if (options.via === "haproxy") return [{ http: options.haproxy, ws: toWs(options.haproxy) }];
  return options.nodes.map((http) => ({ http, ws: toWs(http) }));
}

interface Aggregate {
  connected: number;
  everConnected: number;
  connecting: number;
  latency: Map<string, Histogram>;
  ticketLatency: Histogram;
  connectLatency: Histogram;
  received: number;
  ephReceived: number;
  published: number;
  publishErrors: number;
  gaps: number;
  duplicates: number;
  resets: number;
  resumed: number;
  reconnects: number;
  connectErrors: number;
  ticketErrors: number;
  closeCodes: Record<string, number>;
  timeline: Map<number, TimelineBucket>;
  loadgenRss: number;
}

function aggregate(reports: WorkerReport[]): Aggregate {
  const agg: Aggregate = {
    connected: 0,
    everConnected: 0,
    connecting: 0,
    latency: new Map(),
    ticketLatency: new Histogram(),
    connectLatency: new Histogram(),
    received: 0,
    ephReceived: 0,
    published: 0,
    publishErrors: 0,
    gaps: 0,
    duplicates: 0,
    resets: 0,
    resumed: 0,
    reconnects: 0,
    connectErrors: 0,
    ticketErrors: 0,
    closeCodes: {},
    timeline: new Map(),
    loadgenRss: 0,
  };
  for (const r of reports) {
    agg.connected += r.connected;
    agg.everConnected += r.everConnected;
    agg.connecting += r.connecting;
    for (const [group, data] of Object.entries(r.latency)) {
      let h = agg.latency.get(group);
      if (h === undefined) {
        h = new Histogram();
        agg.latency.set(group, h);
      }
      h.merge(data);
    }
    agg.ticketLatency.merge(r.ticketLatency);
    agg.connectLatency.merge(r.connectLatency);
    agg.received += r.received;
    agg.ephReceived += r.ephReceived;
    agg.published += r.published;
    agg.publishErrors += r.publishErrors;
    agg.gaps += r.gaps;
    agg.duplicates += r.duplicates;
    agg.resets += r.resets;
    agg.resumed += r.resumed;
    agg.reconnects += r.reconnects;
    agg.connectErrors += r.connectErrors;
    agg.ticketErrors += r.ticketErrors;
    for (const [code, n] of Object.entries(r.closeCodes)) agg.closeCodes[code] = (agg.closeCodes[code] ?? 0) + n;
    for (const [key, b] of Object.entries(r.timeline)) {
      const k = Number(key);
      const cur = agg.timeline.get(k) ?? { attempts: 0, opened: 0, failed: 0, ticketErrors: 0 };
      cur.attempts += b.attempts;
      cur.opened += b.opened;
      cur.failed += b.failed;
      cur.ticketErrors += b.ticketErrors;
      agg.timeline.set(k, cur);
    }
    agg.loadgenRss = Math.max(agg.loadgenRss, r.memory.rss);
  }
  return agg;
}

class Pool {
  private readonly workers: Worker[] = [];
  private pending = new Map<Worker, (m: WorkerToMain) => void>();

  static async create(options: Options, specs: ClientSpec[], t0: number): Promise<Pool> {
    const pool = new Pool();
    const n = Math.max(1, Math.min(options.workers, specs.length));
    const targets = targetsFor(options);
    const slices: ClientSpec[][] = Array.from({ length: n }, () => []);
    specs.forEach((s, i) => slices[i % n]?.push(s));
    await Promise.all(
      slices.map(async (clients, workerId) => {
        const worker = new Worker(new URL("./worker.js", import.meta.url));
        pool.workers.push(worker);
        worker.on("message", (m: WorkerToMain) => {
          const handler = pool.pending.get(worker);
          if (handler !== undefined) {
            pool.pending.delete(worker);
            handler(m);
          }
        });
        worker.on("error", (e) => console.error(`worker ${workerId} error`, e));
        const config: WorkerConfig = {
          workerId,
          targets,
          codec: options.codec,
          deflate: options.deflate,
          jitter: options.jitter,
          backoffBaseMs: options.backoffBaseMs,
          backoffMaxMs: options.backoffMaxMs,
          jwtSecret: options.jwtSecret,
          connectRatePerSec: options.connectRate / n,
          ticketConcurrency: options.ticketConcurrency,
          t0,
          bucketMs: 100,
        };
        await pool.request(worker, { cmd: "init", config, clients });
      }),
    );
    return pool;
  }

  private request(worker: Worker, message: MainToWorker): Promise<WorkerToMain> {
    return new Promise((resolve) => {
      this.pending.set(worker, resolve);
      worker.postMessage(message);
    });
  }

  broadcast(message: MainToWorker): void {
    for (const w of this.workers) w.postMessage(message);
  }

  async report(reset = false): Promise<Aggregate> {
    const replies = await Promise.all(this.workers.map((w) => this.request(w, { cmd: "report", reset })));
    return aggregate(replies.flatMap((m) => (m.evt === "report" ? [m.report] : [])));
  }

  async close(): Promise<void> {
    await Promise.all(this.workers.map((w) => this.request(w, { cmd: "close" })));
    await Promise.all(this.workers.map((w) => w.terminate()));
  }
}

interface NodeSnapshot {
  url: string;
  samples: Sample[];
  at: number;
}

class NodeMetrics {
  maxLagP99 = new Map<string, number>();
  maxHeap = new Map<string, number>();
  maxRss = new Map<string, number>();
  eluSamples = new Map<string, number[]>();
  private timer: NodeJS.Timeout | null = null;

  constructor(readonly urls: string[]) {}

  async gc(serverKey: string): Promise<void> {
    await Promise.allSettled(
      this.urls.map((u) => fetch(`${u}/admin/gc`, { method: "POST", headers: { "x-api-key": serverKey } }).then((r) => r.text())),
    );
    await sleep(500);
  }

  async snapshot(): Promise<NodeSnapshot[]> {
    const results = await Promise.allSettled(this.urls.map((u) => scrape(`${u}/metrics`)));
    const out: NodeSnapshot[] = [];
    results.forEach((r, i) => {
      if (r.status === "fulfilled") out.push({ url: this.urls[i] as string, samples: r.value, at: Date.now() });
    });
    return out;
  }

  private observe(snaps: NodeSnapshot[]): void {
    for (const s of snaps) {
      const lag = pick(s.samples, "rt_event_loop_lag_seconds", { quantile: "0.99" });
      if (Number.isFinite(lag)) this.maxLagP99.set(s.url, Math.max(this.maxLagP99.get(s.url) ?? 0, lag));
      const heap = pick(s.samples, "nodejs_heap_size_used_bytes");
      if (Number.isFinite(heap)) this.maxHeap.set(s.url, Math.max(this.maxHeap.get(s.url) ?? 0, heap));
      const rss = pick(s.samples, "process_resident_memory_bytes");
      if (Number.isFinite(rss)) this.maxRss.set(s.url, Math.max(this.maxRss.get(s.url) ?? 0, rss));
      const elu = pick(s.samples, "rt_event_loop_utilization");
      if (Number.isFinite(elu)) {
        const list = this.eluSamples.get(s.url) ?? [];
        list.push(elu);
        this.eluSamples.set(s.url, list);
      }
    }
  }

  async startSampling(intervalMs = 2000): Promise<NodeSnapshot[]> {
    this.maxLagP99.clear();
    this.maxHeap.clear();
    this.maxRss.clear();
    this.eluSamples.clear();
    const first = await this.snapshot();
    this.timer = setInterval(() => {
      void this.snapshot().then((s) => this.observe(s));
    }, intervalMs);
    return first;
  }

  async stopSampling(): Promise<NodeSnapshot[]> {
    if (this.timer !== null) clearInterval(this.timer);
    this.timer = null;
    const last = await this.snapshot();
    this.observe(last);
    return last;
  }
}

interface ServerSummary {
  nodes: number;
  msgsOutPerSec: number;
  msgsOutByKindPerSec: Record<string, number>;
  msgsInPerSec: number;
  eventLoopP99MsMax: number;
  eluAvg: number;
  cpuPercentAvg: number;
  heapMbMax: number;
  rssMbMax: number;
  fanoutP50Ms: number;
  fanoutP99Ms: number;
  slowConsumerDisconnects: number;
  ephemeralDropped: number;
  upgradesOk: number;
  connections: number;
  resumeMessages: number;
}

function summarizeServer(before: NodeSnapshot[], after: NodeSnapshot[], metrics: NodeMetrics): ServerSummary {
  let outTotal = 0;
  let inTotal = 0;
  const byKind: Record<string, number> = {};
  let cpu = 0;
  let seconds = 0;
  let slow = 0;
  let dropped = 0;
  let upgrades = 0;
  let connections = 0;
  let resumeMessages = 0;
  const fanBefore = new Map<number, number>();
  const fanAfter = new Map<number, number>();
  for (const a of after) {
    const b = before.find((x) => x.url === a.url);
    if (b === undefined) continue;
    const dt = (a.at - b.at) / 1000;
    seconds = Math.max(seconds, dt);
    const delta = (name: string, labels: Record<string, string> = {}) => {
      const va = pick(a.samples, name, labels);
      const vb = pick(b.samples, name, labels);
      return Number.isFinite(va) && Number.isFinite(vb) ? va - vb : 0;
    };
    outTotal += delta("rt_messages_out_total");
    inTotal += delta("rt_messages_in_total");
    for (const kind of ["durable", "ephemeral", "presence", "control", "history"]) {
      byKind[kind] = (byKind[kind] ?? 0) + delta("rt_messages_out_total", { kind });
    }
    cpu += dt > 0 ? delta("process_cpu_seconds_total") / dt : 0;
    slow += delta("rt_slow_consumer_disconnects_total");
    dropped += delta("rt_ephemeral_dropped_total");
    upgrades += delta("rt_upgrades_total", { result: "ok" });
    resumeMessages += delta("rt_resume_messages_sum");
    connections += pick(a.samples, "rt_connections");
    for (const [k, v] of histogramBuckets(b.samples, "rt_fanout_duration_seconds")) fanBefore.set(k, (fanBefore.get(k) ?? 0) + v);
    for (const [k, v] of histogramBuckets(a.samples, "rt_fanout_duration_seconds")) fanAfter.set(k, (fanAfter.get(k) ?? 0) + v);
  }
  const elus = Array.from(metrics.eluSamples.values()).flat();
  const mb = (v: number) => Math.round((v / 1024 / 1024) * 10) / 10;
  const round = (v: number, d = 2) => Math.round(v * 10 ** d) / 10 ** d;
  return {
    nodes: after.length,
    msgsOutPerSec: seconds > 0 ? Math.round(outTotal / seconds) : 0,
    msgsOutByKindPerSec: Object.fromEntries(Object.entries(byKind).map(([k, v]) => [k, seconds > 0 ? Math.round(v / seconds) : 0])),
    msgsInPerSec: seconds > 0 ? Math.round(inTotal / seconds) : 0,
    eventLoopP99MsMax: round(Math.max(0, ...metrics.maxLagP99.values()) * 1000),
    eluAvg: round(elus.length === 0 ? 0 : elus.reduce((x, y) => x + y, 0) / elus.length, 3),
    cpuPercentAvg: round((cpu / Math.max(1, after.length)) * 100, 1),
    heapMbMax: mb(Math.max(0, ...metrics.maxHeap.values())),
    rssMbMax: mb(Math.max(0, ...metrics.maxRss.values())),
    fanoutP50Ms: round(quantileFromBuckets(fanBefore, fanAfter, 0.5) * 1000, 3),
    fanoutP99Ms: round(quantileFromBuckets(fanBefore, fanAfter, 0.99) * 1000, 3),
    slowConsumerDisconnects: slow,
    ephemeralDropped: dropped,
    upgradesOk: upgrades,
    connections,
    resumeMessages,
  };
}

function latencySummary(agg: Aggregate): Record<string, ReturnType<Histogram["summary"]>> {
  return Object.fromEntries(Array.from(agg.latency, ([k, h]) => [k, h.summary()]));
}

async function waitConnected(pool: Pool, expected: number, timeoutMs: number, quiet = false): Promise<Aggregate> {
  const deadline = Date.now() + timeoutMs;
  let last = await pool.report();
  let lastLog = 0;
  while (last.connected < expected && Date.now() < deadline) {
    await sleep(500);
    last = await pool.report();
    if (!quiet && Date.now() - lastLog > 3000) {
      lastLog = Date.now();
      console.log(`  connected ${last.connected}/${expected} (connect errors ${last.connectErrors}, ticket errors ${last.ticketErrors})`);
    }
  }
  return last;
}

function nodeTotals(snaps: NodeSnapshot[]): { heap: number; rss: number; connections: number; cpu: number } {
  let heap = 0;
  let rss = 0;
  let connections = 0;
  let cpu = 0;
  for (const s of snaps) {
    heap += pick(s.samples, "nodejs_heap_size_used_bytes");
    rss += pick(s.samples, "process_resident_memory_bytes");
    connections += pick(s.samples, "rt_connections");
    cpu += pick(s.samples, "process_cpu_seconds_total");
  }
  return { heap, rss, connections, cpu };
}

function roomSpecs(options: Options, sizes: number[], extra: (room: string, index: number) => Partial<ClientSpec>): ClientSpec[] {
  const specs: ClientSpec[] = [];
  let id = 0;
  sizes.forEach((size, r) => {
    const room = `room:lg-${options.scenario}-${r}`;
    for (let k = 0; k < size; k++) {
      specs.push({
        id,
        uid: `lg-${id}`,
        rooms: [room],
        target: id % (options.via === "haproxy" ? 1 : options.nodes.length),
        presence: true,
        ...extra(room, k),
      });
      id++;
    }
  });
  return specs;
}

function randomRoomSizes(total: number, min: number, max: number): number[] {
  const sizes: number[] = [];
  let left = total;
  while (left > 0) {
    const size = Math.min(left, min + Math.floor(Math.random() * (max - min + 1)));
    sizes.push(size);
    left -= size;
  }
  return sizes;
}

async function scenarioIdle(options: Options, t0: number) {
  const metrics = new NodeMetrics(options.nodes);
  const specs: ClientSpec[] = Array.from({ length: options.connections }, (_, id) => ({
    id,
    uid: `lg-idle-${id}`,
    rooms: options.subscribe === "user" ? [`user:lg-idle-${id}`] : [],
    target: id % options.nodes.length,
    presence: false,
  }));
  await metrics.gc(options.serverKey);
  const base = nodeTotals(await metrics.snapshot());
  const steps: { connections: number; heapMb: number; rssMb: number; heapPerConnKb: number; rssPerConnKb: number; seconds: number }[] = [];
  const stepSize = Math.ceil(options.connections / options.steps);
  const pools: Pool[] = [];
  let connectedSoFar = 0;
  const connectStarted = Date.now();
  for (let s = 0; s < options.steps; s++) {
    const slice = specs.slice(s * stepSize, (s + 1) * stepSize);
    if (slice.length === 0) break;
    const stepStart = Date.now();
    const pool = await Pool.create(options, slice, t0);
    pools.push(pool);
    pool.broadcast({ cmd: "connect" });
    const reached = await waitConnected(pool, slice.length, 120000);
    connectedSoFar += reached.connected;
    const seconds = (Date.now() - stepStart) / 1000;
    await sleep(5000);
    await metrics.gc(options.serverKey);
    const totals = nodeTotals(await metrics.snapshot());
    const step = {
      connections: connectedSoFar,
      heapMb: Math.round(((totals.heap - base.heap) / 1024 / 1024) * 10) / 10,
      rssMb: Math.round(((totals.rss - base.rss) / 1024 / 1024) * 10) / 10,
      heapPerConnKb: Math.round(((totals.heap - base.heap) / connectedSoFar / 1024) * 100) / 100,
      rssPerConnKb: Math.round(((totals.rss - base.rss) / connectedSoFar / 1024) * 100) / 100,
      seconds: Math.round(seconds * 10) / 10,
    };
    steps.push(step);
    console.log(`  step ${s + 1}: ${JSON.stringify(step)}`);
  }
  const connectSeconds = (Date.now() - connectStarted) / 1000;
  const before = await metrics.startSampling();
  await sleep(options.duration * 1000);
  const after = await metrics.stopSampling();
  const reports = await Promise.all(pools.map((p) => p.report()));
  const connectLatency = new Histogram();
  let connected = 0;
  let errors = 0;
  for (const r of reports) {
    connectLatency.merge(r.connectLatency.toJSON());
    connected += r.connected;
    errors += r.connectErrors;
  }
  const server = summarizeServer(before, after, metrics);
  const cpuBefore = nodeTotals(before).cpu;
  const cpuAfter = nodeTotals(after).cpu;
  const totals = nodeTotals(after);
  for (const p of pools) await p.close();
  const last = steps[steps.length - 1];
  return {
    connections: connected,
    connectErrors: errors,
    connectSeconds: Math.round(connectSeconds * 10) / 10,
    connectLatencyMs: connectLatency.summary(),
    steps,
    heapPerConnKb: last?.heapPerConnKb ?? 0,
    rssPerConnKb: last?.rssPerConnKb ?? 0,
    heapPerConnSlopeKb: steps.length >= 2 ? Math.round((((steps[steps.length - 1]?.heapMb ?? 0) - (steps[0]?.heapMb ?? 0)) * 1024 / ((steps[steps.length - 1]?.connections ?? 1) - (steps[0]?.connections ?? 0))) * 100) / 100 : null,
    idleCpuPercentPerNode: Math.round((((cpuAfter - cpuBefore) / options.duration) * 100 / Math.max(1, options.nodes.length)) * 100) / 100,
    nodeHeapMbTotal: Math.round((totals.heap / 1024 / 1024) * 10) / 10,
    nodeRssMbTotal: Math.round((totals.rss / 1024 / 1024) * 10) / 10,
    server,
  };
}

async function measuredRun(options: Options, specs: ClientSpec[], t0: number, extra?: (pool: Pool) => Promise<Record<string, unknown>>) {
  const metrics = new NodeMetrics(options.nodes);
  const pool = await Pool.create(options, specs, t0);
  const connectStart = Date.now();
  pool.broadcast({ cmd: "connect" });
  const reached = await waitConnected(pool, specs.length, 180000);
  const connectSeconds = (Date.now() - connectStart) / 1000;
  console.log(`  connected ${reached.connected}/${specs.length} in ${connectSeconds.toFixed(1)} s`);
  await sleep(2000);
  pool.broadcast({ cmd: "start" });
  await sleep(options.warmup * 1000);
  await pool.report(true);
  const before = await metrics.startSampling();
  const started = Date.now();
  const extraResult = extra === undefined ? {} : await extra(pool);
  const remaining = options.duration * 1000 - (Date.now() - started);
  if (remaining > 0) await sleep(remaining);
  const after = await metrics.stopSampling();
  const agg = await pool.report(true);
  const seconds = (Date.now() - started) / 1000;
  pool.broadcast({ cmd: "stop" });
  await sleep(2000);
  const tail = await pool.report();
  await pool.close();
  return {
    connections: reached.connected,
    connectSeconds: Math.round(connectSeconds * 10) / 10,
    seconds: Math.round(seconds * 10) / 10,
    latencyMs: latencySummary(agg),
    clientMsgsInPerSec: Math.round(agg.received / seconds),
    clientEphInPerSec: Math.round(agg.ephReceived / seconds),
    publishedPerSec: Math.round((agg.published / seconds) * 10) / 10,
    gaps: tail.gaps,
    duplicates: tail.duplicates,
    resets: tail.resets,
    resumedMessages: tail.resumed,
    reconnects: tail.reconnects,
    closeCodes: tail.closeCodes,
    publishErrors: agg.publishErrors,
    loadgenRssMb: Math.round(agg.loadgenRss / 1024 / 1024),
    server: summarizeServer(before, after, metrics),
    ...extraResult,
  };
}

async function scenarioFanout(options: Options, t0: number) {
  const room = `room:lg-big-${Date.now()}`;
  const specs: ClientSpec[] = [];
  for (let i = 0; i < options.connections; i++) {
    specs.push({ id: i, uid: `lg-big-${i}`, rooms: [room], target: i % options.nodes.length, presence: false, group: "big-room" });
  }
  specs.push({
    id: options.connections,
    uid: "lg-big-publisher",
    rooms: [],
    target: 0,
    presence: false,
    publishRoom: room,
    publishIntervalMs: 1000 / options.rate,
    publishBytes: options.payload,
  });
  for (let p = 0; p < options.probes; p++) {
    const probeRoom = `room:lg-probe-${p}-${Date.now()}`;
    const id = options.connections + 1 + p * 2;
    specs.push({ id, uid: `lg-probe-sub-${p}`, rooms: [probeRoom], target: (p + 1) % options.nodes.length, presence: false, group: "probe-rooms" });
    specs.push({
      id: id + 1,
      uid: `lg-probe-pub-${p}`,
      rooms: [],
      target: p % options.nodes.length,
      presence: false,
      publishRoom: probeRoom,
      publishIntervalMs: 200,
    });
  }
  return measuredRun(options, specs, t0);
}

async function scenarioManyRooms(options: Options, t0: number) {
  const sizes = randomRoomSizes(options.connections, 2, 10);
  const slowEvery = options.slowPercent > 0 ? Math.round(100 / options.slowPercent) : 0;
  let index = 0;
  const specs = roomSpecs(options, sizes, (room) => {
    const i = index++;
    const slow = slowEvery > 0 && i % slowEvery === slowEvery - 1;
    return {
      publishRoom: room,
      publishIntervalMs: 1000 / options.rate,
      publishBytes: options.payload,
      cursorHz: slow ? 0 : options.cursorHz,
      slow,
      group: slow ? "slow" : "normal",
    };
  });
  const result = await measuredRun(options, specs, t0);
  return { rooms: sizes.length, slowClients: specs.filter((s) => s.slow === true).length, ...result };
}

async function scenarioStorm(options: Options, t0: number) {
  const roomSize = 50;
  const sizes = Array.from({ length: Math.ceil(options.connections / roomSize) }, (_, i) => Math.min(roomSize, options.connections - i * roomSize));
  const specs = roomSpecs(options, sizes, () => ({ presence: false }));
  let id = specs.length;
  sizes.forEach((_, r) => {
    specs.push({
      id,
      uid: `lg-storm-pub-${r}`,
      rooms: [],
      target: 0,
      presence: false,
      publishRoom: `room:lg-${options.scenario}-${r}`,
      publishIntervalMs: 1000 / options.rate,
      group: "publisher",
    });
    id++;
  });
  const metrics = new NodeMetrics(options.nodes);
  const pool = await Pool.create(options, specs, t0);
  pool.broadcast({ cmd: "connect" });
  const reached = await waitConnected(pool, specs.length, 180000);
  console.log(`  connected ${reached.connected}/${specs.length}`);
  await sleep(3000);
  pool.broadcast({ cmd: "start" });
  await sleep(5000);
  const preKill = await pool.report(true);
  const before = await metrics.startSampling(1000);
  const killAt = Date.now();
  console.log(`  killing: ${options.killCmd}`);
  await execAsync(options.killCmd, { cwd: process.cwd() });
  let dropped = 0;
  let recoveredAt: number | null = null;
  let minConnected = preKill.connected;
  const curve: { t: number; connected: number }[] = [];
  while (Date.now() - killAt < options.duration * 1000) {
    await sleep(100);
    const r = await pool.report();
    minConnected = Math.min(minConnected, r.connected);
    dropped = Math.max(dropped, preKill.connected - r.connected);
    curve.push({ t: Date.now() - killAt, connected: r.connected });
    if (recoveredAt === null && dropped > 0 && r.connected >= preKill.connected) recoveredAt = Date.now();
  }
  const after = await metrics.stopSampling();
  pool.broadcast({ cmd: "stop" });
  await sleep(2000);
  const final = await pool.report();
  await pool.close();
  console.log(`  restarting: ${options.restartCmd}`);
  await execAsync(options.restartCmd, { cwd: process.cwd() }).catch((e: unknown) => console.error(String(e)));
  const killBucket = Math.floor((killAt - t0) / 100);
  const timeline: { tMs: number; attempts: number; opened: number; failed: number }[] = [];
  let peakUpgradesPerSec = 0;
  const keys = Array.from(final.timeline.keys()).filter((k) => k >= killBucket - 10).sort((a, b) => a - b);
  for (const k of keys) {
    const b = final.timeline.get(k);
    if (b === undefined) continue;
    timeline.push({ tMs: (k - killBucket) * 100, attempts: b.attempts, opened: b.opened, failed: b.failed });
  }
  let peak100ms = 0;
  for (let i = 0; i < timeline.length; i++) {
    let sum = 0;
    for (let j = i; j < timeline.length && timeline[j]!.tMs < timeline[i]!.tMs + 1000; j++) sum += timeline[j]!.opened;
    peakUpgradesPerSec = Math.max(peakUpgradesPerSec, sum);
    peak100ms = Math.max(peak100ms, timeline[i]!.opened);
  }
  const reconnectedBy = (fraction: number): number | null => {
    const target = preKill.connected - dropped * (1 - fraction);
    const point = curve.find((c) => c.connected >= target && c.t > 0 && dropped > 0);
    return point === undefined ? null : Math.round(point.t / 100) / 10;
  };
  let failedAfterKill = 0;
  let attemptsAfterKill = 0;
  for (const t of timeline) {
    if (t.tMs >= 0) {
      failedAfterKill += t.failed;
      attemptsAfterKill += t.attempts;
    }
  }
  return {
    jitter: options.jitter,
    connections: preKill.connected,
    dropped,
    minConnected,
    recoverySeconds: recoveredAt === null ? null : Math.round(((recoveredAt - killAt) / 1000) * 10) / 10,
    peakUpgradesPerSec,
    peakUpgradesPer100ms: peak100ms,
    peakInstantRatePerSec: peak100ms * 10,
    reconnected50Seconds: reconnectedBy(0.5),
    reconnected95Seconds: reconnectedBy(0.95),
    attemptsAfterKill,
    failedAttemptsAfterKill: failedAfterKill,
    ticketLatencyMs: final.ticketLatency.summary(),
    gaps: final.gaps,
    duplicates: final.duplicates,
    resets: final.resets,
    resumedMessages: final.resumed,
    closeCodes: final.closeCodes,
    timeline,
    curve,
    server: summarizeServer(before, after, metrics),
  };
}

async function scenarioResume(options: Options, t0: number) {
  const rooms = options.rooms > 0 ? options.rooms : Math.max(1, Math.round(options.connections / 30));
  const per = Math.ceil(options.connections / rooms);
  const sizes = Array.from({ length: rooms }, (_, i) => Math.max(0, Math.min(per, options.connections - i * per))).filter((s) => s > 0);
  const specs = roomSpecs(options, sizes, () => ({
    presence: false,
    chaos: { minOnMs: 5000, maxOnMs: 20000, minOffMs: 1000, maxOffMs: 30000 },
    group: "chaos",
  }));
  let id = specs.length;
  sizes.forEach((_, r) => {
    specs.push({
      id,
      uid: `lg-resume-pub-${r}`,
      rooms: [],
      target: id % options.nodes.length,
      presence: false,
      publishRoom: `room:lg-${options.scenario}-${r}`,
      publishIntervalMs: 1000 / options.rate,
      group: "publisher",
    });
    id++;
  });
  const result = await measuredRun(options, specs, t0);
  return { rooms: sizes.length, ...result };
}

function markdown(result: Record<string, unknown>): string {
  return "```json\n" + JSON.stringify(result, null, 2) + "\n```\n";
}

async function main(): Promise<void> {
  const options = parseArgs(process.argv.slice(2));
  const scenarios: Record<string, (o: Options, t0: number) => Promise<Record<string, unknown>>> = {
    idle: scenarioIdle,
    "fanout-big-room": scenarioFanout,
    "many-rooms": scenarioManyRooms,
    "slow-consumers": scenarioManyRooms,
    "reconnect-storm": scenarioStorm,
    resume: scenarioResume,
  };
  const run = scenarios[options.scenario];
  if (run === undefined) {
    console.log(`usage: rt-loadgen <${Object.keys(scenarios).join("|")}> [--connections N] [--duration S] [--via direct|haproxy] [--jitter on|off] [--codec json|msgpack] [--deflate] [--label name] [--out dir]`);
    process.exit(1);
  }
  const t0 = Date.now();
  console.log(`scenario ${options.scenario} (${options.label}): ${options.connections} connections, ${options.workers} workers, via ${options.via}`);
  const result = await run(options, t0);
  const record = {
    scenario: options.scenario,
    label: options.label,
    at: new Date().toISOString(),
    environment: {
      cpus: cpus().length,
      cpuModel: cpus()[0]?.model ?? "unknown",
      memoryGb: Math.round(totalmem() / 1024 ** 3),
      node: process.version,
      platform: process.platform,
    },
    options: { ...options, jwtSecret: undefined, serverKey: undefined },
    result,
  };
  mkdirSync(options.out, { recursive: true });
  const file = join(options.out, `${options.label}.json`);
  writeFileSync(file, JSON.stringify(record, null, 2));
  writeFileSync(join(options.out, `${options.label}.md`), `# ${options.label}\n\n${markdown(result)}`);
  const { timeline: _timeline, curve: _curve, ...brief } = result as Record<string, unknown>;
  console.log(JSON.stringify(brief, null, 2));
  console.log(`written ${file}`);
}

main().catch((error: unknown) => {
  console.error(error);
  process.exit(1);
});
