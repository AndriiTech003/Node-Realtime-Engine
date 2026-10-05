import { Agent, request } from "node:http";
import { parentPort } from "node:worker_threads";
import WebSocket from "ws";
import { codecByName, type Codec } from "@ashamrai/realtime-protocol";
import { signJwt } from "@ashamrai/realtime-server";
import { Histogram } from "./histogram.js";
import type { ClientSpec, MainToWorker, TimelineBucket, WorkerConfig, WorkerReport, WorkerToMain } from "./types.js";

const now = () => performance.timeOrigin + performance.now();

interface ServerFrameLike {
  t?: unknown;
  id?: unknown;
  ch?: unknown;
  seq?: unknown;
  d?: unknown;
}

class Stats {
  latency = new Map<string, Histogram>();
  ticketLatency = new Histogram();
  connectLatency = new Histogram();
  received = 0;
  ephReceived = 0;
  published = 0;
  publishErrors = 0;
  gaps = 0;
  duplicates = 0;
  resets = 0;
  resumed = 0;
  reconnects = 0;
  connectErrors = 0;
  ticketErrors = 0;
  closeCodes: Record<string, number> = {};
  timeline: Record<string, TimelineBucket> = {};

  bucket(config: WorkerConfig): TimelineBucket {
    const key = String(Math.floor((Date.now() - config.t0) / config.bucketMs));
    let b = this.timeline[key];
    if (b === undefined) {
      b = { attempts: 0, opened: 0, failed: 0, ticketErrors: 0 };
      this.timeline[key] = b;
    }
    return b;
  }

  record(group: string, value: number): void {
    let h = this.latency.get(group);
    if (h === undefined) {
      h = new Histogram();
      this.latency.set(group, h);
    }
    h.record(value);
  }

  resetMeasurements(): void {
    this.latency = new Map();
    this.ticketLatency = new Histogram();
    this.connectLatency = new Histogram();
    this.received = 0;
    this.ephReceived = 0;
    this.published = 0;
  }
}

class TicketPool {
  private readonly agents: Agent[];
  private readonly tokens = new Map<string, string>();

  constructor(private readonly config: WorkerConfig) {
    this.agents = config.targets.map(() => new Agent({ keepAlive: true, maxSockets: config.ticketConcurrency }));
  }

  token(uid: string): string {
    let token = this.tokens.get(uid);
    if (token === undefined) {
      token = signJwt({ sub: uid, name: uid }, this.config.jwtSecret, 24 * 3600);
      this.tokens.set(uid, token);
    }
    return token;
  }

  fetch(uid: string, target: number): Promise<string> {
    const base = new URL(this.config.targets[target]?.http ?? "");
    return new Promise((resolve, reject) => {
      const req = request(
        {
          host: base.hostname,
          port: base.port,
          path: "/v1/tickets",
          method: "POST",
          agent: this.agents[target],
          headers: { authorization: `Bearer ${this.token(uid)}`, "content-length": 0 },
          timeout: 10000,
        },
        (res) => {
          const chunks: Buffer[] = [];
          res.on("data", (c: Buffer) => chunks.push(c));
          res.on("end", () => {
            if (res.statusCode !== 200) {
              reject(new Error(`ticket status ${res.statusCode}`));
              return;
            }
            try {
              resolve((JSON.parse(Buffer.concat(chunks).toString("utf8")) as { ticket: string }).ticket);
            } catch (error) {
              reject(error instanceof Error ? error : new Error(String(error)));
            }
          });
        },
      );
      req.on("timeout", () => req.destroy(new Error("ticket timeout")));
      req.on("error", reject);
      req.end();
    });
  }

  close(): void {
    for (const agent of this.agents) agent.destroy();
  }
}

class LgClient {
  ws: WebSocket | null = null;
  open = false;
  everOpened = false;
  stopped = false;
  offline = false;
  private attempt = 0;
  private nextId = 1;
  private readonly last = new Map<string, number | null>();
  private readonly subs = new Map<number, { ch: string; resuming: boolean }>();
  private readonly resuming = new Set<string>();
  nextPubAt = 0;
  nextCursorAt = 0;
  nextChaosAt = 0;
  private reconnectTimer: NodeJS.Timeout | null = null;
  private pad: string;

  constructor(
    readonly spec: ClientSpec,
    private readonly ctx: WorkerContext,
  ) {
    for (const room of spec.rooms) this.last.set(room, null);
    this.pad = spec.publishBytes !== undefined && spec.publishBytes > 0 ? "x".repeat(spec.publishBytes) : "";
  }

  private get targetIndex(): number {
    return this.spec.target % this.ctx.config.targets.length;
  }

  async connect(): Promise<void> {
    if (this.stopped || this.offline) return;
    const { config, stats, tickets } = this.ctx;
    const started = now();
    stats.bucket(config).attempts++;
    let ticket: string;
    try {
      ticket = await tickets.fetch(this.spec.uid, this.targetIndex);
      stats.ticketLatency.record(now() - started);
    } catch {
      stats.ticketErrors++;
      stats.bucket(config).ticketErrors++;
      stats.bucket(config).failed++;
      this.scheduleReconnect(1006);
      return;
    }
    if (this.stopped || this.offline) return;
    const target = config.targets[this.targetIndex];
    const ws = new WebSocket(`${target?.ws ?? ""}?ticket=${ticket}`, this.ctx.codec.subprotocol, {
      perMessageDeflate: config.deflate,
      handshakeTimeout: 15000,
    });
    this.ws = ws;
    ws.binaryType = "nodebuffer";
    let opened = false;
    ws.on("message", (data: Buffer, isBinary: boolean) => {
      if (this.ws !== ws) return;
      this.onData(data, isBinary, started, () => {
        opened = true;
      });
    });
    ws.on("close", (code: number) => {
      if (this.ws !== ws) return;
      this.ws = null;
      const wasOpen = this.open;
      this.open = false;
      if (!opened) {
        stats.connectErrors++;
        stats.bucket(config).failed++;
      }
      stats.closeCodes[String(code)] = (stats.closeCodes[String(code)] ?? 0) + 1;
      if (wasOpen) this.ctx.openCount--;
      this.scheduleReconnect(code);
    });
    ws.on("error", () => undefined);
  }

  private scheduleReconnect(code: number): void {
    if (this.stopped || this.offline || this.spec.slow === true) return;
    const { config } = this.ctx;
    const attempt = this.attempt++;
    let delay: number;
    if (code === 4008 && attempt === 0) delay = 0;
    else {
      const exp = Math.min(config.backoffMaxMs, config.backoffBaseMs * 2 ** attempt);
      delay = config.jitter ? Math.floor(Math.random() * exp) : exp;
    }
    if (this.reconnectTimer !== null) clearTimeout(this.reconnectTimer);
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      void this.connect();
    }, delay);
  }

  private send(frame: Record<string, unknown>): void {
    const ws = this.ws;
    if (ws === null || ws.readyState !== WebSocket.OPEN) return;
    ws.send(this.ctx.codec.encode(frame));
  }

  private onData(data: Buffer, isBinary: boolean, started: number, markOpened: () => void): void {
    let frame: ServerFrameLike;
    try {
      frame = this.ctx.codec.decode(isBinary ? data : data.toString("utf8")) as ServerFrameLike;
    } catch {
      return;
    }
    const { stats } = this.ctx;
    switch (frame.t) {
      case "msg": {
        const ch = frame.ch as string;
        const seq = frame.seq as number;
        const last = this.last.get(ch);
        if (last === undefined) return;
        if (last !== null) {
          if (seq <= last) {
            stats.duplicates++;
            return;
          }
          if (seq > last + 1) stats.gaps += seq - last - 1;
        }
        this.last.set(ch, seq);
        stats.received++;
        const replayed = this.resuming.has(ch);
        if (replayed) stats.resumed++;
        const d = frame.d as { ts?: unknown } | null;
        if (d !== null && typeof d === "object" && typeof d.ts === "number") {
          stats.record(replayed ? `${this.spec.group ?? "main"}-replayed` : (this.spec.group ?? "main"), now() - d.ts);
        }
        return;
      }
      case "eph":
        stats.ephReceived++;
        return;
      case "hello":
        markOpened();
        this.open = true;
        this.ctx.openCount++;
        if (this.everOpened) stats.reconnects++;
        else this.ctx.everConnected++;
        this.everOpened = true;
        this.attempt = 0;
        stats.connectLatency.record(now() - started);
        stats.bucket(this.ctx.config).opened++;
        for (const room of this.spec.rooms) {
          const id = this.nextId++;
          const last = this.last.get(room) ?? null;
          const frameOut: Record<string, unknown> = { t: "sub", id, ch: room };
          if (last !== null) {
            frameOut["from"] = last;
            this.resuming.add(room);
          }
          if (!this.spec.presence) frameOut["presence"] = false;
          this.subs.set(id, { ch: room, resuming: last !== null });
          this.send(frameOut);
        }
        return;
      case "ok": {
        const sub = this.subs.get(frame.id as number);
        if (sub === undefined) return;
        this.subs.delete(frame.id as number);
        this.resuming.delete(sub.ch);
        const last = this.last.get(sub.ch);
        if (last === null && typeof frame.seq === "number") this.last.set(sub.ch, frame.seq);
        if (this.spec.slow === true && this.subs.size === 0) {
          const socket = (this.ws as unknown as { _socket?: { pause(): void } } | null)?._socket;
          socket?.pause();
        }
        return;
      }
      case "reset":
        stats.resets++;
        this.last.set(frame.ch as string, frame.seq as number);
        return;
      case "err":
        stats.publishErrors++;
        return;
      default:
        return;
    }
  }

  tick(t: number): void {
    if (!this.open || !this.ctx.running) return;
    const spec = this.spec;
    if (spec.publishRoom !== undefined && spec.publishIntervalMs !== undefined && this.nextPubAt === 0) {
      this.nextPubAt = t + Math.random() * spec.publishIntervalMs;
    } else if (spec.publishRoom !== undefined && spec.publishIntervalMs !== undefined && t >= this.nextPubAt) {
      this.nextPubAt += spec.publishIntervalMs;
      if (this.nextPubAt < t) this.nextPubAt = t + spec.publishIntervalMs;
      const id = this.nextId++;
      this.send({
        t: "pub",
        id,
        ch: spec.publishRoom,
        cmid: `${spec.uid}-${id}`,
        d: this.pad.length > 0 ? { ts: now(), p: this.pad } : { ts: now() },
      });
      this.ctx.stats.published++;
    }
    if (spec.cursorHz !== undefined && spec.cursorHz > 0 && t >= this.nextCursorAt) {
      const interval = 1000 / spec.cursorHz;
      this.nextCursorAt = this.nextCursorAt === 0 ? t + Math.random() * interval : Math.max(this.nextCursorAt + interval, t);
      const room = spec.rooms[0];
      if (room !== undefined) this.send({ t: "eph", ch: room, d: { k: "cursor", x: Math.random(), y: Math.random() } });
    }
    if (spec.chaos !== undefined && t >= this.nextChaosAt) {
      const c = spec.chaos;
      if (this.nextChaosAt === 0) {
        this.nextChaosAt = t + c.minOnMs + Math.random() * (c.maxOnMs - c.minOnMs);
        return;
      }
      const offFor = c.minOffMs + Math.random() * (c.maxOffMs - c.minOffMs);
      this.offline = true;
      const ws = this.ws;
      if (ws !== null) ws.terminate();
      this.nextChaosAt = Number.POSITIVE_INFINITY;
      setTimeout(() => {
        this.offline = false;
        this.attempt = 0;
        this.nextChaosAt = 0;
        if (!this.stopped) void this.connect();
      }, offFor);
    }
  }

  stop(): void {
    this.stopped = true;
    if (this.reconnectTimer !== null) clearTimeout(this.reconnectTimer);
    const ws = this.ws;
    this.ws = null;
    if (ws !== null) {
      if (this.open) this.ctx.openCount--;
      this.open = false;
      ws.terminate();
    }
  }
}

interface WorkerContext {
  config: WorkerConfig;
  codec: Codec;
  stats: Stats;
  tickets: TicketPool;
  running: boolean;
  openCount: number;
  everConnected: number;
}

let ctx: WorkerContext | null = null;
let clients: LgClient[] = [];
let ticker: NodeJS.Timeout | null = null;
let connecting = 0;

function post(message: WorkerToMain): void {
  parentPort?.postMessage(message);
}

function report(reset: boolean): WorkerReport {
  if (ctx === null) throw new Error("not initialised");
  const s = ctx.stats;
  const mem = process.memoryUsage();
  const out: WorkerReport = {
    workerId: ctx.config.workerId,
    connected: ctx.openCount,
    everConnected: ctx.everConnected,
    connecting,
    latency: Object.fromEntries(Array.from(s.latency, ([k, h]) => [k, h.toJSON()])),
    ticketLatency: s.ticketLatency.toJSON(),
    connectLatency: s.connectLatency.toJSON(),
    received: s.received,
    ephReceived: s.ephReceived,
    published: s.published,
    publishErrors: s.publishErrors,
    gaps: s.gaps,
    duplicates: s.duplicates,
    resets: s.resets,
    resumed: s.resumed,
    reconnects: s.reconnects,
    connectErrors: s.connectErrors,
    ticketErrors: s.ticketErrors,
    closeCodes: { ...s.closeCodes },
    timeline: s.timeline,
    memory: { rss: mem.rss, heapUsed: mem.heapUsed },
  };
  if (reset) s.resetMeasurements();
  return out;
}

async function connectAll(): Promise<void> {
  if (ctx === null) return;
  const perTick = Math.max(1, Math.round((ctx.config.connectRatePerSec * 10) / 1000));
  connecting = clients.length;
  for (let i = 0; i < clients.length; i += perTick) {
    const batch = clients.slice(i, i + perTick);
    for (const c of batch) {
      void c.connect().finally(() => {
        connecting--;
      });
    }
    await new Promise((r) => setTimeout(r, 10));
  }
}

parentPort?.on("message", (message: MainToWorker) => {
  switch (message.cmd) {
    case "init": {
      ctx = {
        config: message.config,
        codec: codecByName(message.config.codec),
        stats: new Stats(),
        tickets: new TicketPool(message.config),
        running: false,
        openCount: 0,
        everConnected: 0,
      };
      const context = ctx;
      clients = message.clients.map((spec) => new LgClient(spec, context));
      ticker = setInterval(() => {
        const t = Date.now();
        for (const c of clients) c.tick(t);
      }, 10);
      post({ evt: "ready" });
      return;
    }
    case "connect":
      void connectAll();
      return;
    case "start":
      if (ctx !== null) ctx.running = true;
      return;
    case "stop":
      if (ctx !== null) ctx.running = false;
      return;
    case "report":
      post({ evt: "report", report: report(message.reset) });
      return;
    case "close":
      if (ticker !== null) clearInterval(ticker);
      for (const c of clients) c.stop();
      ctx?.tickets.close();
      post({ evt: "closed" });
      return;
  }
});
