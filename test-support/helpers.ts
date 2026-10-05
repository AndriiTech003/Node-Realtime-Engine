import { randomBytes } from "node:crypto";
import { createServer } from "node:net";
import WebSocket from "ws";
import { Redis } from "ioredis";
import {
  jsonCodec,
  msgpackCodec,
  type Codec,
  type ServerFrame,
} from "@ashamrai/realtime-protocol";
import { RealtimeServer, signJwt, type ConfigOverrides } from "@ashamrai/realtime-server";

export const JWT_SECRET = "test-jwt-secret";
export const SERVER_KEY = "test-server-key";

export function redisUrl(): string {
  return process.env["TEST_REDIS_URL"] ?? "redis://127.0.0.1:6379/3";
}

export function uniquePrefix(): string {
  const base = process.env["RT_TEST_PREFIX"] ?? "rt:test:local:";
  return `${base}${randomBytes(3).toString("hex")}:`;
}

const usedPorts = new Set<number>();

function tryListen(port: number): Promise<boolean> {
  return new Promise((resolve) => {
    const srv = createServer();
    srv.once("error", () => resolve(false));
    srv.listen(port, "127.0.0.1", () => srv.close(() => resolve(true)));
  });
}

export async function freePort(from = 4330, to = 4389): Promise<number> {
  for (let port = from; port <= to; port++) {
    if (usedPorts.has(port)) continue;
    if (await tryListen(port)) {
      usedPorts.add(port);
      return port;
    }
  }
  usedPorts.clear();
  for (let port = from; port <= to; port++) {
    if (await tryListen(port)) {
      usedPorts.add(port);
      return port;
    }
  }
  throw new Error("no free port in range");
}

export async function startNode(prefix: string, overrides: ConfigOverrides = {}): Promise<RealtimeServer> {
  const port = await freePort();
  const server = new RealtimeServer({
    config: {
      nodeId: overrides.nodeId ?? `test-${randomBytes(2).toString("hex")}`,
      host: "127.0.0.1",
      port,
      redisUrl: redisUrl(),
      redisPrefix: prefix,
      jwtSecret: JWT_SECRET,
      serverApiKey: SERVER_KEY,
      logLevel: "silent",
      allowedOrigins: ["http://allowed.test"],
      presenceRefreshMs: 1000,
      presenceSweepMs: 1000,
      nodeAliveRefreshMs: 500,
      nodeAliveTtlMs: 1500,
      drainMaxDelayMs: 500,
      drainCloseAfterMs: 3000,
      ...overrides,
    },
  });
  await server.start();
  return server;
}

export function baseUrl(server: RealtimeServer): string {
  return `http://127.0.0.1:${server.address().port}`;
}

export function tokenFor(uid: string, extra: Record<string, unknown> = {}, ttlSec = 3600): string {
  return signJwt({ sub: uid, name: uid, ...extra }, JWT_SECRET, ttlSec);
}

export async function getTicket(server: RealtimeServer, uid: string, extra: Record<string, unknown> = {}): Promise<string> {
  const res = await fetch(`${baseUrl(server)}/v1/tickets`, {
    method: "POST",
    headers: { authorization: `Bearer ${tokenFor(uid, extra)}` },
  });
  if (res.status !== 200) throw new Error(`ticket failed: ${res.status}`);
  const body = (await res.json()) as { ticket: string };
  return body.ticket;
}

export async function serverPublish(server: RealtimeServer, ch: string, d: unknown, cmid?: string): Promise<{ seq: number; mid: string; dup: boolean }> {
  const res = await fetch(`${baseUrl(server)}/v1/publish`, {
    method: "POST",
    headers: { "x-api-key": SERVER_KEY, "content-type": "application/json" },
    body: JSON.stringify(cmid === undefined ? { ch, d } : { ch, d, cmid }),
  });
  if (res.status !== 200) throw new Error(`publish failed: ${res.status} ${await res.text()}`);
  return (await res.json()) as { seq: number; mid: string; dup: boolean };
}

export interface ConnectOptions {
  codec?: Codec;
  origin?: string;
  extraClaims?: Record<string, unknown>;
}

export class TestClient {
  readonly frames: ServerFrame[] = [];
  closeCode: number | null = null;
  private waiters: { pred: (f: ServerFrame) => boolean; resolve: (f: ServerFrame) => void }[] = [];
  private closeWaiters: ((code: number) => void)[] = [];
  private nextId = 1;

  private constructor(
    readonly ws: WebSocket,
    readonly codec: Codec,
  ) {
    ws.on("message", (data: Buffer, isBinary: boolean) => {
      const frame = codec.decode(isBinary ? new Uint8Array(data) : data.toString("utf8")) as ServerFrame;
      this.frames.push(frame);
      this.waiters = this.waiters.filter((w) => {
        if (w.pred(frame)) {
          w.resolve(frame);
          return false;
        }
        return true;
      });
    });
    ws.on("close", (code: number) => {
      this.closeCode = code;
      for (const w of this.closeWaiters) w(code);
      this.closeWaiters = [];
    });
    ws.on("error", () => undefined);
  }

  static async connect(server: RealtimeServer, uid: string, options: ConnectOptions = {}): Promise<TestClient> {
    const codec = options.codec ?? jsonCodec;
    const ticket = await getTicket(server, uid, options.extraClaims);
    const ws = new WebSocket(`ws://127.0.0.1:${server.address().port}/v1/connect?ticket=${ticket}`, codec.subprotocol, {
      ...(options.origin === undefined ? {} : { origin: options.origin }),
    });
    const client = new TestClient(ws, codec);
    await new Promise<void>((resolve, reject) => {
      ws.once("open", () => resolve());
      ws.once("unexpected-response", (_req, res) => reject(new Error(`upgrade rejected ${res.statusCode}`)));
      ws.once("error", reject);
    });
    await Promise.race([client.waitFor((f) => f.t === "hello"), client.waitClose()]);
    return client;
  }

  get id(): number {
    return this.nextId;
  }

  send(frame: Record<string, unknown>): void {
    this.ws.send(this.codec.encode(frame));
  }

  request(frame: Record<string, unknown>): Promise<ServerFrame> {
    const id = this.nextId++;
    const response = this.waitFor((f) => (f.t === "ok" || f.t === "err") && "id" in f && f.id === id);
    this.send({ ...frame, id });
    return response;
  }

  sub(ch: string, extra: Record<string, unknown> = {}): Promise<ServerFrame> {
    return this.request({ t: "sub", ch, ...extra });
  }

  pub(ch: string, d: unknown, cmid: string = randomBytes(8).toString("hex")): Promise<ServerFrame> {
    return this.request({ t: "pub", ch, d, cmid });
  }

  waitFor(pred: (f: ServerFrame) => boolean, timeoutMs = 5000, fromIndex = 0): Promise<ServerFrame> {
    for (let i = fromIndex; i < this.frames.length; i++) {
      const frame = this.frames[i] as ServerFrame;
      if (pred(frame)) return Promise.resolve(frame);
    }
    return new Promise((resolve, reject) => {
      const waiter = { pred, resolve: (f: ServerFrame) => {
        clearTimeout(timer);
        resolve(f);
      } };
      const timer = setTimeout(() => {
        this.waiters = this.waiters.filter((w) => w !== waiter);
        reject(new Error(`timeout waiting for frame; got ${JSON.stringify(this.frames.slice(-5))}`));
      }, timeoutMs);
      this.waiters.push(waiter);
    });
  }

  messages(ch?: string): { seq: number; d: unknown; from: string }[] {
    const out: { seq: number; d: unknown; from: string }[] = [];
    for (const f of this.frames) if (f.t === "msg" && (ch === undefined || f.ch === ch)) out.push({ seq: f.seq, d: f.d, from: f.from });
    return out;
  }

  async waitForSeq(ch: string, seq: number, timeoutMs = 5000): Promise<void> {
    await this.waitFor((f) => f.t === "msg" && f.ch === ch && f.seq >= seq, timeoutMs);
  }

  waitClose(timeoutMs = 5000): Promise<number> {
    if (this.closeCode !== null) return Promise.resolve(this.closeCode);
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("timeout waiting for close")), timeoutMs);
      this.closeWaiters.push((code) => {
        clearTimeout(timer);
        resolve(code);
      });
    });
  }

  pauseReading(): void {
    const socket = (this.ws as unknown as { _socket: { pause(): void } })._socket;
    socket.pause();
  }

  resumeReading(): void {
    const socket = (this.ws as unknown as { _socket: { resume(): void } })._socket;
    socket.resume();
  }

  close(): void {
    this.ws.close();
  }

  terminate(): void {
    this.ws.terminate();
  }
}

export { msgpackCodec, jsonCodec };

export async function purgePrefix(prefix: string): Promise<void> {
  const redis = new Redis(redisUrl(), { lazyConnect: true });
  await redis.connect();
  let cursor = "0";
  do {
    const [next, keys] = await redis.scan(cursor, "MATCH", `${prefix}*`, "COUNT", 1000);
    cursor = next;
    if (keys.length > 0) await redis.unlink(...keys);
  } while (cursor !== "0");
  await redis.quit();
}

export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export async function eventually(check: () => boolean | Promise<boolean>, timeoutMs = 5000, stepMs = 25): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await check()) return;
    await sleep(stepMs);
  }
  if (!(await check())) throw new Error("condition not met in time");
}
