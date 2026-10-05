import { randomBytes, randomUUID } from "node:crypto";
import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import type { AddressInfo } from "node:net";
import type { Duplex } from "node:stream";
import { pino, type Logger } from "pino";
import { WebSocketServer, type RawData, type WebSocket } from "ws";
import {
  CloseCode,
  CodecError,
  ErrorCode,
  PROTOCOL_VERSION,
  codecForSubprotocol,
  int,
  json,
  obj,
  opt,
  parseChannel,
  selectSubprotocol,
  str,
  validate,
  validateClientFrame,
  type ChannelInfo,
  type ClientFrame,
  type EphFrame,
  type JsonValue,
  type PresFrame,
  type PubFrame,
  type SubFrame,
  type UnsubFrame,
} from "@ashamrai/realtime-protocol";
import { bearer, safeEqual, userFromClaims, verifyJwt, type AuthUser, type UserClaims } from "./auth.js";
import { ChannelRegistry } from "./channels.js";
import { resolveConfig, type ConfigOverrides, type ServerConfig } from "./config.js";
import { Connection, type Subscription } from "./connection.js";
import { HttpError, corsHeaders, readJson, rejectUpgrade, sendJson } from "./http-util.js";
import { Keys } from "./keys.js";
import { Metrics } from "./metrics.js";
import { durableFrameObject } from "./outbound.js";
import { RedisStore, type PresenceEntry } from "./redis.js";
import { checkPage, mergeLive, planResume } from "./resume.js";
import { UserConnectionLimiter } from "./user-limit.js";

export type ChannelAction = "subscribe" | "publish" | "ephemeral" | "presence";

export type Authorizer = (user: AuthUser, action: ChannelAction, channel: ChannelInfo) => boolean;

export const defaultAuthorizer: Authorizer = (user, action, channel) => {
  switch (channel.type) {
    case "room":
      return true;
    case "user":
      return action === "subscribe" && channel.id === user.id;
    case "broadcast":
      return action === "subscribe";
  }
};

export interface RealtimeServerOptions {
  config?: ConfigOverrides;
  authorize?: Authorizer;
  logger?: Logger;
}

interface TicketPayload {
  claims: UserClaims;
}

const publishBodySchema = obj({
  ch: str({ min: 1, max: 128 }),
  d: json(),
  cmid: opt(str({ min: 1, max: 64, pattern: /^[A-Za-z0-9_-]+$/ })),
});

const debugBodySchema = obj({ cid: str({ min: 1 }), enabled: opt(json()) });

const simulateSlowSchema = obj({ cid: str({ min: 1 }), ms: int({ min: 1, max: 60000 }) });

export class RealtimeServer {
  readonly config: ServerConfig;
  readonly metrics: Metrics;
  readonly keys: Keys;
  readonly store: RedisStore;
  readonly channels: ChannelRegistry;
  readonly log: Logger;
  readonly bootId = randomBytes(4).toString("hex");
  private readonly http: Server;
  private readonly wss: WebSocketServer;
  private readonly authorize: Authorizer;
  private readonly connections = new Map<string, Connection>();
  readonly userLimiter: UserConnectionLimiter;
  private readonly lagging = new Set<Connection>();
  private readonly ephDirty = new Set<Connection>();
  private readonly allowedOrigins: Set<string>;
  private readonly timers: NodeJS.Timeout[] = [];
  private connCounter = 0;
  private ready = false;
  private draining = false;
  private stopped = false;

  constructor(options: RealtimeServerOptions = {}) {
    this.config = resolveConfig(options.config);
    this.authorize = options.authorize ?? defaultAuthorizer;
    this.log = options.logger ?? pino({ level: this.config.logLevel, base: { node: this.config.nodeId } });
    this.keys = new Keys(this.config.redisPrefix);
    this.metrics = new Metrics(() => this.stats(), { node: this.config.nodeId });
    this.store = new RedisStore(this.config.redisUrl, this.keys, this.metrics);
    this.channels = new ChannelRegistry(this.store, this.keys, this.metrics, {
      serializeOnce: this.config.fanoutSerializeOnce,
      chunkThreshold: this.config.fanoutChunkThreshold,
      chunkSize: this.config.fanoutChunkSize,
    });
    this.userLimiter = new UserConnectionLimiter(this.store, this.config.nodeId, this.config.maxConnectionsPerUser, this.log);
    this.allowedOrigins = new Set(this.config.allowedOrigins);
    this.http = createServer((req, res) => {
      this.handleHttp(req, res).catch((error: unknown) => this.httpFailure(res, error));
    });
    this.http.keepAliveTimeout = 5000;
    this.wss = new WebSocketServer({
      noServer: true,
      maxPayload: this.config.maxPayload,
      perMessageDeflate: this.config.perMessageDeflate,
      clientTracking: false,
      handleProtocols: (protocols) => selectSubprotocol(protocols) ?? false,
    });
    this.http.on("upgrade", (req: IncomingMessage, socket: Duplex, head: Buffer) => {
      this.handleUpgrade(req, socket, head).catch((error: unknown) => {
        this.log.error({ err: error }, "upgrade failed");
        rejectUpgrade(socket, 500, "Internal Server Error");
      });
    });
  }

  get nodeId(): string {
    return this.config.nodeId;
  }

  get isReady(): boolean {
    return this.ready && !this.draining;
  }

  get connectionCount(): number {
    return this.connections.size;
  }

  address(): AddressInfo {
    return this.http.address() as AddressInfo;
  }

  connection(cid: string): Connection | undefined {
    return this.connections.get(cid);
  }

  allConnections(): IterableIterator<Connection> {
    return this.connections.values();
  }

  async start(): Promise<void> {
    await this.store.connect();
    await this.store.markAlive(this.nodeId, this.config.nodeAliveTtlMs);
    await this.cleanupPreviousIncarnation();
    this.metrics.startGcObserver();
    await new Promise<void>((resolve, reject) => {
      this.http.once("error", reject);
      this.http.listen(this.config.port, this.config.host, () => {
        this.http.off("error", reject);
        resolve();
      });
    });
    this.startTimers();
    this.ready = true;
    this.log.info({ port: this.address().port }, "realtime node started");
  }

  private async cleanupPreviousIncarnation(): Promise<void> {
    await this.store.clearNodeUsers(this.nodeId);
    const members = await this.store.cmd.smembers(this.keys.nodePresence(this.nodeId));
    for (const member of members) {
      const sep = member.indexOf("\n");
      if (sep < 0) continue;
      await this.store.presenceLeave(member.slice(0, sep), member.slice(sep + 1), this.nodeId);
    }
  }

  private every(ms: number, fn: () => void | Promise<void>): void {
    const timer = setInterval(() => {
      Promise.resolve()
        .then(fn)
        .catch((error: unknown) => this.log.warn({ err: error }, "periodic task failed"));
    }, ms);
    timer.unref();
    this.timers.push(timer);
  }

  private startTimers(): void {
    if (this.config.heartbeatMode === "shared") {
      const tick = Math.max(50, Math.min(5000, Math.floor(this.config.heartbeatTimeoutMs / 2)));
      this.every(tick, () => this.heartbeatTick());
    }
    this.every(this.config.backpressure.drainTickMs, () => this.flushLagging());
    this.every(this.config.ephemeralCoalesceMs, () => this.flushEphemeral());
    this.every(this.config.nodeAliveRefreshMs, () => this.refreshAlive());
    this.every(this.config.presenceRefreshMs, () => this.refreshPresence());
    this.every(this.config.presenceSweepMs, () => this.sweepPresence());
    this.every(this.config.bufferedSampleMs, () => this.sampleBuffered());
  }

  private async refreshAlive(): Promise<void> {
    const rejoined = await this.store.markAlive(this.nodeId, this.config.nodeAliveTtlMs);
    if (rejoined && this.ready) {
      this.log.warn("node was swept as dead while alive; restoring presence and user connection counts");
      await this.rejoinPresence();
    }
    if (rejoined || this.userLimiter.needsResync) await this.userLimiter.resync();
  }

  private async rejoinPresence(): Promise<void> {
    const joins: Promise<number>[] = [];
    const expiresAt = Date.now() + this.config.presenceTtlMs;
    for (const conn of this.connections.values()) {
      if (conn.state === "closing") continue;
      for (const sub of conn.subs.values()) {
        if (!sub.joined) continue;
        const meta = sub.meta ?? this.initialMeta(conn.user);
        const entry = JSON.stringify({ uid: conn.user.id, meta, node: this.nodeId, at: Date.now() });
        const joinFrame = JSON.stringify({ t: "pj", ch: sub.ch, uid: conn.user.id, meta });
        joins.push(this.store.presenceJoin(sub.ch, conn.id, conn.user.id, entry, expiresAt, joinFrame, this.nodeId));
      }
    }
    await Promise.all(joins);
  }

  private stopTimers(): void {
    for (const timer of this.timers) clearInterval(timer);
    this.timers.length = 0;
  }

  private stats() {
    let active = 0;
    let lagging = 0;
    let subscriptions = 0;
    let pending = 0;
    for (const conn of this.connections.values()) {
      if (conn.state === "lagging") lagging++;
      else if (conn.state === "active") active++;
      subscriptions += conn.subs.size;
      pending += conn.pendingCount;
    }
    return { active, lagging, subscriptions, channels: this.channels.size, pending };
  }

  private heartbeatTick(): void {
    const now = Date.now();
    const { heartbeatIntervalMs, heartbeatTimeoutMs } = this.config;
    for (const conn of this.connections.values()) {
      if (conn.state === "closing") continue;
      if (conn.user.expiresAt !== null && conn.user.expiresAt < now) {
        conn.close(CloseCode.InvalidTicket, "session expired");
        continue;
      }
      if (conn.pingSentAt > 0) {
        if (now - conn.pingSentAt > heartbeatTimeoutMs) {
          this.metrics.heartbeatTerminations.inc();
          conn.log.debug("pong timeout, terminating");
          conn.terminate();
        }
        continue;
      }
      if (now - conn.lastPingAt >= heartbeatIntervalMs) {
        conn.pingSentAt = now;
        conn.ws.ping();
      }
    }
  }

  private startPerConnectionHeartbeat(conn: Connection): void {
    const { heartbeatIntervalMs, heartbeatTimeoutMs } = this.config;
    const interval = setInterval(() => {
      if (conn.state === "closing") return;
      conn.pingSentAt = Date.now();
      conn.ws.ping();
      const timeout = setTimeout(() => {
        if (conn.pingSentAt > 0) {
          this.metrics.heartbeatTerminations.inc();
          conn.terminate();
        }
      }, heartbeatTimeoutMs);
      conn.timers.push(timeout);
      if (conn.timers.length > 8) conn.timers.splice(1, conn.timers.length - 8);
    }, heartbeatIntervalMs);
    conn.timers.unshift(interval);
  }

  private flushLagging(): void {
    if (this.lagging.size === 0) return;
    const now = Date.now();
    for (const conn of this.lagging) {
      const result = conn.flushPending(now);
      if (result !== "lagging") this.lagging.delete(conn);
    }
  }

  private flushEphemeral(): void {
    if (this.ephDirty.size === 0) return;
    const messages: [string, string][] = [];
    for (const conn of this.ephDirty) {
      for (const [ch, d] of conn.ephPending) {
        const frame = JSON.stringify({ t: "eph", ch, d, from: conn.user.id });
        messages.push([this.keys.fan(ch), `e|${conn.id}|${frame}`]);
      }
      conn.ephPending.clear();
    }
    this.ephDirty.clear();
    this.store.spublishMany(messages).catch((error: unknown) => this.log.warn({ err: error }, "ephemeral publish failed"));
  }

  private async refreshPresence(): Promise<void> {
    const byChannel = new Map<string, string[]>();
    for (const conn of this.connections.values()) {
      for (const sub of conn.subs.values()) {
        if (!sub.joined) continue;
        let list = byChannel.get(sub.ch);
        if (list === undefined) {
          list = [];
          byChannel.set(sub.ch, list);
        }
        list.push(conn.id);
      }
    }
    await this.store.refreshPresence(byChannel, Date.now() + this.config.presenceTtlMs);
  }

  private async sweepPresence(): Promise<void> {
    const token = `${this.nodeId}:${this.bootId}`;
    const lock = this.keys.sweepLock();
    const acquired = await this.store.acquireLock(lock, token, Math.max(1000, Math.floor(this.config.presenceSweepMs * 0.9)));
    if (!acquired) return;
    try {
      const result = await this.store.sweepPresence(Date.now(), this.nodeId, this.nodeId);
      if (result.removed > 0 || result.deadNodes.length > 0) {
        this.log.info({ removed: result.removed, deadNodes: result.deadNodes }, "presence sweep");
      }
    } finally {
      await this.store.releaseLock(lock, token);
    }
  }

  private sampleBuffered(): void {
    for (const conn of this.connections.values()) {
      this.metrics.wsBufferedBytes.observe(conn.ws.bufferedAmount);
    }
  }

  private async handleHttp(req: IncomingMessage, res: ServerResponse): Promise<void> {
    const url = new URL(req.url ?? "/", "http://localhost");
    const origin = req.headers.origin;
    const cors = corsHeaders(origin, this.allowedOrigins);
    const route = `${req.method ?? "GET"} ${url.pathname}`;
    if (req.method === "OPTIONS") {
      res.writeHead(204, cors);
      res.end();
      return;
    }
    switch (route) {
      case "GET /health/live":
        sendJson(res, 200, { status: "ok", node: this.nodeId });
        return;
      case "GET /health/ready": {
        const redisOk = this.store.cmd.status === "ready" && this.store.sub.status === "ready";
        const ok = this.isReady && redisOk;
        sendJson(res, ok ? 200 : 503, {
          status: ok ? "ready" : "not_ready",
          node: this.nodeId,
          draining: this.draining,
          redis: redisOk,
          connections: this.connections.size,
        });
        return;
      }
      case "GET /metrics": {
        const body = await this.metrics.registry.metrics();
        res.writeHead(200, { "content-type": this.metrics.registry.contentType });
        res.end(body);
        return;
      }
      case "POST /v1/tickets":
        await this.httpCreateTicket(req, res, cors);
        return;
      case "POST /v1/publish":
        await this.httpPublish(req, res);
        return;
      case "GET /v1/history":
        await this.httpHistory(req, res, url, cors);
        return;
      case "POST /admin/debug":
        await this.httpDebug(req, res);
        return;
      case "POST /admin/gc": {
        this.requireServerKey(req);
        const gc = (globalThis as { gc?: () => void }).gc;
        if (gc !== undefined) gc();
        const mem = process.memoryUsage();
        sendJson(res, 200, { gc: gc !== undefined, heapUsed: mem.heapUsed, rss: mem.rss, connections: this.connections.size });
        return;
      }
      case "POST /admin/simulate-slow":
        await this.httpSimulateSlow(req, res);
        return;
      case "GET /admin/connections":
        this.requireServerKey(req);
        sendJson(res, 200, {
          node: this.nodeId,
          connections: Array.from(this.connections.values()).map((c) => ({
            cid: c.id,
            uid: c.user.id,
            state: c.state,
            subs: Array.from(c.subs.keys()),
            buffered: c.ws.bufferedAmount,
            pending: c.pendingCount,
            debug: c.debug,
          })),
        });
        return;
      default:
        sendJson(res, 404, { error: "not found" }, cors);
    }
  }

  private httpFailure(res: ServerResponse, error: unknown): void {
    if (res.headersSent) {
      res.destroy();
      return;
    }
    if (error instanceof HttpError) {
      sendJson(res, error.status, { error: error.code, message: error.message });
      return;
    }
    this.log.error({ err: error }, "http handler failed");
    sendJson(res, 500, { error: "INTERNAL" });
  }

  private authenticateJwt(req: IncomingMessage): UserClaims {
    const token = bearer(req.headers.authorization);
    if (token === null) throw new HttpError(401, "UNAUTHORIZED", "missing bearer token");
    const result = verifyJwt(token, this.config.jwtSecret);
    if (!result.ok) throw new HttpError(401, "UNAUTHORIZED", result.reason);
    return result.claims;
  }

  private requireServerKey(req: IncomingMessage): void {
    const header = req.headers["x-api-key"];
    const key = typeof header === "string" ? header : bearer(req.headers.authorization);
    if (key === null || !safeEqual(key, this.config.serverApiKey)) {
      throw new HttpError(401, "UNAUTHORIZED", "invalid server key");
    }
  }

  private async httpCreateTicket(req: IncomingMessage, res: ServerResponse, cors: Record<string, string>): Promise<void> {
    let claims: UserClaims;
    try {
      claims = this.authenticateJwt(req);
    } catch (error) {
      if (error instanceof HttpError) {
        sendJson(res, error.status, { error: error.code, message: error.message }, cors);
        return;
      }
      throw error;
    }
    const payload: TicketPayload = { claims };
    const ticket = await this.store.createTicket(JSON.stringify(payload), this.config.ticketTtlSec);
    sendJson(res, 200, { ticket, expiresIn: this.config.ticketTtlSec }, cors);
  }

  private async httpPublish(req: IncomingMessage, res: ServerResponse): Promise<void> {
    this.requireServerKey(req);
    const body = await readJson(req, this.config.maxPayload);
    const parsed = validate(publishBodySchema, body);
    if (!parsed.ok) throw new HttpError(400, "BAD_REQUEST", parsed.error);
    const channel = parseChannel(parsed.value.ch);
    if (!channel.ok) {
      throw new HttpError(400, channel.reason === "unknown_type" ? "UNKNOWN_CHANNEL_TYPE" : "BAD_REQUEST", "bad channel");
    }
    const result = await this.publishDurable(channel.channel.name, parsed.value.d, parsed.value.cmid ?? randomUUID(), "server");
    sendJson(res, 200, result);
  }

  private async httpHistory(req: IncomingMessage, res: ServerResponse, url: URL, cors: Record<string, string>): Promise<void> {
    let claims: UserClaims;
    try {
      claims = this.authenticateJwt(req);
    } catch (error) {
      if (error instanceof HttpError) {
        sendJson(res, error.status, { error: error.code, message: error.message }, cors);
        return;
      }
      throw error;
    }
    const ch = url.searchParams.get("ch") ?? "";
    const limit = Math.min(1000, Math.max(1, Number(url.searchParams.get("limit") ?? "50") || 50));
    const channel = parseChannel(ch);
    if (!channel.ok) {
      sendJson(res, 400, { error: "BAD_REQUEST" }, cors);
      return;
    }
    if (!this.authorize(userFromClaims(claims), "subscribe", channel.channel)) {
      sendJson(res, 403, { error: "FORBIDDEN" }, cors);
      return;
    }
    const head = await this.store.headSeq(ch);
    const from = Math.max(1, head - limit + 1);
    const entries = head === 0 ? [] : await this.store.readRange(ch, from, head, limit);
    const messages = entries.map((e) => durableFrameObject(ch, e.seq, e.ts, e.payload));
    sendJson(res, 200, { ch, seq: head, messages }, cors);
  }

  private async httpDebug(req: IncomingMessage, res: ServerResponse): Promise<void> {
    this.requireServerKey(req);
    const body = await readJson(req, 4096);
    const parsed = validate(debugBodySchema, body);
    if (!parsed.ok) throw new HttpError(400, "BAD_REQUEST", parsed.error);
    const conn = this.connections.get(parsed.value.cid);
    if (conn === undefined) {
      sendJson(res, 404, { error: "NOT_FOUND" });
      return;
    }
    conn.setDebug(parsed.value.enabled !== false);
    conn.log.info({ debug: conn.debug }, "debug logging toggled");
    sendJson(res, 200, { cid: conn.id, debug: conn.debug });
  }

  private async httpSimulateSlow(req: IncomingMessage, res: ServerResponse): Promise<void> {
    this.requireServerKey(req);
    const body = await readJson(req, 4096);
    const parsed = validate(simulateSlowSchema, body);
    if (!parsed.ok) throw new HttpError(400, "BAD_REQUEST", parsed.error);
    const conn = this.connections.get(parsed.value.cid);
    if (conn === undefined) {
      sendJson(res, 404, { error: "NOT_FOUND" });
      return;
    }
    const socket = (conn.ws as unknown as { _socket?: { cork(): void; uncork(): void; destroyed: boolean } })._socket;
    if (socket === undefined) {
      sendJson(res, 409, { error: "NO_SOCKET" });
      return;
    }
    socket.cork();
    conn.log.info({ ms: parsed.value.ms }, "simulating slow client");
    setTimeout(() => {
      if (!socket.destroyed) socket.uncork();
    }, parsed.value.ms).unref();
    sendJson(res, 200, { cid: conn.id, ms: parsed.value.ms });
  }

  private async handleUpgrade(req: IncomingMessage, socket: Duplex, head: Buffer): Promise<void> {
    socket.on("error", () => socket.destroy());
    const url = new URL(req.url ?? "/", "http://localhost");
    if (url.pathname !== "/v1/connect") {
      this.metrics.upgrades.inc({ result: "not_found" });
      rejectUpgrade(socket, 404, "Not Found");
      return;
    }
    if (!this.isReady || this.stopped) {
      this.metrics.upgrades.inc({ result: "draining" });
      rejectUpgrade(socket, 503, "Service Unavailable");
      return;
    }
    const origin = req.headers.origin;
    if (origin === undefined ? !this.config.allowNoOrigin : !this.allowedOrigins.has(origin)) {
      this.metrics.upgrades.inc({ result: "bad_origin" });
      rejectUpgrade(socket, 403, "Forbidden");
      return;
    }
    if (this.connections.size >= this.config.maxConnections) {
      this.metrics.upgrades.inc({ result: "shed" });
      rejectUpgrade(socket, 503, "Service Unavailable");
      return;
    }
    const ticket = url.searchParams.get("ticket");
    if (ticket === null || ticket.length === 0 || ticket.length > 128) {
      this.metrics.upgrades.inc({ result: "unauthorized" });
      rejectUpgrade(socket, 401, "Unauthorized");
      return;
    }
    const raw = await this.store.consumeTicket(ticket);
    if (raw === null) {
      this.metrics.upgrades.inc({ result: "unauthorized" });
      rejectUpgrade(socket, 401, "Unauthorized");
      return;
    }
    if (socket.destroyed) {
      this.metrics.upgrades.inc({ result: "aborted" });
      return;
    }
    const payload = JSON.parse(raw) as TicketPayload;
    const user = userFromClaims(payload.claims);
    const slot = await this.userLimiter.acquire(user.id);
    if (socket.destroyed || this.stopped) {
      if (slot) this.userLimiter.release(user.id);
      this.metrics.upgrades.inc({ result: "aborted" });
      if (!socket.destroyed) rejectUpgrade(socket, 503, "Service Unavailable");
      return;
    }
    let handedOver = false;
    socket.once("close", () => {
      if (!handedOver && slot) this.userLimiter.release(user.id);
    });
    this.wss.handleUpgrade(req, socket, head, (ws) => {
      handedOver = true;
      this.onConnection(ws, user, slot);
    });
  }

  private onConnection(ws: WebSocket, user: AuthUser, slot: boolean): void {
    this.connCounter++;
    const cid = `${this.nodeId}.${this.bootId}.${this.connCounter.toString(36)}`;
    const codec = codecForSubprotocol(ws.protocol);
    const conn = new Connection(cid, ws, user, codec, this.log, this.metrics, this.config.backpressure, this.config.rate, {
      onLagging: (c) => this.lagging.add(c),
      onSlowClose: (c, reason) => c.log.info({ reason, buffered: c.ws.bufferedAmount }, "slow consumer disconnected"),
    });
    conn.holdsUserSlot = slot;
    this.connections.set(cid, conn);
    ws.on("close", () => this.onClose(conn));
    ws.on("error", (error) => conn.log.debug({ err: error }, "socket error"));
    ws.on("pong", () => {
      conn.pingSentAt = 0;
      conn.lastPingAt = Date.now();
    });
    if (!slot) {
      this.metrics.upgrades.inc({ result: "user_limit" });
      conn.close(CloseCode.RateLimited, "too many connections for user");
      return;
    }
    this.metrics.upgrades.inc({ result: "ok" });
    ws.on("message", (data, isBinary) => this.onMessage(conn, data, isBinary));
    if (this.config.heartbeatMode === "per-connection") this.startPerConnectionHeartbeat(conn);
    conn.sendControl({ t: "hello", cid, node: this.nodeId, hb: this.config.heartbeatIntervalMs, v: PROTOCOL_VERSION });
    if (this.draining) conn.sendControl({ t: "drain", after: Math.floor(Math.random() * this.config.drainMaxDelayMs) });
    conn.log.debug("connected");
  }

  private onClose(conn: Connection): void {
    if (!this.connections.delete(conn.id)) return;
    conn.state = "closing";
    conn.clearTimers();
    this.lagging.delete(conn);
    this.ephDirty.delete(conn);
    if (conn.holdsUserSlot) {
      conn.holdsUserSlot = false;
      if (!this.stopped) this.userLimiter.release(conn.user.id);
    }
    for (const sub of conn.subs.values()) {
      this.channels.remove(sub.ch, conn);
      if (sub.joined && !this.stopped) {
        this.store.presenceLeave(sub.ch, conn.id, this.nodeId).catch(() => undefined);
      }
    }
    conn.subs.clear();
    conn.log.debug("closed");
  }

  private onMessage(conn: Connection, data: RawData, isBinary: boolean): void {
    if (conn.state === "closing") return;
    let decoded: unknown;
    try {
      const bytes = Array.isArray(data) ? Buffer.concat(data) : Buffer.isBuffer(data) ? data : Buffer.from(data);
      decoded = conn.codec.decode(isBinary || conn.codec.binary ? bytes : bytes.toString("utf8"));
    } catch (error) {
      if (error instanceof CodecError) {
        conn.close(CloseCode.ProtocolViolation, "undecodable frame");
        return;
      }
      throw error;
    }
    const result = validateClientFrame(decoded);
    if (!result.ok) {
      const t = typeof decoded === "object" && decoded !== null ? (decoded as Record<string, unknown>)["t"] : undefined;
      const known = typeof t === "string" && ["sub", "unsub", "pub", "eph", "pres", "ping"].includes(t);
      if (!known) {
        conn.close(CloseCode.ProtocolViolation, "unknown frame type");
        return;
      }
      const id = (decoded as Record<string, unknown>)["id"];
      conn.sendControl({ t: "err", ...(typeof id === "number" ? { id } : {}), code: ErrorCode.BadRequest, msg: result.error });
      return;
    }
    const frame = result.value;
    this.metrics.messagesIn.inc({ t: frame.t });
    if (conn.debug) conn.log.debug({ t: frame.t, ch: "ch" in frame ? frame.ch : undefined }, "frame in");
    this.dispatch(conn, frame);
  }

  private rateLimited(conn: Connection, bucket: string, id: number | undefined): void {
    this.metrics.rateLimited.inc({ bucket });
    conn.sendControl({ t: "err", ...(id === undefined ? {} : { id }), code: ErrorCode.RateLimited, msg: `${bucket} rate exceeded` });
    if (conn.violations.hit()) conn.close(CloseCode.RateLimited, "rate limit exceeded");
  }

  private dispatch(conn: Connection, frame: ClientFrame): void {
    switch (frame.t) {
      case "ping":
        if (!conn.ctl.take()) return this.rateLimited(conn, "ctl", undefined);
        conn.sendControl({ t: "pong", ts: frame.ts });
        return;
      case "sub":
        if (!conn.ctl.take()) return this.rateLimited(conn, "ctl", frame.id);
        this.handleSub(conn, frame).catch((error: unknown) => this.requestFailed(conn, frame.id, error));
        return;
      case "unsub":
        if (!conn.ctl.take()) return this.rateLimited(conn, "ctl", frame.id);
        this.handleUnsub(conn, frame);
        return;
      case "pub":
        if (!conn.pub.take()) return this.rateLimited(conn, "pub", frame.id);
        this.handlePub(conn, frame).catch((error: unknown) => this.requestFailed(conn, frame.id, error));
        return;
      case "eph":
        if (!conn.eph.take()) return this.rateLimited(conn, "eph", undefined);
        this.handleEph(conn, frame);
        return;
      case "pres":
        if (!conn.ctl.take()) return this.rateLimited(conn, "ctl", frame.id);
        this.handlePres(conn, frame).catch((error: unknown) => this.requestFailed(conn, frame.id, error));
        return;
    }
  }

  private requestFailed(conn: Connection, id: number, error: unknown): void {
    conn.log.error({ err: error }, "request failed");
    conn.sendControl({ t: "err", id, code: ErrorCode.Internal, msg: "internal error" });
  }

  private sendError(conn: Connection, id: number | undefined, code: ErrorCode, msg: string): void {
    conn.sendControl({ t: "err", ...(id === undefined ? {} : { id }), code, msg });
  }

  private resolveChannel(conn: Connection, id: number | undefined, ch: string): ChannelInfo | null {
    const parsed = parseChannel(ch);
    if (!parsed.ok) {
      this.sendError(
        conn,
        id,
        parsed.reason === "unknown_type" ? ErrorCode.UnknownChannelType : ErrorCode.BadRequest,
        parsed.reason === "unknown_type" ? "unknown channel type" : "invalid channel name",
      );
      return null;
    }
    return parsed.channel;
  }

  private async handleSub(conn: Connection, frame: SubFrame): Promise<void> {
    const info = this.resolveChannel(conn, frame.id, frame.ch);
    if (info === null) return;
    if (!this.authorize(conn.user, "subscribe", info)) {
      this.sendError(conn, frame.id, ErrorCode.Forbidden, "not allowed to subscribe");
      return;
    }
    const existing = conn.subs.get(info.name);
    if (existing === undefined && conn.subs.size >= this.config.maxSubscriptions) {
      this.sendError(conn, frame.id, ErrorCode.TooManySubscriptions, `limit is ${this.config.maxSubscriptions}`);
      return;
    }
    const sub: Subscription = {
      ch: info.name,
      info,
      state: "syncing",
      buffer: [],
      lastSeq: 0,
      presence: info.presence && frame.presence !== false,
      joined: existing?.joined ?? false,
      generation: conn.nextGeneration(),
      ...(existing?.meta === undefined ? {} : { meta: existing.meta }),
    };
    conn.subs.set(info.name, sub);
    const alive = () => conn.state !== "closing" && conn.subs.get(info.name) === sub;
    await this.channels.add(info.name, conn);
    if (!alive()) return;
    const head = await this.store.headSeq(info.name);
    if (!alive()) return;
    const plan = planResume(frame.from, frame.history, head, this.config.resumeLimit);
    let lastSent = head;
    let replayed = 0;
    if (plan.kind === "reset") {
      this.metrics.resets.inc({ reason: plan.reason });
      conn.sendControl({ t: "reset", ch: info.name, seq: plan.seq });
    } else if (plan.kind === "replay") {
      let next = plan.from + 1;
      let resetSent = false;
      while (next <= plan.to) {
        const page = await this.store.readRange(info.name, next, plan.to, this.config.historyPageSize);
        if (!alive()) return;
        const check = checkPage(next, page, plan.to);
        if (!check.ok) {
          if (plan.strict) {
            this.metrics.resets.inc({ reason: "trimmed" });
            conn.sendControl({ t: "reset", ch: info.name, seq: head });
            resetSent = true;
            break;
          }
          if (page.length === 0) break;
        }
        for (const entry of page) {
          const bytes = conn.encode(durableFrameObject(info.name, entry.seq, entry.ts, entry.payload));
          conn.sendBytes(bytes, "history");
          replayed++;
        }
        const last = page[page.length - 1];
        if (last === undefined) break;
        next = last.seq + 1;
      }
      if (resetSent) replayed = 0;
      this.metrics.resumeMessages.observe(replayed);
      lastSent = head;
    }
    let presence: { total: number; entries: PresenceEntry[] } | null = null;
    if (sub.presence) {
      if (!sub.joined) {
        const meta = this.initialMeta(conn.user);
        const entry = JSON.stringify({ uid: conn.user.id, meta, node: this.nodeId, at: Date.now() });
        const joinFrame = JSON.stringify({ t: "pj", ch: info.name, uid: conn.user.id, meta });
        sub.meta = meta;
        await this.store.presenceJoin(
          info.name,
          conn.id,
          conn.user.id,
          entry,
          Date.now() + this.config.presenceTtlMs,
          joinFrame,
          this.nodeId,
        );
        sub.joined = true;
        if (!alive()) {
          if (conn.subs.get(info.name) === undefined || conn.state === "closing") {
            await this.store.presenceLeave(info.name, conn.id, this.nodeId);
          }
          return;
        }
      }
      presence = await this.store.presenceList(info.name, this.config.presenceListLimit);
      if (!alive()) return;
    } else if (sub.joined) {
      await this.store.presenceLeave(info.name, conn.id, this.nodeId);
      sub.joined = false;
    }
    const ok: Record<string, unknown> = { t: "ok", id: frame.id, seq: lastSent };
    if (presence !== null) {
      ok["presence"] = presence.entries.map((e) => (e.meta === undefined ? { uid: e.uid } : { uid: e.uid, meta: e.meta }));
      ok["pn"] = presence.total;
    }
    conn.sendControl(ok);
    const merged = mergeLive(lastSent, sub.buffer);
    sub.buffer = [];
    sub.lastSeq = merged.lastSeq;
    sub.state = "live";
    for (const item of merged.items) {
      conn.sendFrame(item.frame, item.kind === "durable" ? "durable" : "presence", item.ch);
    }
    if (conn.debug) conn.log.debug({ ch: info.name, from: frame.from, head, replayed }, "subscribed");
  }

  private initialMeta(user: AuthUser): JsonValue {
    const meta = user.claims["meta"];
    if (typeof meta === "object" && meta !== null && !Array.isArray(meta)) return { name: user.name, ...(meta as Record<string, JsonValue>) };
    return { name: user.name };
  }

  private handleUnsub(conn: Connection, frame: UnsubFrame): void {
    const sub = conn.subs.get(frame.ch);
    if (sub === undefined) {
      this.sendError(conn, frame.id, ErrorCode.NotSubscribed, "not subscribed");
      return;
    }
    conn.subs.delete(frame.ch);
    conn.ephPending.delete(frame.ch);
    this.channels.remove(frame.ch, conn);
    if (sub.joined) this.store.presenceLeave(frame.ch, conn.id, this.nodeId).catch(() => undefined);
    conn.sendControl({ t: "ok", id: frame.id });
  }

  private async handlePub(conn: Connection, frame: PubFrame): Promise<void> {
    const info = this.resolveChannel(conn, frame.id, frame.ch);
    if (info === null) return;
    if (!info.clientPublish || !this.authorize(conn.user, "publish", info)) {
      this.sendError(conn, frame.id, ErrorCode.Forbidden, "not allowed to publish");
      return;
    }
    const result = await this.publishDurable(info.name, frame.d, frame.cmid, conn.user.id);
    if (conn.debug) conn.log.debug({ ch: info.name, seq: result.seq, dup: result.dup }, "published");
    conn.sendControl(result.dup ? { t: "ok", id: frame.id, seq: result.seq, mid: result.mid, dup: true } : { t: "ok", id: frame.id, seq: result.seq, mid: result.mid });
  }

  async publishDurable(ch: string, d: JsonValue, cmid: string, from: string): Promise<{ seq: number; mid: string; dup: boolean }> {
    const mid = randomUUID();
    const payload = JSON.stringify({ mid, from, d });
    const result = await this.store.publish({
      ch,
      payload,
      ts: Date.now(),
      cmid,
      mid,
      maxLen: this.config.historyMaxLen,
      retentionCutoff: Date.now() - this.config.historyRetentionMs,
      cmidTtlSec: this.config.cmidTtlSec,
    });
    this.metrics.publishes.inc({ result: result.dup ? "duplicate" : "ok" });
    return result;
  }

  private handleEph(conn: Connection, frame: EphFrame): void {
    const parsed = parseChannel(frame.ch);
    if (!parsed.ok) {
      this.sendError(conn, undefined, parsed.reason === "unknown_type" ? ErrorCode.UnknownChannelType : ErrorCode.BadRequest, "bad channel");
      return;
    }
    if (!parsed.channel.ephemeral || !this.authorize(conn.user, "ephemeral", parsed.channel)) {
      this.sendError(conn, undefined, ErrorCode.Forbidden, "ephemeral not allowed");
      return;
    }
    if (!conn.subs.has(frame.ch)) {
      this.sendError(conn, undefined, ErrorCode.NotSubscribed, "subscribe before sending ephemeral");
      return;
    }
    conn.ephPending.set(frame.ch, frame.d);
    this.ephDirty.add(conn);
  }

  private async handlePres(conn: Connection, frame: PresFrame): Promise<void> {
    const sub = conn.subs.get(frame.ch);
    if (sub === undefined) {
      this.sendError(conn, frame.id, ErrorCode.NotSubscribed, "not subscribed");
      return;
    }
    if (!sub.info.presence || !this.authorize(conn.user, "presence", sub.info)) {
      this.sendError(conn, frame.id, ErrorCode.Forbidden, "presence not available on this channel");
      return;
    }
    if (!sub.joined) {
      this.sendError(conn, frame.id, ErrorCode.BadRequest, "presence not joined");
      return;
    }
    const meta = frame.meta === undefined ? this.initialMeta(conn.user) : frame.meta;
    const entry = JSON.stringify({ uid: conn.user.id, meta, node: this.nodeId, at: Date.now() });
    const update = JSON.stringify({ t: "pu", ch: frame.ch, uid: conn.user.id, meta });
    sub.meta = meta;
    await this.store.presenceUpdate(frame.ch, conn.id, entry, update);
    conn.sendControl({ t: "ok", id: frame.id });
  }

  async drain(): Promise<void> {
    if (this.draining || this.stopped) return;
    this.draining = true;
    this.log.info({ connections: this.connections.size }, "draining");
    if (this.config.drainNotifyDelayMs > 0) await new Promise((r) => setTimeout(r, this.config.drainNotifyDelayMs));
    for (const conn of this.connections.values()) {
      conn.sendControl({ t: "drain", after: Math.floor(Math.random() * this.config.drainMaxDelayMs) });
    }
    const deadline = Date.now() + this.config.drainCloseAfterMs;
    while (this.connections.size > 0 && Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, 100));
    }
    for (const conn of this.connections.values()) conn.close(CloseCode.GoingAway, "node draining");
    const closeDeadline = Date.now() + 2000;
    while (this.connections.size > 0 && Date.now() < closeDeadline) {
      await new Promise((r) => setTimeout(r, 50));
    }
    await this.stop();
  }

  async stop(): Promise<void> {
    if (this.stopped) return;
    this.ready = false;
    this.stopTimers();
    const leaves: Promise<unknown>[] = [];
    for (const conn of this.connections.values()) {
      for (const sub of conn.subs.values()) {
        if (sub.joined) leaves.push(this.store.presenceLeave(sub.ch, conn.id, this.nodeId).catch(() => undefined));
      }
    }
    await Promise.all(leaves);
    this.stopped = true;
    for (const conn of this.connections.values()) conn.terminate();
    await this.store.removeNode(this.nodeId).catch(() => undefined);
    this.metrics.stop();
    this.wss.close();
    await new Promise<void>((resolve) => {
      this.http.close(() => resolve());
      this.http.closeAllConnections();
    });
    await this.store.quit();
    this.log.info("realtime node stopped");
  }

  async crash(): Promise<void> {
    this.stopped = true;
    this.ready = false;
    this.stopTimers();
    this.metrics.stop();
    this.store.disconnectNow();
    for (const conn of this.connections.values()) conn.ws.terminate();
    this.wss.close();
    await new Promise<void>((resolve) => {
      this.http.close(() => resolve());
      this.http.closeAllConnections();
    });
  }
}
