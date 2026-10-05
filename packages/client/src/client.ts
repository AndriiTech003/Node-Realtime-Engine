import {
  CloseCode,
  closeCodePolicy,
  jsonCodec,
  msgpackCodec,
  validateServerFrame,
  type Codec,
  type CodecName,
  type ErrFrame,
  type JsonValue,
  type ServerFrame,
} from "@ashamrai/realtime-protocol";
import { backoffDelay, type JitterMode } from "./backoff.js";
import { Channel, type ChannelHost, type PublishAck, type SubscribeOptions } from "./channel.js";
import { Emitter } from "./emitter.js";
import { uuid } from "./uuid.js";

export type ClientState = "idle" | "connecting" | "open" | "reconnecting" | "closed";

export interface WebSocketLike {
  readonly readyState: number;
  readonly protocol: string;
  binaryType: string;
  onopen: ((event: unknown) => void) | null;
  onmessage: ((event: { data: unknown }) => void) | null;
  onclose: ((event: { code: number; reason: string }) => void) | null;
  onerror: ((event: unknown) => void) | null;
  send(data: string | ArrayBufferLike | ArrayBufferView): void;
  close(code?: number, reason?: string): void;
}

export type WebSocketConstructor = new (url: string, protocols?: string | string[]) => WebSocketLike;

export interface RealtimeClientOptions {
  url: string;
  getTicket: () => Promise<string>;
  codec?: CodecName;
  WebSocket?: WebSocketConstructor;
  reconnect?: { baseMs?: number; maxMs?: number; jitter?: JitterMode };
  pingIntervalMs?: number;
  pongTimeoutMs?: number;
  ephemeralThrottleMs?: number;
  offlineQueueLimit?: number;
  requestTimeoutMs?: number;
  autoConnect?: boolean;
}

export interface ClientStats {
  reconnects: number;
  gaps: number;
  duplicates: number;
  resumedMessages: number;
  resets: number;
  queued: number;
}

export interface ClientEvents {
  state: { state: ClientState; previous: ClientState };
  open: { cid: string; node: string };
  close: { code: number; reason: string };
  drain: { after: number };
  reconnecting: { delay: number; attempt: number; reason: string };
  error: { message: string; frame?: ErrFrame };
}

export class RealtimeError extends Error {
  constructor(
    readonly code: string,
    message: string,
  ) {
    super(message);
    this.name = "RealtimeError";
  }
}

interface OutboxEntry {
  ch: string;
  d: JsonValue;
  cmid: string;
  resolve: (ack: PublishAck) => void;
  reject: (error: Error) => void;
  sentWith: number | null;
}

interface PendingRequest {
  resolve: (frame: ServerFrame) => void;
  reject: (error: Error) => void;
  timer: ReturnType<typeof setTimeout>;
}

interface EphemeralSlot {
  last: number;
  pending: JsonValue | undefined;
  timer: ReturnType<typeof setTimeout> | null;
}

const OPEN = 1;

function resolveWebSocket(option: WebSocketConstructor | undefined): WebSocketConstructor {
  if (option !== undefined) return option;
  const global = (globalThis as { WebSocket?: WebSocketConstructor }).WebSocket;
  if (global === undefined) throw new Error("No WebSocket implementation available; pass options.WebSocket");
  return global;
}

function toWire(data: unknown): string | Uint8Array | null {
  if (typeof data === "string") return data;
  if (data instanceof ArrayBuffer) return new Uint8Array(data);
  if (ArrayBuffer.isView(data)) return new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
  return null;
}

export class RealtimeClient extends Emitter<ClientEvents> {
  state: ClientState = "idle";
  cid: string | null = null;
  node: string | null = null;
  readonly stats: ClientStats = { reconnects: 0, gaps: 0, duplicates: 0, resumedMessages: 0, resets: 0, queued: 0 };
  private readonly codec: Codec;
  private readonly WS: WebSocketConstructor;
  private readonly channels = new Map<string, Channel>();
  private readonly outbox: OutboxEntry[] = [];
  private readonly inflight = new Map<number, OutboxEntry>();
  private readonly requests = new Map<number, PendingRequest>();
  private readonly ephemeral = new Map<string, EphemeralSlot>();
  private ws: WebSocketLike | null = null;
  private nextId = 1;
  private attempt = 0;
  private manualClose = false;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private drainTimer: ReturnType<typeof setTimeout> | null = null;
  private pingTimer: ReturnType<typeof setInterval> | null = null;
  private pongTimer: ReturnType<typeof setTimeout> | null = null;
  private everOpened = false;
  private readonly opts: Required<Omit<RealtimeClientOptions, "WebSocket" | "reconnect" | "codec">> & {
    reconnect: { baseMs: number; maxMs: number; jitter: JitterMode };
  };

  constructor(options: RealtimeClientOptions) {
    super();
    this.codec = options.codec === "msgpack" ? msgpackCodec : jsonCodec;
    this.WS = resolveWebSocket(options.WebSocket);
    this.opts = {
      url: options.url,
      getTicket: options.getTicket,
      pingIntervalMs: options.pingIntervalMs ?? 20000,
      pongTimeoutMs: options.pongTimeoutMs ?? 10000,
      ephemeralThrottleMs: options.ephemeralThrottleMs ?? 50,
      offlineQueueLimit: options.offlineQueueLimit ?? 1000,
      requestTimeoutMs: options.requestTimeoutMs ?? 10000,
      autoConnect: options.autoConnect ?? true,
      reconnect: {
        baseMs: options.reconnect?.baseMs ?? 500,
        maxMs: options.reconnect?.maxMs ?? 30000,
        jitter: options.reconnect?.jitter ?? "full",
      },
    };
    if (this.opts.autoConnect) this.connect();
  }

  get codecName(): CodecName {
    return this.codec.name;
  }

  connect(): void {
    if (this.state === "connecting" || this.state === "open" || this.state === "reconnecting") return;
    this.manualClose = false;
    this.setState(this.everOpened ? "reconnecting" : "connecting");
    void this.openSocket();
  }

  disconnect(): void {
    this.manualClose = true;
    this.clearTimers();
    const ws = this.ws;
    this.ws = null;
    if (ws !== null) {
      this.detach(ws);
      ws.close(CloseCode.Normal, "client disconnect");
    }
    this.onDisconnected();
    this.setState("closed");
    this.emit("close", { code: CloseCode.Normal, reason: "client disconnect" });
  }

  close(): void {
    this.disconnect();
    for (const entry of this.outbox) entry.reject(new RealtimeError("CLOSED", "client closed"));
    this.outbox.length = 0;
    this.stats.queued = 0;
  }

  kill(): void {
    if (this.ws !== null) this.ws.close(4000, "simulated network failure");
  }

  subscribe(name: string, options: SubscribeOptions = {}): Channel {
    const existing = this.channels.get(name);
    if (existing !== undefined && existing.state !== "closed") return existing;
    const channel = new Channel(name, options, this.host);
    this.channels.set(name, channel);
    if (this.isOpen()) this.sendSub(channel);
    return channel;
  }

  channel(name: string): Channel | undefined {
    return this.channels.get(name);
  }

  publish(ch: string, d: JsonValue): Promise<PublishAck> {
    return new Promise((resolve, reject) => {
      if (this.outbox.length >= this.opts.offlineQueueLimit) {
        reject(new RealtimeError("QUEUE_FULL", "offline queue is full"));
        return;
      }
      const entry: OutboxEntry = { ch, d, cmid: uuid(), resolve, reject, sentWith: null };
      this.outbox.push(entry);
      this.stats.queued = this.outbox.length;
      if (this.isOpen()) this.sendPub(entry);
    });
  }

  private readonly host: ChannelHost = {
    sendSub: (channel) => {
      if (this.isOpen()) this.sendSub(channel);
    },
    sendUnsub: (channel) => {
      this.channels.delete(channel.name);
      this.ephemeral.delete(channel.name);
      if (this.isOpen()) this.send({ t: "unsub", id: this.nextId++, ch: channel.name });
    },
    publish: (ch, d) => this.publish(ch, d),
    sendEphemeral: (ch, d) => this.sendEphemeral(ch, d),
    setPresence: async (ch, meta) => {
      const frame = await this.request({ t: "pres", ch, meta });
      if (frame.t === "err") throw new RealtimeError(frame.code, frame.msg);
    },
    countDuplicate: () => {
      this.stats.duplicates++;
    },
    countGap: () => {
      this.stats.gaps++;
    },
    countResumed: (n) => {
      this.stats.resumedMessages += n;
    },
    countReset: () => {
      this.stats.resets++;
    },
  };

  private isOpen(): boolean {
    return this.state === "open" && this.ws !== null && this.ws.readyState === OPEN;
  }

  private setState(state: ClientState): void {
    if (state === this.state) return;
    const previous = this.state;
    this.state = state;
    this.emit("state", { state, previous });
  }

  private send(frame: Record<string, unknown>): void {
    const ws = this.ws;
    if (ws === null || ws.readyState !== OPEN) return;
    ws.send(this.codec.encode(frame));
  }

  private request(frame: Record<string, unknown>): Promise<ServerFrame> {
    return new Promise((resolve, reject) => {
      if (!this.isOpen()) {
        reject(new RealtimeError("NOT_CONNECTED", "not connected"));
        return;
      }
      const id = this.nextId++;
      const timer = setTimeout(() => {
        this.requests.delete(id);
        reject(new RealtimeError("TIMEOUT", "request timed out"));
      }, this.opts.requestTimeoutMs);
      this.requests.set(id, { resolve, reject, timer });
      this.send({ ...frame, id });
    });
  }

  private sendSub(channel: Channel): void {
    const id = this.nextId++;
    this.send(channel.buildSubFrame(id) as unknown as Record<string, unknown>);
  }

  private sendPub(entry: OutboxEntry): void {
    const id = this.nextId++;
    entry.sentWith = id;
    this.inflight.set(id, entry);
    this.send({ t: "pub", id, ch: entry.ch, d: entry.d, cmid: entry.cmid });
  }

  private sendEphemeral(ch: string, d: JsonValue): void {
    if (!this.isOpen()) return;
    let slot = this.ephemeral.get(ch);
    if (slot === undefined) {
      slot = { last: 0, pending: undefined, timer: null };
      this.ephemeral.set(ch, slot);
    }
    const now = Date.now();
    if (slot.timer !== null) {
      slot.pending = d;
      return;
    }
    const wait = this.opts.ephemeralThrottleMs - (now - slot.last);
    if (wait <= 0) {
      slot.last = now;
      this.send({ t: "eph", ch, d });
      return;
    }
    slot.pending = d;
    const s = slot;
    slot.timer = setTimeout(() => {
      s.timer = null;
      if (s.pending === undefined) return;
      const value = s.pending;
      s.pending = undefined;
      s.last = Date.now();
      if (this.isOpen()) this.send({ t: "eph", ch, d: value });
    }, wait);
  }

  private async openSocket(): Promise<void> {
    let ticket: string;
    try {
      ticket = await this.opts.getTicket();
    } catch (error) {
      if (this.manualClose) return;
      this.emit("error", { message: `ticket request failed: ${error instanceof Error ? error.message : String(error)}` });
      this.scheduleReconnect("ticket");
      return;
    }
    if (this.manualClose) return;
    const separator = this.opts.url.includes("?") ? "&" : "?";
    let ws: WebSocketLike;
    try {
      ws = new this.WS(`${this.opts.url}${separator}ticket=${encodeURIComponent(ticket)}`, [this.codec.subprotocol]);
    } catch (error) {
      this.emit("error", { message: error instanceof Error ? error.message : String(error) });
      this.scheduleReconnect("socket");
      return;
    }
    ws.binaryType = "arraybuffer";
    this.ws = ws;
    ws.onmessage = (event) => {
      if (this.ws !== ws) return;
      this.onData(event.data);
    };
    ws.onclose = (event) => {
      if (this.ws !== ws) return;
      this.onSocketClosed(event.code, event.reason);
    };
    ws.onerror = () => undefined;
  }

  private detach(ws: WebSocketLike): void {
    ws.onmessage = null;
    ws.onclose = null;
    ws.onerror = null;
    ws.onopen = null;
  }

  private onData(data: unknown): void {
    const wire = toWire(data);
    if (wire === null) return;
    let decoded: unknown;
    try {
      decoded = this.codec.decode(wire);
    } catch {
      this.emit("error", { message: "undecodable frame from server" });
      return;
    }
    const result = validateServerFrame(decoded);
    if (result.kind !== "frame") return;
    this.onFrame(result.frame);
  }

  private onFrame(frame: ServerFrame): void {
    switch (frame.t) {
      case "hello":
        this.onHello(frame.cid, frame.node);
        return;
      case "msg":
        this.channels.get(frame.ch)?.handleMsg(frame);
        return;
      case "ok":
        this.onOk(frame);
        return;
      case "err":
        this.onErr(frame);
        return;
      case "eph":
        this.channels.get(frame.ch)?.handleEphemeral(frame.d, frame.from);
        return;
      case "pj":
        this.channels.get(frame.ch)?.handleJoin(frame.uid, frame.meta);
        return;
      case "pl":
        this.channels.get(frame.ch)?.handleLeave(frame.uid);
        return;
      case "pu":
        this.channels.get(frame.ch)?.handleUpdate(frame.uid, frame.meta);
        return;
      case "reset":
        this.channels.get(frame.ch)?.handleReset(frame.seq);
        return;
      case "lag":
        this.channels.get(frame.ch)?.handleLag();
        return;
      case "drain":
        this.onDrain(frame.after);
        return;
      case "pong":
        if (this.pongTimer !== null) {
          clearTimeout(this.pongTimer);
          this.pongTimer = null;
        }
        return;
    }
  }

  private onHello(cid: string, node: string): void {
    this.cid = cid;
    this.node = node;
    if (this.everOpened) this.stats.reconnects++;
    this.everOpened = true;
    this.attempt = 0;
    this.setState("open");
    this.emit("open", { cid, node });
    for (const channel of this.channels.values()) {
      if (channel.state !== "closed") this.sendSub(channel);
    }
    for (const entry of this.outbox) this.sendPub(entry);
    this.startPing();
  }

  private onOk(frame: Extract<ServerFrame, { t: "ok" }>): void {
    const pub = this.inflight.get(frame.id);
    if (pub !== undefined) {
      this.inflight.delete(frame.id);
      const index = this.outbox.indexOf(pub);
      if (index >= 0) this.outbox.splice(index, 1);
      this.stats.queued = this.outbox.length;
      pub.resolve({ seq: frame.seq ?? 0, mid: frame.mid ?? "", dup: frame.dup === true });
      return;
    }
    const request = this.requests.get(frame.id);
    if (request !== undefined) {
      this.requests.delete(frame.id);
      clearTimeout(request.timer);
      request.resolve(frame);
      return;
    }
    for (const channel of this.channels.values()) {
      if (channel.requestId === frame.id && channel.state === "subscribing") {
        channel.handleOk(frame.seq, frame.presence, frame.pn);
        return;
      }
    }
  }

  private onErr(frame: ErrFrame): void {
    if (frame.id !== undefined) {
      const pub = this.inflight.get(frame.id);
      if (pub !== undefined) {
        this.inflight.delete(frame.id);
        const index = this.outbox.indexOf(pub);
        if (index >= 0) this.outbox.splice(index, 1);
        this.stats.queued = this.outbox.length;
        pub.reject(new RealtimeError(frame.code, frame.msg));
        return;
      }
      const request = this.requests.get(frame.id);
      if (request !== undefined) {
        this.requests.delete(frame.id);
        clearTimeout(request.timer);
        request.resolve(frame);
        return;
      }
      for (const channel of this.channels.values()) {
        if (channel.requestId === frame.id) {
          channel.handleError(frame);
          return;
        }
      }
    }
    this.emit("error", { message: frame.msg, frame });
  }

  private onDrain(after: number): void {
    this.emit("drain", { after });
    if (this.drainTimer !== null) clearTimeout(this.drainTimer);
    this.drainTimer = setTimeout(() => {
      this.drainTimer = null;
      if (this.manualClose) return;
      const ws = this.ws;
      if (ws !== null) {
        this.ws = null;
        this.detach(ws);
        ws.close(CloseCode.Normal, "drain reconnect");
      }
      this.onDisconnected();
      this.setState("reconnecting");
      this.emit("reconnecting", { delay: 0, attempt: this.attempt, reason: "drain" });
      void this.openSocket();
    }, after);
  }

  private startPing(): void {
    this.stopPing();
    this.pingTimer = setInterval(() => {
      if (!this.isOpen() || this.pongTimer !== null) return;
      this.send({ t: "ping", ts: Date.now() });
      this.pongTimer = setTimeout(() => {
        this.pongTimer = null;
        const ws = this.ws;
        if (ws === null) return;
        this.emit("error", { message: "pong timeout" });
        this.ws = null;
        this.detach(ws);
        ws.close(4000, "pong timeout");
        this.onSocketLost(1006, "pong timeout");
      }, this.opts.pongTimeoutMs);
    }, this.opts.pingIntervalMs);
  }

  private stopPing(): void {
    if (this.pingTimer !== null) clearInterval(this.pingTimer);
    if (this.pongTimer !== null) clearTimeout(this.pongTimer);
    this.pingTimer = null;
    this.pongTimer = null;
  }

  private onSocketClosed(code: number, reason: string): void {
    this.ws = null;
    this.onSocketLost(code, reason);
  }

  private onSocketLost(code: number, reason: string): void {
    this.onDisconnected();
    this.emit("close", { code, reason });
    if (this.manualClose) {
      this.setState("closed");
      return;
    }
    const policy = closeCodePolicy(code);
    switch (policy) {
      case "none":
        this.setState("closed");
        return;
      case "drain":
        if (this.drainTimer !== null) {
          this.setState("reconnecting");
          return;
        }
        this.scheduleReconnect("going away");
        return;
      case "new-ticket":
        this.scheduleReconnect("ticket expired", 0);
        return;
      case "immediate":
        this.scheduleReconnect("slow consumer", 0);
        return;
      default:
        this.scheduleReconnect(code === CloseCode.RateLimited ? "rate limited" : "connection lost");
    }
  }

  private onDisconnected(): void {
    this.stopPing();
    this.inflight.clear();
    for (const entry of this.outbox) entry.sentWith = null;
    for (const request of this.requests.values()) {
      clearTimeout(request.timer);
      request.reject(new RealtimeError("DISCONNECTED", "connection lost"));
    }
    this.requests.clear();
    for (const channel of this.channels.values()) channel.markDisconnected();
    for (const slot of this.ephemeral.values()) {
      if (slot.timer !== null) clearTimeout(slot.timer);
      slot.timer = null;
      slot.pending = undefined;
    }
  }

  private scheduleReconnect(reason: string, fixedDelay?: number): void {
    if (this.manualClose) return;
    const attempt = this.attempt;
    this.attempt++;
    const delay = fixedDelay !== undefined && attempt === 0 ? fixedDelay : backoffDelay(attempt, this.opts.reconnect);
    this.setState("reconnecting");
    this.emit("reconnecting", { delay, attempt, reason });
    if (this.reconnectTimer !== null) clearTimeout(this.reconnectTimer);
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      if (this.manualClose) return;
      void this.openSocket();
    }, delay);
  }

  private clearTimers(): void {
    this.stopPing();
    if (this.reconnectTimer !== null) clearTimeout(this.reconnectTimer);
    if (this.drainTimer !== null) clearTimeout(this.drainTimer);
    this.reconnectTimer = null;
    this.drainTimer = null;
  }
}
