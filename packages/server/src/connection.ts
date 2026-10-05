import type { WebSocket } from "ws";
import type { Logger } from "pino";
import { CloseCode, type ChannelInfo, type Codec, type JsonValue } from "@ashamrai/realtime-protocol";
import type { AuthUser } from "./auth.js";
import type { BackpressureConfig, RateConfig } from "./config.js";
import type { Metrics } from "./metrics.js";
import { toBuffer, type FanItem, type OutboundFrame, type OutKind } from "./outbound.js";
import { TokenBucket, ViolationCounter } from "./token-bucket.js";

export type ConnState = "active" | "lagging" | "closing";

export interface Subscription {
  ch: string;
  info: ChannelInfo;
  state: "syncing" | "live";
  buffer: FanItem[];
  lastSeq: number;
  presence: boolean;
  joined: boolean;
  generation: number;
  meta?: JsonValue;
}

export interface ConnectionHooks {
  onLagging(conn: Connection): void;
  onSlowClose(conn: Connection, reason: string): void;
}

const kindCounter = (metrics: Metrics, kind: OutKind) => {
  switch (kind) {
    case "durable":
      return metrics.outDurable;
    case "ephemeral":
      return metrics.outEphemeral;
    case "presence":
      return metrics.outPresence;
    case "history":
      return metrics.outHistory;
    default:
      return metrics.outControl;
  }
};

export class Connection {
  state: ConnState = "active";
  readonly subs = new Map<string, Subscription>();
  readonly ephPending = new Map<string, JsonValue>();
  readonly lagNotified = new Set<string>();
  readonly pub: TokenBucket;
  readonly eph: TokenBucket;
  readonly ctl: TokenBucket;
  readonly violations: ViolationCounter;
  pending: Buffer[] = [];
  pendingHead = 0;
  lagSince = 0;
  pingSentAt = 0;
  lastPingAt: number;
  debug = false;
  holdsUserSlot = false;
  log: Logger;
  timers: NodeJS.Timeout[] = [];
  private generationCounter = 0;

  constructor(
    readonly id: string,
    readonly ws: WebSocket,
    readonly user: AuthUser,
    readonly codec: Codec,
    private readonly baseLog: Logger,
    private readonly metrics: Metrics,
    private readonly bp: BackpressureConfig,
    rate: RateConfig,
    private readonly hooks: ConnectionHooks,
    readonly connectedAt: number = Date.now(),
  ) {
    this.pub = new TokenBucket(rate.pubPerSec, rate.pubBurst);
    this.eph = new TokenBucket(rate.ephPerSec, rate.ephBurst);
    this.ctl = new TokenBucket(rate.ctlPerSec, rate.ctlBurst);
    this.violations = new ViolationCounter(rate.violationsToClose, rate.violationWindowMs);
    this.lastPingAt = Date.now();
    this.log = baseLog.child({ cid: id, uid: user.id });
  }

  nextGeneration(): number {
    this.generationCounter++;
    return this.generationCounter;
  }

  setDebug(enabled: boolean): void {
    this.debug = enabled;
    this.log = this.baseLog.child({ cid: this.id, uid: this.user.id }, { level: enabled ? "debug" : this.baseLog.level });
  }

  get pendingCount(): number {
    return this.pending.length - this.pendingHead;
  }

  encode(frame: unknown): Buffer {
    return toBuffer(this.codec.encode(frame));
  }

  sendControl(frame: unknown): boolean {
    return this.sendBytes(this.encode(frame), "control");
  }

  sendFrame(frame: OutboundFrame, kind: OutKind, ch?: string): boolean {
    return this.sendBytes(frame.bytes(this.codec), kind, ch);
  }

  sendBytes(bytes: Buffer, kind: OutKind, ch?: string): boolean {
    if (this.state === "closing") return false;
    const buffered = this.ws.bufferedAmount;
    if (buffered > this.bp.hardBytes) {
      this.closeSlow("hard watermark");
      return false;
    }
    if (this.state === "lagging" || buffered > this.bp.highBytes) {
      if (kind === "ephemeral") {
        this.metrics.ephemeralDropped.inc();
        if (ch !== undefined && !this.lagNotified.has(ch)) {
          this.lagNotified.add(ch);
          this.enqueue(this.encode({ t: "lag", ch }));
        }
        this.markLagging();
        return false;
      }
      this.enqueue(bytes);
      kindCounter(this.metrics, kind).inc();
      if (this.pendingCount > this.bp.maxPending) {
        this.closeSlow("pending overflow");
        return false;
      }
      this.markLagging();
      return true;
    }
    this.ws.send(bytes, { binary: this.codec.binary });
    kindCounter(this.metrics, kind).inc();
    return true;
  }

  private enqueue(bytes: Buffer): void {
    this.pending.push(bytes);
  }

  private markLagging(): void {
    if (this.state === "lagging") return;
    this.state = "lagging";
    this.lagSince = Date.now();
    this.hooks.onLagging(this);
  }

  flushPending(now: number): "active" | "lagging" | "closed" {
    if (this.state === "closing") return "closed";
    if (now - this.lagSince > this.bp.lagTimeoutMs) {
      this.closeSlow("lagging timeout");
      return "closed";
    }
    while (this.pendingHead < this.pending.length && this.ws.bufferedAmount < this.bp.lowBytes) {
      const bytes = this.pending[this.pendingHead] as Buffer;
      this.pendingHead++;
      this.ws.send(bytes, { binary: this.codec.binary });
    }
    if (this.pendingHead >= this.pending.length) {
      this.pending = [];
      this.pendingHead = 0;
      if (this.ws.bufferedAmount < this.bp.highBytes) {
        this.state = "active";
        this.lagNotified.clear();
        return "active";
      }
    } else if (this.pendingHead > 1024 && this.pendingHead * 2 > this.pending.length) {
      this.pending = this.pending.slice(this.pendingHead);
      this.pendingHead = 0;
    }
    return "lagging";
  }

  closeSlow(reason: string): void {
    if (this.state === "closing") return;
    this.metrics.slowConsumerDisconnects.inc();
    this.hooks.onSlowClose(this, reason);
    this.close(CloseCode.SlowConsumer, "slow consumer");
  }

  close(code: number, reason: string): void {
    if (this.state === "closing") return;
    this.state = "closing";
    this.pending = [];
    this.pendingHead = 0;
    this.metrics.closes.inc({ code: String(code) });
    try {
      this.ws.close(code, reason);
    } catch {
      this.ws.terminate();
      return;
    }
    const ws = this.ws;
    setTimeout(() => ws.terminate(), 3000).unref();
  }

  terminate(): void {
    this.state = "closing";
    this.pending = [];
    this.ws.terminate();
  }

  clearTimers(): void {
    for (const timer of this.timers) clearTimeout(timer);
    this.timers = [];
  }
}
