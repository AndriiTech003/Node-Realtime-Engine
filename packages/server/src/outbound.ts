import type { Codec } from "@ashamrai/realtime-protocol";

export type OutKind = "durable" | "ephemeral" | "presence" | "control" | "history";

export function toBuffer(data: string | Uint8Array): Buffer {
  if (typeof data === "string") return Buffer.from(data, "utf8");
  return Buffer.isBuffer(data) ? data : Buffer.from(data.buffer, data.byteOffset, data.byteLength);
}

export class OutboundFrame {
  private jsonBytes: Buffer | null = null;
  private msgpackBytes: Buffer | null = null;
  private object: unknown = undefined;

  constructor(
    private readonly buildObject: () => unknown,
    private readonly jsonText: string | null,
    private readonly cache: boolean,
  ) {}

  value(): unknown {
    if (this.object === undefined) this.object = this.buildObject();
    return this.object;
  }

  bytes(codec: Codec): Buffer {
    if (!this.cache) {
      return toBuffer(codec.encode(this.buildObject()));
    }
    if (codec.name === "json") {
      if (this.jsonBytes === null) {
        this.jsonBytes = Buffer.from(this.jsonText ?? JSON.stringify(this.value()), "utf8");
      }
      return this.jsonBytes;
    }
    if (this.msgpackBytes === null) this.msgpackBytes = toBuffer(codec.encode(this.value()));
    return this.msgpackBytes;
  }
}

export interface DurableItem {
  kind: "durable";
  ch: string;
  seq: number;
  frame: OutboundFrame;
  receivedAt: number;
}

export interface PresenceItem {
  kind: "presence";
  ch: string;
  seq?: undefined;
  frame: OutboundFrame;
  receivedAt: number;
}

export interface EphemeralItem {
  kind: "ephemeral";
  ch: string;
  seq?: undefined;
  origin: string;
  frame: OutboundFrame;
  receivedAt: number;
}

export type FanItem = DurableItem | PresenceItem | EphemeralItem;

export function durableFrameJson(ch: string, seq: number, ts: number, payload: string): string {
  return `{"t":"msg","ch":${JSON.stringify(ch)},"seq":${seq},"ts":${ts},${payload.slice(1)}`;
}

export function durableFrameObject(ch: string, seq: number, ts: number, payload: string): Record<string, unknown> {
  const parsed = JSON.parse(payload) as Record<string, unknown>;
  return { t: "msg", ch, seq, ts, mid: parsed["mid"], from: parsed["from"], d: parsed["d"] };
}

export function parseFanMessage(ch: string, message: string, cache: boolean, receivedAt: number): FanItem | null {
  const first = message.charCodeAt(0);
  if (first >= 48 && first <= 57) {
    const a = message.indexOf("|");
    const b = message.indexOf("|", a + 1);
    if (a < 0 || b < 0) return null;
    const seq = Number(message.slice(0, a));
    const ts = Number(message.slice(a + 1, b));
    const payload = message.slice(b + 1);
    return {
      kind: "durable",
      ch,
      seq,
      receivedAt,
      frame: new OutboundFrame(
        () => durableFrameObject(ch, seq, ts, payload),
        cache ? durableFrameJson(ch, seq, ts, payload) : null,
        cache,
      ),
    };
  }
  if (message.startsWith("p|")) {
    const json = message.slice(2);
    return {
      kind: "presence",
      ch,
      receivedAt,
      frame: new OutboundFrame(() => JSON.parse(json) as unknown, json, cache),
    };
  }
  if (message.startsWith("e|")) {
    const sep = message.indexOf("|", 2);
    if (sep < 0) return null;
    const origin = message.slice(2, sep);
    const json = message.slice(sep + 1);
    return {
      kind: "ephemeral",
      ch,
      origin,
      receivedAt,
      frame: new OutboundFrame(() => JSON.parse(json) as unknown, json, cache),
    };
  }
  return null;
}
