import { CHANNEL_NAME_MAX, CHANNEL_NAME_PATTERN } from "./constants.js";
import type { ErrorCode } from "./constants.js";
import {
  arr,
  bool,
  discriminated,
  int,
  json,
  lit,
  num,
  obj,
  opt,
  str,
  validate,
  type Infer,
  type JsonValue,
  type ValidationResult,
} from "./validator.js";

const requestId = int({ min: 0, max: 2 ** 31 - 1 });
const channelName = str({ min: 1, max: CHANNEL_NAME_MAX, pattern: CHANNEL_NAME_PATTERN });
const seqNumber = int({ min: 0 });

export const subFrameSchema = obj({
  t: lit("sub"),
  id: requestId,
  ch: channelName,
  from: opt(seqNumber),
  history: opt(int({ min: 0, max: 1000 })),
  presence: opt(bool()),
});

export const unsubFrameSchema = obj({
  t: lit("unsub"),
  id: requestId,
  ch: channelName,
});

export const pubFrameSchema = obj({
  t: lit("pub"),
  id: requestId,
  ch: channelName,
  d: json(),
  cmid: str({ min: 1, max: 64, pattern: /^[A-Za-z0-9_-]+$/ }),
});

export const ephFrameSchema = obj({
  t: lit("eph"),
  ch: channelName,
  d: json(),
});

export const presFrameSchema = obj({
  t: lit("pres"),
  id: requestId,
  ch: channelName,
  meta: opt(json({ maxDepth: 4 })),
});

export const pingFrameSchema = obj({
  t: lit("ping"),
  ts: num(),
});

export const clientFrameSchema = discriminated("t", {
  sub: subFrameSchema,
  unsub: unsubFrameSchema,
  pub: pubFrameSchema,
  eph: ephFrameSchema,
  pres: presFrameSchema,
  ping: pingFrameSchema,
});

export type SubFrame = Infer<typeof subFrameSchema>;
export type UnsubFrame = Infer<typeof unsubFrameSchema>;
export type PubFrame = Infer<typeof pubFrameSchema>;
export type EphFrame = Infer<typeof ephFrameSchema>;
export type PresFrame = Infer<typeof presFrameSchema>;
export type PingFrame = Infer<typeof pingFrameSchema>;
export type ClientFrame = SubFrame | UnsubFrame | PubFrame | EphFrame | PresFrame | PingFrame;

export function validateClientFrame(value: unknown): ValidationResult<ClientFrame> {
  return validate(clientFrameSchema, value) as ValidationResult<ClientFrame>;
}

export interface PresenceMember {
  uid: string;
  meta?: JsonValue;
}

export interface HelloFrame {
  t: "hello";
  cid: string;
  node: string;
  hb: number;
  v: number;
}

export interface OkFrame {
  t: "ok";
  id: number;
  seq?: number;
  mid?: string;
  dup?: boolean;
  presence?: PresenceMember[];
  pn?: number;
}

export interface ErrFrame {
  t: "err";
  id?: number;
  code: ErrorCode;
  msg: string;
}

export interface MsgFrame {
  t: "msg";
  ch: string;
  seq: number;
  mid: string;
  d: JsonValue;
  ts: number;
  from: string;
}

export interface EphOutFrame {
  t: "eph";
  ch: string;
  d: JsonValue;
  from: string;
}

export interface PresenceJoinFrame {
  t: "pj";
  ch: string;
  uid: string;
  meta?: JsonValue;
}

export interface PresenceLeaveFrame {
  t: "pl";
  ch: string;
  uid: string;
}

export interface PresenceUpdateFrame {
  t: "pu";
  ch: string;
  uid: string;
  meta?: JsonValue;
}

export interface ResetFrame {
  t: "reset";
  ch: string;
  seq: number;
}

export interface LagFrame {
  t: "lag";
  ch: string;
}

export interface DrainFrame {
  t: "drain";
  after: number;
}

export interface PongFrame {
  t: "pong";
  ts: number;
}

export type PresenceFrame = PresenceJoinFrame | PresenceLeaveFrame | PresenceUpdateFrame;

export type ServerFrame =
  | HelloFrame
  | OkFrame
  | ErrFrame
  | MsgFrame
  | EphOutFrame
  | PresenceFrame
  | ResetFrame
  | LagFrame
  | DrainFrame
  | PongFrame;

const presenceMemberSchema = obj({ uid: str({ min: 1 }), meta: opt(json()) });

export const serverFrameSchema = discriminated("t", {
  hello: obj({ t: lit("hello"), cid: str(), node: str(), hb: num({ min: 0 }), v: int() }),
  ok: obj({
    t: lit("ok"),
    id: int(),
    seq: opt(int({ min: 0 })),
    mid: opt(str()),
    dup: opt(bool()),
    presence: opt(arr(presenceMemberSchema)),
    pn: opt(int({ min: 0 })),
  }),
  err: obj({ t: lit("err"), id: opt(int()), code: str(), msg: str() }),
  msg: obj({ t: lit("msg"), ch: str(), seq: int({ min: 1 }), mid: str(), d: json({ maxDepth: 64 }), ts: num(), from: str() }),
  eph: obj({ t: lit("eph"), ch: str(), d: json({ maxDepth: 64 }), from: str() }),
  pj: obj({ t: lit("pj"), ch: str(), uid: str(), meta: opt(json()) }),
  pl: obj({ t: lit("pl"), ch: str(), uid: str() }),
  pu: obj({ t: lit("pu"), ch: str(), uid: str(), meta: opt(json()) }),
  reset: obj({ t: lit("reset"), ch: str(), seq: int({ min: 0 }) }),
  lag: obj({ t: lit("lag"), ch: str() }),
  drain: obj({ t: lit("drain"), after: num({ min: 0 }) }),
  pong: obj({ t: lit("pong"), ts: num() }),
});

export const SERVER_FRAME_TYPES = new Set(["hello", "ok", "err", "msg", "eph", "pj", "pl", "pu", "reset", "lag", "drain", "pong"]);

export type ServerFrameResult = { kind: "frame"; frame: ServerFrame } | { kind: "unknown" } | { kind: "invalid"; error: string };

export function validateServerFrame(value: unknown): ServerFrameResult {
  if (typeof value !== "object" || value === null) return { kind: "invalid", error: "$: expected object" };
  const t = (value as Record<string, unknown>)["t"];
  if (typeof t !== "string" || !SERVER_FRAME_TYPES.has(t)) return { kind: "unknown" };
  const result = validate(serverFrameSchema, value);
  if (!result.ok) return { kind: "invalid", error: result.error };
  return { kind: "frame", frame: value as ServerFrame };
}
