import { Packr } from "msgpackr";
import { SUBPROTOCOL_JSON, SUBPROTOCOL_MSGPACK, type Subprotocol } from "./constants.js";

export type CodecName = "json" | "msgpack";

export type WireData = string | Uint8Array;

export interface Codec {
  readonly name: CodecName;
  readonly subprotocol: Subprotocol;
  readonly binary: boolean;
  encode(frame: unknown): WireData;
  decode(data: WireData): unknown;
}

const textDecoder = new TextDecoder();

export class CodecError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "CodecError";
  }
}

export const jsonCodec: Codec = {
  name: "json",
  subprotocol: SUBPROTOCOL_JSON,
  binary: false,
  encode(frame) {
    return JSON.stringify(frame);
  },
  decode(data) {
    const text = typeof data === "string" ? data : textDecoder.decode(data);
    try {
      return JSON.parse(text) as unknown;
    } catch {
      throw new CodecError("invalid JSON");
    }
  },
};

const packr = new Packr({ useRecords: false, mapsAsObjects: true, int64AsType: "number" });

export const msgpackCodec: Codec = {
  name: "msgpack",
  subprotocol: SUBPROTOCOL_MSGPACK,
  binary: true,
  encode(frame) {
    return packr.pack(frame);
  },
  decode(data) {
    if (typeof data === "string") throw new CodecError("msgpack codec expects binary frames");
    try {
      return packr.unpack(data) as unknown;
    } catch {
      throw new CodecError("invalid MessagePack");
    }
  },
};

export function codecForSubprotocol(subprotocol: string | undefined | null): Codec {
  return subprotocol === SUBPROTOCOL_MSGPACK ? msgpackCodec : jsonCodec;
}

export function codecByName(name: CodecName): Codec {
  return name === "msgpack" ? msgpackCodec : jsonCodec;
}

export function selectSubprotocol(offered: Iterable<string>): Subprotocol | null {
  let fallback: Subprotocol | null = null;
  for (const p of offered) {
    if (p === SUBPROTOCOL_JSON || p === SUBPROTOCOL_MSGPACK) {
      if (fallback === null) fallback = p;
    }
  }
  return fallback;
}
