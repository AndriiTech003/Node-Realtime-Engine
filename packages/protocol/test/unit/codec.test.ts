import { describe, expect, it } from "vitest";
import {
  CodecError,
  codecByName,
  codecForSubprotocol,
  jsonCodec,
  msgpackCodec,
  selectSubprotocol,
} from "../../src/codec.js";
import { SUBPROTOCOL_JSON, SUBPROTOCOL_MSGPACK } from "../../src/constants.js";

const frame = { t: "msg", ch: "room:42", seq: 1181, mid: "abc", d: { text: "hi", n: [1, 2.5, null], ok: true }, ts: 1759250000123, from: "u_7" };

describe("codecs", () => {
  it("round-trips JSON as text", () => {
    const wire = jsonCodec.encode(frame);
    expect(typeof wire).toBe("string");
    expect(jsonCodec.decode(wire)).toEqual(frame);
    expect(jsonCodec.decode(new TextEncoder().encode(wire as string))).toEqual(frame);
  });

  it("round-trips MessagePack as binary and is smaller than JSON", () => {
    const wire = msgpackCodec.encode(frame);
    expect(wire).toBeInstanceOf(Uint8Array);
    expect(msgpackCodec.decode(wire)).toEqual(frame);
    expect((wire as Uint8Array).byteLength).toBeLessThan((jsonCodec.encode(frame) as string).length);
  });

  it("keeps large sequence numbers and timestamps as numbers", () => {
    const value = msgpackCodec.decode(msgpackCodec.encode({ seq: 2 ** 40, ts: Date.now() })) as { seq: number; ts: number };
    expect(typeof value.seq).toBe("number");
    expect(value.seq).toBe(2 ** 40);
  });

  it("raises CodecError on garbage", () => {
    expect(() => jsonCodec.decode("{nope")).toThrow(CodecError);
    expect(() => msgpackCodec.decode("text")).toThrow(CodecError);
    expect(() => msgpackCodec.decode(new Uint8Array([0x93, 0x01]))).toThrow(CodecError);
  });

  it("negotiates subprotocols", () => {
    expect(codecForSubprotocol(SUBPROTOCOL_MSGPACK)).toBe(msgpackCodec);
    expect(codecForSubprotocol(SUBPROTOCOL_JSON)).toBe(jsonCodec);
    expect(codecForSubprotocol(undefined)).toBe(jsonCodec);
    expect(codecByName("msgpack")).toBe(msgpackCodec);
    expect(selectSubprotocol(["chat", SUBPROTOCOL_MSGPACK, SUBPROTOCOL_JSON])).toBe(SUBPROTOCOL_MSGPACK);
    expect(selectSubprotocol(new Set([SUBPROTOCOL_JSON]))).toBe(SUBPROTOCOL_JSON);
    expect(selectSubprotocol(["pulse.v2.json"])).toBeNull();
  });
});
