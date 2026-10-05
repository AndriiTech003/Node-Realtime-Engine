import { describe, expect, it } from "vitest";
import { jsonCodec, msgpackCodec } from "@ashamrai/realtime-protocol";
import { durableFrameJson, durableFrameObject, OutboundFrame, parseFanMessage } from "../../src/outbound.js";

const payload = JSON.stringify({ mid: "m-1", from: "u_1", d: { text: "hi \"quoted\" ü" } });

describe("fan-out message parsing", () => {
  it("builds the same frame via string splicing as via JSON.stringify", () => {
    const spliced = durableFrameJson("room:42", 7, 1700000000000, payload);
    expect(JSON.parse(spliced)).toEqual(durableFrameObject("room:42", 7, 1700000000000, payload));
    expect(JSON.parse(spliced)).toEqual({ t: "msg", ch: "room:42", seq: 7, ts: 1700000000000, mid: "m-1", from: "u_1", d: { text: "hi \"quoted\" ü" } });
  });

  it("parses durable, presence and ephemeral messages", () => {
    const durable = parseFanMessage("room:1", `12|1700|${payload}`, true, 0);
    expect(durable).toMatchObject({ kind: "durable", seq: 12, ch: "room:1" });
    const presence = parseFanMessage("room:1", `p|{"t":"pj","ch":"room:1","uid":"a"}`, true, 0);
    expect(presence?.kind).toBe("presence");
    const eph = parseFanMessage("room:1", `e|cid-1|{"t":"eph","ch":"room:1","d":1,"from":"a"}`, true, 0);
    expect(eph).toMatchObject({ kind: "ephemeral", origin: "cid-1" });
    expect(parseFanMessage("room:1", "zzz", true, 0)).toBeNull();
  });

  it("serializes once per codec when caching is on", () => {
    const item = parseFanMessage("room:1", `3|1700|${payload}`, true, 0);
    if (item === null) throw new Error("parse failed");
    const a = item.frame.bytes(jsonCodec);
    const b = item.frame.bytes(jsonCodec);
    expect(a).toBe(b);
    const m1 = item.frame.bytes(msgpackCodec);
    expect(m1).toBe(item.frame.bytes(msgpackCodec));
    expect(msgpackCodec.decode(m1)).toEqual(JSON.parse(a.toString("utf8")));
  });

  it("re-encodes for every subscriber when caching is off", () => {
    const frame = new OutboundFrame(() => ({ t: "pong", ts: 1 }), null, false);
    const a = frame.bytes(jsonCodec);
    const b = frame.bytes(jsonCodec);
    expect(a).not.toBe(b);
    expect(a.equals(b)).toBe(true);
  });
});
