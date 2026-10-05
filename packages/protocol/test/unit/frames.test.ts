import { describe, expect, it } from "vitest";
import { validateClientFrame, validateServerFrame } from "../../src/frames.js";

describe("client frames", () => {
  it("accepts every documented client frame", () => {
    const frames = [
      { t: "sub", id: 1, ch: "room:42", from: 1180 },
      { t: "sub", id: 2, ch: "room:42", history: 20 },
      { t: "sub", id: 3, ch: "room:42", presence: false },
      { t: "unsub", id: 4, ch: "room:42" },
      { t: "pub", id: 5, ch: "room:42", cmid: "9b2c-11", d: { text: "hello" } },
      { t: "eph", ch: "room:42", d: { x: 1, y: 2 } },
      { t: "pres", id: 6, ch: "room:42", meta: { status: "away" } },
      { t: "pres", id: 7, ch: "room:42" },
      { t: "ping", ts: 1759250000123 },
    ];
    for (const frame of frames) expect(validateClientFrame(frame)).toEqual({ ok: true, value: frame });
  });

  it("rejects malformed client frames", () => {
    const bad = [
      null,
      "sub",
      { t: "nope" },
      { t: "sub", id: -1, ch: "room:1" },
      { t: "sub", id: 1, ch: "" },
      { t: "sub", id: 1, ch: "room:{1}" },
      { t: "sub", id: 1, ch: "room:1", from: -5 },
      { t: "sub", id: 1, ch: "room:1", history: 5000 },
      { t: "pub", id: 1, ch: "room:1", d: { a: 1 } },
      { t: "pub", id: 1, ch: "room:1", cmid: "has space", d: 1 },
      { t: "pub", id: 1, ch: "room:1", cmid: "x" },
      { t: "eph", ch: "room:1" },
      { t: "ping" },
      { t: "sub", id: 1.5, ch: "room:1" },
      { t: "sub", id: 1, ch: "x".repeat(129) },
    ];
    for (const frame of bad) expect(validateClientFrame(frame).ok).toBe(false);
  });
});

describe("server frames", () => {
  it("classifies frames as valid, unknown or invalid", () => {
    expect(validateServerFrame({ t: "msg", ch: "room:1", seq: 3, mid: "m", d: { a: 1 }, ts: 1, from: "u" }).kind).toBe("frame");
    expect(validateServerFrame({ t: "hello", cid: "c", node: "n", hb: 25000, v: 1 }).kind).toBe("frame");
    expect(validateServerFrame({ t: "ok", id: 1, seq: 0, presence: [{ uid: "a" }], pn: 1 }).kind).toBe("frame");
    expect(validateServerFrame({ t: "future-frame", x: 1 }).kind).toBe("unknown");
    expect(validateServerFrame({ t: "msg", ch: "room:1" }).kind).toBe("invalid");
    expect(validateServerFrame(42).kind).toBe("invalid");
  });

  it("ignores unknown fields for forward compatibility", () => {
    expect(validateServerFrame({ t: "pong", ts: 5, newField: true }).kind).toBe("frame");
  });
});
