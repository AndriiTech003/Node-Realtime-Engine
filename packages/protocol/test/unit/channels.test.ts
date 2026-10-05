import { describe, expect, it } from "vitest";
import { parseChannel } from "../../src/channels.js";
import { CloseCode, closeCodePolicy } from "../../src/constants.js";

describe("channels", () => {
  it("parses the three channel types", () => {
    const room = parseChannel("room:42");
    expect(room.ok && room.channel).toMatchObject({ type: "room", id: "42", presence: true, ephemeral: true, clientPublish: true });
    const user = parseChannel("user:u_7");
    expect(user.ok && user.channel).toMatchObject({ type: "user", id: "u_7", presence: false, clientPublish: false });
    const broadcast = parseChannel("broadcast:news");
    expect(broadcast.ok && broadcast.channel).toMatchObject({ type: "broadcast", clientPublish: false });
  });

  it("rejects unknown prefixes and invalid names", () => {
    expect(parseChannel("queue:1")).toEqual({ ok: false, reason: "unknown_type" });
    expect(parseChannel("room:")).toEqual({ ok: false, reason: "invalid" });
    expect(parseChannel("room")).toEqual({ ok: false, reason: "invalid" });
    expect(parseChannel("room:{x}")).toEqual({ ok: false, reason: "invalid" });
    expect(parseChannel(":x")).toEqual({ ok: false, reason: "invalid" });
  });
});

describe("close code policy", () => {
  it("maps close codes to client behaviour", () => {
    expect(closeCodePolicy(CloseCode.Normal)).toBe("none");
    expect(closeCodePolicy(CloseCode.GoingAway)).toBe("drain");
    expect(closeCodePolicy(CloseCode.MessageTooBig)).toBe("none");
    expect(closeCodePolicy(CloseCode.InvalidTicket)).toBe("new-ticket");
    expect(closeCodePolicy(CloseCode.Forbidden)).toBe("none");
    expect(closeCodePolicy(CloseCode.SlowConsumer)).toBe("immediate");
    expect(closeCodePolicy(CloseCode.RateLimited)).toBe("backoff");
    expect(closeCodePolicy(CloseCode.ProtocolViolation)).toBe("none");
    expect(closeCodePolicy(1006)).toBe("backoff");
  });
});
