import { describe, expect, it } from "vitest";
import { Keys } from "../../src/keys.js";
import { configFromEnv, resolveConfig } from "../../src/config.js";

describe("Keys", () => {
  it("puts every channel key in the same hash slot via a hash tag", () => {
    const keys = new Keys("rt:");
    expect(keys.seq("room:42")).toBe("rt:ch:{room:42}:seq");
    expect(keys.log("room:42")).toBe("rt:ch:{room:42}:log");
    expect(keys.cmid("room:42", "abc")).toBe("rt:ch:{room:42}:cmid:abc");
    expect(keys.fan("room:42")).toBe("rt:fan:{room:42}");
    expect(keys.pres("room:42")).toBe("rt:pres:{room:42}");
    expect(keys.presExp("room:42")).toBe("rt:pres:{room:42}:exp");
    expect(keys.channelFromFan("rt:fan:{room:42}")).toBe("room:42");
    expect(keys.channelFromFan("other")).toBeNull();
  });

  it("keeps a user's connection counter in one slot and indexes users per node", () => {
    const keys = new Keys("rt:");
    expect(keys.userConns("u-1")).toBe("rt:user:{u-1}:conns");
    expect(keys.nodeUsers("node-2")).toBe("rt:node:node-2:users");
  });

  it("refuses prefixes that would break the hash tag", () => {
    expect(() => new Keys("{rt}:")).toThrow();
  });
});

describe("config", () => {
  it("merges nested overrides", () => {
    const config = resolveConfig({ backpressure: { highBytes: 10 }, rate: { pubPerSec: 1 } });
    expect(config.backpressure.highBytes).toBe(10);
    expect(config.backpressure.hardBytes).toBe(4 * 1024 * 1024);
    expect(config.rate.pubPerSec).toBe(1);
    expect(config.rate.pubBurst).toBe(40);
  });

  it("reads environment variables", () => {
    const config = configFromEnv({
      PORT: "4302",
      NODE_ID: "node-2",
      ALLOWED_ORIGINS: "http://a, http://b",
      PERMESSAGE_DEFLATE: "true",
      HEARTBEAT_MODE: "per-connection",
      BP_HIGH_BYTES: "2048",
      FANOUT_SERIALIZE_ONCE: "false",
    });
    expect(config).toMatchObject({ port: 4302, nodeId: "node-2", allowedOrigins: ["http://a", "http://b"], perMessageDeflate: true, heartbeatMode: "per-connection", fanoutSerializeOnce: false });
    expect(config.backpressure.highBytes).toBe(2048);
    expect(() => configFromEnv({ PORT: "x" })).toThrow();
  });
});
