import { describe, expect, it } from "vitest";
import { colorFor, configFromEnv, signToken, slugify } from "../../src/app.js";
import { verifyJwt } from "@ashamrai/realtime-server";

describe("auth-stub helpers", () => {
  it("issues tokens the realtime server accepts", () => {
    const token = signToken({ sub: "u_ann_1", name: "Ann" }, "s", 60);
    const result = verifyJwt(token, "s");
    expect(result.ok && result.claims.name).toBe("Ann");
  });

  it("slugifies display names", () => {
    expect(slugify("Ann Lee!")).toBe("ann-lee");
    expect(slugify("   ")).toBe("guest");
    expect(slugify("Ünïcôdé")).toBe("unicode");
  });

  it("assigns a stable color per user id", () => {
    expect(colorFor("u_a")).toBe(colorFor("u_a"));
    expect(colorFor("u_a")).toMatch(/^#[0-9a-f]{6}$/);
  });

  it("reads config from env", () => {
    const config = configFromEnv({ PORT: "4399", REALTIME_NODE_URLS: "http://a,http://b" });
    expect(config.port).toBe(4399);
    expect(config.realtimeNodeUrls).toEqual(["http://a", "http://b"]);
  });
});
