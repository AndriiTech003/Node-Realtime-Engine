import { describe, expect, it } from "vitest";
import { bearer, safeEqual, signJwt, userFromClaims, verifyJwt } from "../../src/auth.js";

describe("JWT", () => {
  it("signs and verifies HS256 tokens", () => {
    const token = signJwt({ sub: "u_1", name: "Ann" }, "secret");
    const result = verifyJwt(token, "secret");
    expect(result.ok).toBe(true);
    if (result.ok) expect(result.claims).toMatchObject({ sub: "u_1", name: "Ann" });
  });

  it("rejects wrong secrets, tampering and expiry", () => {
    const token = signJwt({ sub: "u_1" }, "secret");
    expect(verifyJwt(token, "other")).toEqual({ ok: false, reason: "bad signature" });
    const [h, , s] = token.split(".");
    const forged = `${h}.${Buffer.from(JSON.stringify({ sub: "admin" })).toString("base64url")}.${s}`;
    expect(verifyJwt(forged, "secret").ok).toBe(false);
    const expired = signJwt({ sub: "u_1" }, "secret", -10);
    expect(verifyJwt(expired, "secret")).toEqual({ ok: false, reason: "expired" });
    expect(verifyJwt("a.b", "secret").ok).toBe(false);
    const none = `${Buffer.from(JSON.stringify({ alg: "none" })).toString("base64url")}.e30.`;
    expect(verifyJwt(none, "secret")).toEqual({ ok: false, reason: "unsupported alg" });
  });

  it("requires a subject", () => {
    const token = signJwt({ sub: "" }, "secret");
    expect(verifyJwt(token, "secret")).toEqual({ ok: false, reason: "missing sub" });
  });

  it("maps claims to a user", () => {
    const user = userFromClaims({ sub: "u_1", exp: 100 });
    expect(user).toMatchObject({ id: "u_1", name: "u_1", expiresAt: 100000 });
  });
});

describe("server key comparison", () => {
  it("compares in constant time over digests", () => {
    expect(safeEqual("abc", "abc")).toBe(true);
    expect(safeEqual("abc", "abd")).toBe(false);
    expect(safeEqual("abc", "abcd")).toBe(false);
  });

  it("parses bearer headers", () => {
    expect(bearer("Bearer x.y.z")).toBe("x.y.z");
    expect(bearer("basic abc")).toBeNull();
    expect(bearer(undefined)).toBeNull();
  });
});
