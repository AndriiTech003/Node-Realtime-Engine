import { createHash, createHmac, timingSafeEqual } from "node:crypto";

export interface UserClaims {
  sub: string;
  name?: string;
  exp?: number;
  iat?: number;
  [key: string]: unknown;
}

export interface AuthUser {
  id: string;
  name: string;
  claims: UserClaims;
  expiresAt: number | null;
}

function b64url(input: Buffer | string): string {
  return Buffer.from(input).toString("base64url");
}

export function signJwt(claims: UserClaims, secret: string, ttlSec = 3600): string {
  const now = Math.floor(Date.now() / 1000);
  const header = b64url(JSON.stringify({ alg: "HS256", typ: "JWT" }));
  const payload = b64url(JSON.stringify({ iat: now, exp: now + ttlSec, ...claims }));
  const signature = createHmac("sha256", secret).update(`${header}.${payload}`).digest("base64url");
  return `${header}.${payload}.${signature}`;
}

export type JwtResult = { ok: true; claims: UserClaims } | { ok: false; reason: string };

export function verifyJwt(token: string, secret: string, nowSec = Math.floor(Date.now() / 1000)): JwtResult {
  const parts = token.split(".");
  if (parts.length !== 3) return { ok: false, reason: "malformed" };
  const [header, payload, signature] = parts as [string, string, string];
  let headerJson: { alg?: unknown };
  try {
    headerJson = JSON.parse(Buffer.from(header, "base64url").toString("utf8")) as { alg?: unknown };
  } catch {
    return { ok: false, reason: "malformed" };
  }
  if (headerJson.alg !== "HS256") return { ok: false, reason: "unsupported alg" };
  const expected = createHmac("sha256", secret).update(`${header}.${payload}`).digest();
  const actual = Buffer.from(signature, "base64url");
  if (actual.length !== expected.length || !timingSafeEqual(actual, expected)) return { ok: false, reason: "bad signature" };
  let claims: UserClaims;
  try {
    claims = JSON.parse(Buffer.from(payload, "base64url").toString("utf8")) as UserClaims;
  } catch {
    return { ok: false, reason: "malformed" };
  }
  if (typeof claims.sub !== "string" || claims.sub.length === 0 || claims.sub.length > 64) {
    return { ok: false, reason: "missing sub" };
  }
  if (typeof claims.exp === "number" && claims.exp < nowSec) return { ok: false, reason: "expired" };
  return { ok: true, claims };
}

export function safeEqual(a: string, b: string): boolean {
  const ha = createHash("sha256").update(a).digest();
  const hb = createHash("sha256").update(b).digest();
  return timingSafeEqual(ha, hb) && a.length === b.length;
}

export function bearer(header: string | undefined): string | null {
  if (header === undefined) return null;
  const match = /^Bearer\s+(.+)$/i.exec(header);
  return match?.[1]?.trim() ?? null;
}

export function userFromClaims(claims: UserClaims): AuthUser {
  return {
    id: claims.sub,
    name: typeof claims.name === "string" ? claims.name : claims.sub,
    claims,
    expiresAt: typeof claims.exp === "number" ? claims.exp * 1000 : null,
  };
}
