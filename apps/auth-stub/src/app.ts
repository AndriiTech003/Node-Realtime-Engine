import { createHmac, randomBytes } from "node:crypto";
import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";

export interface AuthStubConfig {
  port: number;
  host: string;
  jwtSecret: string;
  jwtTtlSec: number;
  serverApiKey: string;
  realtimeHttpUrl: string;
  realtimeNodeUrls: string[];
  publicWsUrl: string;
  allowedOrigins: string[];
}

export function configFromEnv(env: Record<string, string | undefined> = process.env): AuthStubConfig {
  const list = (v: string | undefined, fallback: string[]) => (v === undefined || v === "" ? fallback : v.split(",").map((x) => x.trim()).filter(Boolean));
  return {
    port: Number(env["PORT"] ?? 4310),
    host: env["HOST"] ?? "0.0.0.0",
    jwtSecret: env["JWT_SECRET"] ?? "dev-jwt-secret-change-me",
    jwtTtlSec: Number(env["JWT_TTL_SEC"] ?? 8 * 3600),
    serverApiKey: env["SERVER_API_KEY"] ?? "dev-server-key-change-me",
    realtimeHttpUrl: env["REALTIME_HTTP_URL"] ?? "http://127.0.0.1:4300",
    realtimeNodeUrls: list(env["REALTIME_NODE_URLS"], ["http://127.0.0.1:4301", "http://127.0.0.1:4302", "http://127.0.0.1:4303"]),
    publicWsUrl: env["PUBLIC_WS_URL"] ?? "ws://localhost:4300/v1/connect",
    allowedOrigins: list(env["ALLOWED_ORIGINS"], ["http://localhost:4320", "http://127.0.0.1:4320", "http://localhost:4321", "http://127.0.0.1:4321"]),
  };
}

const COLORS = ["#e4572e", "#29335c", "#f3a712", "#669bbc", "#a8c686", "#8e44ad", "#16a085", "#d35400"];

export function signToken(claims: Record<string, unknown>, secret: string, ttlSec: number): string {
  const now = Math.floor(Date.now() / 1000);
  const header = Buffer.from(JSON.stringify({ alg: "HS256", typ: "JWT" })).toString("base64url");
  const payload = Buffer.from(JSON.stringify({ iat: now, exp: now + ttlSec, ...claims })).toString("base64url");
  const signature = createHmac("sha256", secret).update(`${header}.${payload}`).digest("base64url");
  return `${header}.${payload}.${signature}`;
}

export function slugify(name: string): string {
  const slug = name
    .toLowerCase()
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 20);
  return slug.length > 0 ? slug : "guest";
}

export function colorFor(id: string): string {
  let hash = 0;
  for (const ch of id) hash = (hash * 31 + ch.charCodeAt(0)) >>> 0;
  return COLORS[hash % COLORS.length] as string;
}

class HttpError extends Error {
  constructor(
    readonly status: number,
    message: string,
  ) {
    super(message);
  }
}

async function readJson(req: IncomingMessage): Promise<Record<string, unknown>> {
  const chunks: Buffer[] = [];
  let size = 0;
  for await (const chunk of req) {
    size += (chunk as Buffer).length;
    if (size > 16 * 1024) throw new HttpError(413, "body too large");
    chunks.push(chunk as Buffer);
  }
  if (chunks.length === 0) return {};
  try {
    const value = JSON.parse(Buffer.concat(chunks).toString("utf8")) as unknown;
    return typeof value === "object" && value !== null ? (value as Record<string, unknown>) : {};
  } catch {
    throw new HttpError(400, "invalid JSON");
  }
}

function bearer(req: IncomingMessage): string {
  const match = /^Bearer\s+(.+)$/i.exec(req.headers.authorization ?? "");
  if (match?.[1] === undefined) throw new HttpError(401, "missing bearer token");
  return match[1];
}

export function createAuthStub(config: AuthStubConfig): Server {
  const origins = new Set(config.allowedOrigins);

  const send = (req: IncomingMessage, res: ServerResponse, status: number, body: unknown) => {
    const origin = req.headers.origin;
    const headers: Record<string, string> = { "content-type": "application/json", "cache-control": "no-store" };
    if (origin !== undefined && origins.has(origin)) {
      headers["access-control-allow-origin"] = origin;
      headers["access-control-allow-headers"] = "authorization, content-type";
      headers["access-control-allow-methods"] = "GET, POST, OPTIONS";
      headers["vary"] = "Origin";
    }
    res.writeHead(status, headers);
    res.end(status === 204 ? undefined : JSON.stringify(body));
  };

  const handle = async (req: IncomingMessage, res: ServerResponse): Promise<void> => {
    const url = new URL(req.url ?? "/", "http://localhost");
    const route = `${req.method ?? "GET"} ${url.pathname}`;
    if (req.method === "OPTIONS") {
      send(req, res, 204, null);
      return;
    }
    switch (route) {
      case "GET /health":
        send(req, res, 200, { status: "ok" });
        return;
      case "GET /api/config":
        send(req, res, 200, { wsUrl: config.publicWsUrl });
        return;
      case "POST /api/session": {
        const body = await readJson(req);
        const name = typeof body["name"] === "string" ? body["name"].trim().slice(0, 32) : "";
        if (name.length === 0) throw new HttpError(400, "name is required");
        const id = `u_${slugify(name)}_${randomBytes(3).toString("hex")}`;
        const color = colorFor(id);
        const token = signToken({ sub: id, name, meta: { color } }, config.jwtSecret, config.jwtTtlSec);
        send(req, res, 200, { token, user: { id, name, color } });
        return;
      }
      case "POST /api/ticket": {
        const token = bearer(req);
        const upstream = await fetch(`${config.realtimeHttpUrl}/v1/tickets`, {
          method: "POST",
          headers: { authorization: `Bearer ${token}` },
        });
        if (upstream.status !== 200) throw new HttpError(upstream.status === 401 ? 401 : 502, `ticket request failed (${upstream.status})`);
        const data = (await upstream.json()) as { ticket: string; expiresIn: number };
        send(req, res, 200, { ticket: data.ticket, expiresIn: data.expiresIn, wsUrl: config.publicWsUrl });
        return;
      }
      case "GET /api/history": {
        const token = bearer(req);
        const ch = url.searchParams.get("ch") ?? "";
        const limit = url.searchParams.get("limit") ?? "50";
        const upstream = await fetch(`${config.realtimeHttpUrl}/v1/history?ch=${encodeURIComponent(ch)}&limit=${encodeURIComponent(limit)}`, {
          headers: { authorization: `Bearer ${token}` },
        });
        send(req, res, upstream.status, await upstream.json());
        return;
      }
      case "POST /api/simulate-slow": {
        bearer(req);
        const body = await readJson(req);
        const cid = typeof body["cid"] === "string" ? body["cid"] : "";
        const ms = typeof body["ms"] === "number" ? Math.min(30000, Math.max(100, Math.floor(body["ms"]))) : 5000;
        if (cid.length === 0) throw new HttpError(400, "cid is required");
        const results = await Promise.allSettled(
          config.realtimeNodeUrls.map((base) =>
            fetch(`${base}/admin/simulate-slow`, {
              method: "POST",
              headers: { "x-api-key": config.serverApiKey, "content-type": "application/json" },
              body: JSON.stringify({ cid, ms }),
            }).then((r) => r.status),
          ),
        );
        const applied = results.some((r) => r.status === "fulfilled" && r.value === 200);
        send(req, res, applied ? 200 : 404, { applied, ms });
        return;
      }
      default:
        send(req, res, 404, { error: "not found" });
    }
  };

  return createServer((req, res) => {
    handle(req, res).catch((error: unknown) => {
      const status = error instanceof HttpError ? error.status : 500;
      const message = error instanceof Error ? error.message : "internal error";
      if (!res.headersSent) send(req, res, status, { error: message });
      else res.destroy();
    });
  });
}
