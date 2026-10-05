#!/usr/bin/env node
import { spawn } from "node:child_process";
import { randomBytes } from "node:crypto";
import { join } from "node:path";
import { Redis } from "ioredis";

const root = new URL("..", import.meta.url).pathname;
const prefix = `rt:test:e2e-${randomBytes(3).toString("hex")}:`;
const redisUrl = process.env.TEST_REDIS_URL ?? "redis://127.0.0.1:6379/3";
const origins = "http://127.0.0.1:4343,http://localhost:4343";
const shared = { JWT_SECRET: "e2e-jwt-secret", SERVER_API_KEY: "e2e-server-key", REDIS_URL: redisUrl, REDIS_PREFIX: prefix };
const children = [];

function start(name, args, env, cwd = root) {
  const child = spawn(process.execPath, args, { cwd, env: { ...process.env, ...env }, stdio: ["ignore", "inherit", "inherit"] });
  child.on("exit", (code) => {
    if (!stopping) {
      console.error(`${name} exited with ${code}`);
      shutdown(1);
    }
  });
  children.push(child);
  return child;
}

async function waitHttp(url, timeoutMs = 30000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      const res = await fetch(url);
      if (res.ok) return;
    } catch {
      await new Promise((r) => setTimeout(r, 100));
      continue;
    }
    await new Promise((r) => setTimeout(r, 100));
  }
  throw new Error(`timeout waiting for ${url}`);
}

let stopping = false;

async function purge() {
  const redis = new Redis(redisUrl, { lazyConnect: true });
  await redis.connect();
  let cursor = "0";
  do {
    const [next, keys] = await redis.scan(cursor, "MATCH", `${prefix}*`, "COUNT", 1000);
    cursor = next;
    if (keys.length > 0) await redis.unlink(...keys);
  } while (cursor !== "0");
  await redis.quit();
}

async function shutdown(code = 0) {
  if (stopping) return;
  stopping = true;
  for (const child of children) child.kill("SIGINT");
  await new Promise((r) => setTimeout(r, 1500));
  for (const child of children) if (child.exitCode === null) child.kill("SIGKILL");
  await purge().catch(() => undefined);
  process.exit(code);
}

process.on("SIGTERM", () => void shutdown(0));
process.on("SIGINT", () => void shutdown(0));

start("node-e2e-1", ["packages/server/dist/main.js"], {
  ...shared,
  NODE_ID: "e2e-node-1",
  PORT: "4341",
  HOST: "127.0.0.1",
  ALLOWED_ORIGINS: origins,
  LOG_LEVEL: "warn",
});
await waitHttp("http://127.0.0.1:4341/health/ready");
start("auth-e2e", ["apps/auth-stub/dist/main.js"], {
  ...shared,
  PORT: "4342",
  HOST: "127.0.0.1",
  REALTIME_HTTP_URL: "http://127.0.0.1:4341",
  REALTIME_NODE_URLS: "http://127.0.0.1:4341",
  PUBLIC_WS_URL: "ws://127.0.0.1:4341/v1/connect",
  ALLOWED_ORIGINS: origins,
});
await waitHttp("http://127.0.0.1:4342/health");
start(
  "demo-e2e",
  ["node_modules/vite/bin/vite.js", "preview", "--port", "4343", "--strictPort", "--host", "127.0.0.1"],
  { AUTH_URL: "http://127.0.0.1:4342" },
  join(root, "apps/demo"),
);
await waitHttp("http://127.0.0.1:4343/");
console.log(`e2e stack ready (prefix ${prefix})`);
