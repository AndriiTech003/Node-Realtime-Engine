import { randomBytes } from "node:crypto";
import { Redis } from "ioredis";

interface StartedContainer {
  getConnectionUrl(): string;
  stop(): Promise<unknown>;
}

let container: StartedContainer | null = null;

async function purge(url: string, pattern: string): Promise<number> {
  const redis = new Redis(url, { lazyConnect: true });
  await redis.connect();
  let cursor = "0";
  let removed = 0;
  do {
    const [next, keys] = await redis.scan(cursor, "MATCH", pattern, "COUNT", 1000);
    cursor = next;
    if (keys.length > 0) removed += await redis.unlink(...keys);
  } while (cursor !== "0");
  await redis.quit();
  return removed;
}

export async function setup(): Promise<void> {
  if (process.env["TESTCONTAINERS"] === "1") {
    const moduleName = "@testcontainers/redis";
    const mod = (await import(moduleName)) as {
      RedisContainer: new (image: string) => { start(): Promise<StartedContainer> };
    };
    container = await new mod.RedisContainer("redis:8-alpine").start();
    process.env["TEST_REDIS_URL"] = `${container.getConnectionUrl()}/3`;
  }
  const url = process.env["TEST_REDIS_URL"] ?? "redis://127.0.0.1:6379/3";
  process.env["TEST_REDIS_URL"] = url;
  process.env["RT_TEST_PREFIX"] = `rt:test:${randomBytes(3).toString("hex")}:`;
  await purge(url, "rt:test:*");
}

export async function teardown(): Promise<void> {
  const url = process.env["TEST_REDIS_URL"] ?? "redis://127.0.0.1:6379/3";
  await purge(url, "rt:test:*");
  if (container !== null) await container.stop();
}
