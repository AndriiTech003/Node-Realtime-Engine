#!/usr/bin/env node
import { Redis } from "ioredis";

const prefix = process.argv[2];
if (prefix === undefined || prefix.length < 4) {
  console.error("usage: purge-prefix.mjs <prefix>");
  process.exit(1);
}
const redis = new Redis(process.env.REDIS_URL ?? "redis://127.0.0.1:6379/3");
let cursor = "0";
let removed = 0;
do {
  const [next, keys] = await redis.scan(cursor, "MATCH", `${prefix}*`, "COUNT", 1000);
  cursor = next;
  if (keys.length > 0) removed += await redis.unlink(...keys);
} while (cursor !== "0");
redis.disconnect();
console.log(`purged ${removed} keys with prefix ${prefix}`);
