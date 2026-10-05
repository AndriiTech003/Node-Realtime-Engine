import { writeFileSync, mkdirSync } from "node:fs";
import { Redis } from "ioredis";
import { PUBLISH_SCRIPT } from "../dist/index.js";

const PUBLISHERS = Number(process.env.BENCH_PUBLISHERS ?? 8);
const PER = Number(process.env.BENCH_PER_PUBLISHER ?? 500);
const url = "redis://127.0.0.1:6379/3";
const prefix = `rt:bench:atomic:${Date.now()}:`;

async function run(mode) {
  const ch = `room:${mode}`;
  const keys = { seq: `${prefix}ch:{${ch}}:seq`, log: `${prefix}ch:{${ch}}:log`, fan: `${prefix}fan:{${ch}}` };
  const sub = new Redis(url);
  const received = [];
  sub.on("smessage", (_c, m) => received.push(Number(m.split("|")[0])));
  await sub.ssubscribe(keys.fan);
  const publishers = Array.from({ length: PUBLISHERS }, () => new Redis(url));
  const started = performance.now();
  await Promise.all(
    publishers.map(async (r, p) => {
      for (let i = 0; i < PER; i++) {
        const payload = JSON.stringify({ mid: `${p}-${i}`, from: `p${p}`, d: i });
        const ts = Date.now();
        if (mode === "lua") {
          await r.eval(PUBLISH_SCRIPT, 3, keys.seq, keys.log, `${prefix}ch:{${ch}}:cmid:${p}-${i}`, payload, ts, keys.fan, 10000, 0, 300, `${p}-${i}`);
        } else {
          const seq = await r.incr(keys.seq);
          await r.xadd(keys.log, "MAXLEN", "~", 10000, `0-${seq}`, "p", payload, "ts", ts).catch(() => null);
          await r.spublish(keys.fan, `${seq}|${ts}|${payload}`);
        }
      }
    }),
  );
  const elapsed = performance.now() - started;
  await new Promise((r) => setTimeout(r, 300));
  let inversions = 0;
  for (let i = 1; i < received.length; i++) if (received[i] < received[i - 1]) inversions++;
  const total = PUBLISHERS * PER;
  const stream = await publishers[0].xlen(keys.log);
  for (const r of publishers) r.disconnect();
  sub.disconnect();
  return {
    mode,
    published: total,
    receivedOverPubSub: received.length,
    outOfOrderDeliveries: inversions,
    streamEntries: stream,
    xaddRejected: total - stream,
    publishesPerSec: Math.round(total / (elapsed / 1000)),
  };
}

const results = [await run("separate-commands"), await run("lua")];
console.table(results);
const cleanup = new Redis(url);
const keys = await cleanup.keys(`${prefix}*`);
if (keys.length > 0) await cleanup.del(...keys);
cleanup.disconnect();
mkdirSync("results", { recursive: true });
writeFileSync(process.env.BENCH_OUT ?? "results/bench-atomicity.json", JSON.stringify({ at: new Date().toISOString(), publishers: PUBLISHERS, perPublisher: PER, results }, null, 2));
