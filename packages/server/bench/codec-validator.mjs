import { writeFileSync, mkdirSync } from "node:fs";
import { deflateRawSync, constants } from "node:zlib";
import { z } from "zod";
import { jsonCodec, msgpackCodec, validateClientFrame } from "@ashamrai/realtime-protocol";

const ITER = Number(process.env.BENCH_ITER ?? 200000);

const frames = {
  chat: { t: "msg", ch: "room:42", seq: 1181, ts: 1759250000123, mid: "6c238e38-7be0-47f5-86ea-a7dda14f6954", from: "u_ann_1a2b3c", d: { text: "hello everyone, this is a typical chat message", name: "Ann", color: "#e4572e" } },
  cursor: { t: "eph", ch: "room:42", d: { k: "cursor", x: 0.4231, y: 0.7712 }, from: "u_ann_1a2b3c" },
  presence: { t: "pj", ch: "room:42", uid: "u_ann_1a2b3c", meta: { name: "Ann", color: "#e4572e" } },
  big: { t: "msg", ch: "room:42", seq: 99, ts: 1759250000123, mid: "x", from: "u", d: { items: Array.from({ length: 50 }, (_, i) => ({ id: i, title: `item ${i}`, price: i * 1.5, tags: ["a", "b"] })) } },
};

function bench(fn) {
  for (let i = 0; i < 5000; i++) fn();
  const t = performance.now();
  for (let i = 0; i < ITER; i++) fn();
  const ms = performance.now() - t;
  return Math.round((ITER / ms) * 1000);
}

const codecRows = [];
for (const [name, frame] of Object.entries(frames)) {
  const json = Buffer.from(jsonCodec.encode(frame));
  const mp = msgpackCodec.encode(frame);
  const deflated = deflateRawSync(json, { flush: constants.Z_SYNC_FLUSH });
  codecRows.push({
    frame: name,
    jsonBytes: json.length,
    msgpackBytes: mp.length,
    jsonDeflateBytes: deflated.length - 4,
    jsonEncodeOpsPerSec: bench(() => Buffer.from(jsonCodec.encode(frame))),
    msgpackEncodeOpsPerSec: bench(() => msgpackCodec.encode(frame)),
    jsonDecodeOpsPerSec: bench(() => jsonCodec.decode(json.toString("utf8"))),
    msgpackDecodeOpsPerSec: bench(() => msgpackCodec.decode(mp)),
  });
}

const zodFrame = z.discriminatedUnion("t", [
  z.object({ t: z.literal("sub"), id: z.number().int().min(0), ch: z.string().min(1).max(128).regex(/^[A-Za-z0-9_.:-]+$/), from: z.number().int().min(0).optional(), history: z.number().int().min(0).max(1000).optional(), presence: z.boolean().optional() }),
  z.object({ t: z.literal("unsub"), id: z.number().int().min(0), ch: z.string().min(1).max(128).regex(/^[A-Za-z0-9_.:-]+$/) }),
  z.object({ t: z.literal("pub"), id: z.number().int().min(0), ch: z.string().min(1).max(128).regex(/^[A-Za-z0-9_.:-]+$/), d: z.json(), cmid: z.string().min(1).max(64).regex(/^[A-Za-z0-9_-]+$/) }),
  z.object({ t: z.literal("eph"), ch: z.string().min(1).max(128).regex(/^[A-Za-z0-9_.:-]+$/), d: z.json() }),
  z.object({ t: z.literal("pres"), id: z.number().int().min(0), ch: z.string().min(1).max(128).regex(/^[A-Za-z0-9_.:-]+$/), meta: z.json().optional() }),
  z.object({ t: z.literal("ping"), ts: z.number() }),
]);

const inbound = {
  pub: { t: "pub", id: 7, ch: "room:42", cmid: "9b2c1f00-aa11-4f6e-8a51-cc0d1e2f3a4b", d: { text: "hello everyone", name: "Ann", color: "#e4572e" } },
  eph: { t: "eph", ch: "room:42", d: { k: "cursor", x: 0.42, y: 0.77 } },
  sub: { t: "sub", id: 3, ch: "room:42", from: 1180 },
};
const validatorRows = [];
for (const [name, frame] of Object.entries(inbound)) {
  if (!validateClientFrame(frame).ok || !zodFrame.safeParse(frame).success) throw new Error(`fixture ${name} invalid`);
  validatorRows.push({
    frame: name,
    ownValidatorOpsPerSec: bench(() => validateClientFrame(frame)),
    zodOpsPerSec: bench(() => zodFrame.safeParse(frame)),
  });
}
for (const r of validatorRows) r.speedup = Math.round((r.ownValidatorOpsPerSec / r.zodOpsPerSec) * 10) / 10;

console.table(codecRows);
console.table(validatorRows);
mkdirSync("results", { recursive: true });
writeFileSync(process.env.BENCH_OUT ?? "results/bench-codec-validator.json", JSON.stringify({ at: new Date().toISOString(), node: process.version, iterations: ITER, codec: codecRows, validator: validatorRows }, null, 2));
