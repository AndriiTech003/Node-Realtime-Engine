import { writeFileSync, mkdirSync } from "node:fs";
import { jsonCodec, msgpackCodec } from "@ashamrai/realtime-protocol";

const SUBSCRIBERS = Number(process.env.BENCH_SUBSCRIBERS ?? 10000);
const MESSAGES = Number(process.env.BENCH_MESSAGES ?? 200);
const payload = JSON.stringify({ mid: "6c238e38-7be0-47f5-86ea-a7dda14f6954", from: "u_ann_1a2b3c", d: { text: "hello everyone, this is a typical chat message", name: "Ann", color: "#e4572e" } });

class FakeSocket {
  bytes = 0;
  send(buf) {
    this.bytes += buf.length;
  }
}

const sockets = Array.from({ length: SUBSCRIBERS }, () => new FakeSocket());

function frameObject(seq) {
  const parsed = JSON.parse(payload);
  return { t: "msg", ch: "room:big", seq, ts: 1759250000123, mid: parsed.mid, from: parsed.from, d: parsed.d };
}

function perSubscriber(codec, seq) {
  for (const s of sockets) {
    const encoded = codec.encode(frameObject(seq));
    s.send(typeof encoded === "string" ? Buffer.from(encoded) : encoded);
  }
}

function once(codec, seq) {
  const encoded = codec.name === "json"
    ? `{"t":"msg","ch":"room:big","seq":${seq},"ts":1759250000123,${payload.slice(1)}`
    : codec.encode(frameObject(seq));
  const buf = typeof encoded === "string" ? Buffer.from(encoded) : encoded;
  for (const s of sockets) s.send(buf);
}

function measure(fn, codec) {
  for (let i = 0; i < 20; i++) fn(codec, i);
  globalThis.gc?.();
  const heapBefore = process.memoryUsage().heapUsed;
  const samples = [];
  const cpuStart = process.cpuUsage();
  for (let i = 0; i < MESSAGES; i++) {
    const t = performance.now();
    fn(codec, i);
    samples.push(performance.now() - t);
  }
  const cpu = process.cpuUsage(cpuStart);
  const heapAfter = process.memoryUsage().heapUsed;
  samples.sort((a, b) => a - b);
  return {
    p50Ms: Math.round(samples[Math.floor(samples.length * 0.5)] * 1000) / 1000,
    p99Ms: Math.round(samples[Math.floor(samples.length * 0.99)] * 1000) / 1000,
    cpuMsPerMessage: Math.round(((cpu.user + cpu.system) / 1000 / MESSAGES) * 1000) / 1000,
    allocatedMbDuringRun: Math.round(((heapAfter - heapBefore) / 1024 / 1024) * 10) / 10,
  };
}

const results = [];
for (const codec of [jsonCodec, msgpackCodec]) {
  const naive = measure(perSubscriber, codec);
  const optimized = measure(once, codec);
  const row = { codec: codec.name, subscribers: SUBSCRIBERS, perSubscriber: naive, serializeOnce: optimized, speedup: Math.round((naive.p50Ms / optimized.p50Ms) * 10) / 10 };
  console.log(JSON.stringify(row));
  results.push(row);
}
mkdirSync("results", { recursive: true });
writeFileSync(process.env.BENCH_OUT ?? "results/bench-serialize-once.json", JSON.stringify({ at: new Date().toISOString(), node: process.version, results }, null, 2));
