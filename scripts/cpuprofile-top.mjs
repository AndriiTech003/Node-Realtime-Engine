#!/usr/bin/env node
import { readFileSync } from "node:fs";

const file = process.argv[2];
const top = Number(process.argv[3] ?? 25);
if (file === undefined) {
  console.error("usage: cpuprofile-top.mjs <file.cpuprofile> [top]");
  process.exit(1);
}

const profile = JSON.parse(readFileSync(file, "utf8"));
const byId = new Map(profile.nodes.map((n) => [n.id, n]));
const selfUs = new Map();
for (let i = 0; i < profile.samples.length; i++) {
  const id = profile.samples[i];
  const delta = profile.timeDeltas[i + 1] ?? 0;
  selfUs.set(id, (selfUs.get(id) ?? 0) + delta);
}

function shortUrl(url) {
  if (url === "") return "";
  const nm = url.lastIndexOf("node_modules/");
  if (nm >= 0) return url.slice(nm + "node_modules/".length).replace(/^\.pnpm\/[^/]+\/node_modules\//, "");
  const pkg = url.indexOf("/packages/");
  if (pkg >= 0) return url.slice(pkg + 1);
  return url.replace(/^file:\/\//, "");
}

function category(node) {
  const { functionName, url } = node.callFrame;
  if (functionName === "(idle)") return "idle";
  if (functionName === "(garbage collector)") return "gc";
  if (functionName === "(program)") return "program (native, not attributed)";
  if (url.includes("/packages/server/")) return "engine (packages/server)";
  if (url.includes("/packages/protocol/")) return "protocol";
  if (url.includes("node_modules/ws/") || url.includes("/ws@")) return "ws";
  if (url.includes("ioredis") || url.includes("redis-parser") || url.includes("redis-errors")) return "ioredis";
  if (url.includes("prom-client")) return "prom-client";
  if (url.includes("pino") || url.includes("sonic-boom")) return "pino";
  if (url.startsWith("node:")) return "node core (JS)";
  if (url === "") return "native / builtins";
  return "other";
}

const fns = new Map();
const cats = new Map();
let total = 0;
for (const [id, us] of selfUs) {
  const node = byId.get(id);
  const { functionName, url, lineNumber } = node.callFrame;
  const key = `${functionName || "(anonymous)"} ${shortUrl(url)}${url ? `:${lineNumber + 1}` : ""}`;
  fns.set(key, (fns.get(key) ?? 0) + us);
  const cat = category(node);
  cats.set(cat, (cats.get(cat) ?? 0) + us);
  total += us;
}

const idle = cats.get("idle") ?? 0;
const busy = total - idle;
const ms = (us) => (us / 1000).toFixed(1);
const pct = (us, of) => ((100 * us) / of).toFixed(1);
console.log(`profile ${file}`);
console.log(`wall ${ms(total)} ms, idle ${ms(idle)} ms (${pct(idle, total)} %), busy ${ms(busy)} ms`);
console.log("\nself time by category (% of busy)");
for (const [cat, us] of [...cats].filter(([c]) => c !== "idle").sort((a, b) => b[1] - a[1])) {
  console.log(`${pct(us, busy).padStart(6)} %  ${ms(us).padStart(9)} ms  ${cat}`);
}
console.log(`\ntop ${top} functions by self time (% of busy)`);
for (const [key, us] of [...fns].filter(([k]) => !k.startsWith("(idle)")).sort((a, b) => b[1] - a[1]).slice(0, top)) {
  console.log(`${pct(us, busy).padStart(6)} %  ${ms(us).padStart(9)} ms  ${key}`);
}
