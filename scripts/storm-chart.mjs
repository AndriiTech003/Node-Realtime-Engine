#!/usr/bin/env node
import { readFileSync, writeFileSync, mkdirSync } from "node:fs";

const dir = process.argv[2] ?? "results";
const out = process.argv[3] ?? "docs/assets/reconnect-storm.svg";
const runs = [
  { file: `${dir}/reconnect-storm-no-jitter.json`, label: "no jitter (exponential backoff)", color: "#e4572e" },
  { file: `${dir}/reconnect-storm-jitter.json`, label: "full jitter", color: "#29335c" },
];

const bucketMs = 250;
const fromMs = -1000;
const toMs = 18000;
const series = runs.map((run) => {
  const data = JSON.parse(readFileSync(run.file, "utf8")).result;
  const buckets = new Map();
  for (const point of data.timeline) {
    const b = Math.floor(point.tMs / bucketMs) * bucketMs;
    const cur = buckets.get(b) ?? { opened: 0, failed: 0 };
    cur.opened += point.opened;
    cur.failed += point.failed;
    buckets.set(b, cur);
  }
  const points = [];
  for (let t = fromMs; t <= toMs; t += bucketMs) {
    const cur = buckets.get(t) ?? { opened: 0, failed: 0 };
    points.push({ t, perSec: (cur.opened * 1000) / bucketMs, failedPerSec: (cur.failed * 1000) / bucketMs });
  }
  return { ...run, points, summary: data };
});

const width = 760;
const height = 400;
const pad = { left: 64, right: 20, top: 48, bottom: 100 };
const maxY = Math.max(10, ...series.flatMap((s) => s.points.flatMap((p) => [p.perSec, p.failedPerSec])));
const niceMax = Math.ceil(maxY / 100) * 100;
const x = (t) => pad.left + ((t - fromMs) / (toMs - fromMs)) * (width - pad.left - pad.right);
const y = (v) => height - pad.bottom - (v / niceMax) * (height - pad.top - pad.bottom);

const parts = [];
parts.push(`<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ${width} ${height}" font-family="Inter, system-ui, sans-serif" font-size="12">`);
parts.push(`<rect width="${width}" height="${height}" fill="#ffffff"/>`);
parts.push(`<text x="${pad.left}" y="22" font-size="15" font-weight="700" fill="#1f2430">Reconnect storm after kill -9 of one node (${series[0].summary.dropped} clients dropped)</text>`);
parts.push(`<text x="${pad.left}" y="38" fill="#6b7280">successful WebSocket upgrades per second through HAProxy, ${bucketMs} ms buckets</text>`);
for (let i = 0; i <= 4; i++) {
  const v = (niceMax / 4) * i;
  parts.push(`<line x1="${pad.left}" x2="${width - pad.right}" y1="${y(v)}" y2="${y(v)}" stroke="#e5e3dd"/>`);
  parts.push(`<text x="${pad.left - 8}" y="${y(v) + 4}" text-anchor="end" fill="#6b7280">${Math.round(v)}</text>`);
}
for (let t = 0; t <= toMs; t += 2000) {
  parts.push(`<text x="${x(t)}" y="${height - pad.bottom + 18}" text-anchor="middle" fill="#6b7280">${t / 1000}s</text>`);
}
parts.push(`<line x1="${x(0)}" x2="${x(0)}" y1="${pad.top}" y2="${height - pad.bottom}" stroke="#1f2430" stroke-dasharray="4 3"/>`);
parts.push(`<text x="${x(0) + 4}" y="${pad.top + 10}" fill="#1f2430">kill</text>`);
parts.push(`<text x="${(pad.left + width - pad.right) / 2}" y="${height - pad.bottom + 36}" text-anchor="middle" fill="#6b7280">time since kill</text>`);
series.forEach((s, i) => {
  const d = s.points.map((p, k) => `${k === 0 ? "M" : "L"}${x(p.t).toFixed(1)},${y(p.perSec).toFixed(1)}`).join(" ");
  const f = s.points.map((p, k) => `${k === 0 ? "M" : "L"}${x(p.t).toFixed(1)},${y(p.failedPerSec).toFixed(1)}`).join(" ");
  parts.push(`<path d="${d}" fill="none" stroke="${s.color}" stroke-width="2"/>`);
  parts.push(`<path d="${f}" fill="none" stroke="${s.color}" stroke-width="1" stroke-dasharray="3 3" opacity="0.8"/>`);
  const ly = height - 34 + i * 18;
  const r = s.summary;
  parts.push(`<rect x="${pad.left}" y="${ly - 6}" width="14" height="3" fill="${s.color}"/>`);
  parts.push(`<text x="${pad.left + 20}" y="${ly}" fill="#1f2430">${s.label}: ${r.failedAttemptsAfterKill} failed attempts, 95% reconnected after ${r.reconnected95Seconds ?? "n/a"} s, all after ${r.recoverySeconds ?? "n/a"} s, ticket p99 ${Math.round(r.ticketLatencyMs.p99)} ms</text>`);
});
parts.push(`<text x="${width - pad.right}" y="${height - 52}" text-anchor="end" fill="#6b7280">solid: successful upgrades/s · dashed: failed attempts/s</text>`);
parts.push("</svg>");
mkdirSync(out.split("/").slice(0, -1).join("/"), { recursive: true });
writeFileSync(out, parts.join("\n"));
console.log(`written ${out}`);
