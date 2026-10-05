#!/usr/bin/env node
import { spawn, spawnSync } from "node:child_process";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";

const root = new URL("..", import.meta.url).pathname;
const grafana = process.env.GRAFANA_URL ?? "http://127.0.0.1:4391";
const prometheus = process.env.PROMETHEUS_URL ?? "http://127.0.0.1:4390";
const auth = `Basic ${Buffer.from(process.env.GRAFANA_AUTH ?? "admin:realtime-local").toString("base64")}`;
const dashboardUid = "realtime-engine";
const connections = Number(process.env.OBS_CONNECTIONS ?? 600);
const loadSeconds = Number(process.env.OBS_DURATION ?? 60);
const settleSeconds = Number(process.env.OBS_SETTLE ?? 35);
const screenshot = process.env.OBS_SCREENSHOT ?? join(root, "docs/assets/grafana-dashboard.png");
const reportFile = process.env.OBS_REPORT ?? join(root, "results/observability-check.json");
const mustBeNonZero = new Set([
  "Connections by node",
  "Messages out / s by kind",
  "Frames in / s by type",
  "Fan-out duration p99",
  "Event loop utilization",
  "Heap used",
  "Resident memory",
  "Redis command p99",
  "WebSocket upgrades / s",
  "HAProxy sessions per server",
]);

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function reachable(url) {
  try {
    const res = await fetch(url);
    return res.ok;
  } catch {
    return false;
  }
}

async function api(path, init = {}) {
  const res = await fetch(`${grafana}${path}`, {
    ...init,
    headers: { authorization: auth, "content-type": "application/json", ...(init.headers ?? {}) },
  });
  const body = await res.json();
  if (!res.ok) throw new Error(`${path}: ${res.status} ${JSON.stringify(body)}`);
  return body;
}

function cluster(...args) {
  const result = spawnSync(process.execPath, [join(root, "scripts/cluster.mjs"), ...args], { cwd: root, stdio: "inherit" });
  if (result.status !== 0) throw new Error(`cluster.mjs ${args.join(" ")} failed`);
}

function startLoad() {
  const out = join(root, ".run/observability");
  mkdirSync(out, { recursive: true });
  const child = spawn(
    process.execPath,
    [
      join(root, "apps/loadgen/dist/cli.js"),
      "fanout-big-room",
      "--via",
      "haproxy",
      "--connections",
      String(connections),
      "--duration",
      String(loadSeconds),
      "--workers",
      "2",
      "--label",
      "observability-check",
      "--out",
      out,
    ],
    { cwd: root, stdio: ["ignore", "pipe", "pipe"] },
  );
  let log = "";
  child.stdout.on("data", (d) => (log += d));
  child.stderr.on("data", (d) => (log += d));
  const done = new Promise((resolve) => child.on("exit", (code) => resolve({ code, log })));
  return { child, done };
}

function summarize(frames) {
  let series = 0;
  let points = 0;
  let max = Number.NEGATIVE_INFINITY;
  let last = null;
  for (const frame of frames) {
    const values = frame.data?.values ?? [];
    for (let i = 1; i < values.length; i++) {
      const column = values[i] ?? [];
      const nums = column.filter((v) => typeof v === "number" && Number.isFinite(v));
      if (nums.length === 0) continue;
      series++;
      points += nums.length;
      max = Math.max(max, ...nums);
      last = nums[nums.length - 1];
    }
  }
  return { series, points, max: series > 0 ? max : null, last };
}

async function queryPanel(panel) {
  const queries = (panel.targets ?? []).map((t, i) => ({
    refId: t.refId ?? String.fromCharCode(65 + i),
    datasource: panel.datasource ?? t.datasource,
    expr: t.expr,
    range: true,
    instant: false,
    intervalMs: 5000,
    maxDataPoints: 300,
  }));
  const body = await api("/api/ds/query", {
    method: "POST",
    body: JSON.stringify({ queries, from: "now-3m", to: "now" }),
  });
  const frames = [];
  const errors = [];
  for (const q of queries) {
    const result = body.results?.[q.refId];
    if (result?.error) errors.push(result.error);
    frames.push(...(result?.frames ?? []));
  }
  return { exprs: queries.map((q) => q.expr), errors, ...summarize(frames) };
}

async function takeScreenshot() {
  const { chromium } = await import("@playwright/test");
  const browser = await chromium.launch();
  try {
    const context = await browser.newContext({
      viewport: { width: 1600, height: 2600 },
      deviceScaleFactor: 1,
      httpCredentials: { username: "admin", password: (process.env.GRAFANA_AUTH ?? "admin:realtime-local").split(":")[1] },
    });
    const page = await context.newPage();
    await page.goto(`${grafana}/d/${dashboardUid}/node-realtime-engine?orgId=1&from=now-2m&to=now&kiosk&theme=dark`, {
      waitUntil: "networkidle",
    });
    await page.waitForFunction(() => document.querySelectorAll("canvas").length >= 16, null, { timeout: 30000 });
    await page.waitForTimeout(6000);
    await page.screenshot({ path: screenshot });
  } finally {
    await browser.close();
  }
}

async function main() {
  let started = false;
  if (!(await reachable(`${grafana}/api/health`))) {
    console.log("grafana not running; starting cluster with --grafana");
    cluster("up", "--grafana");
    started = true;
  }
  const failures = [];
  try {
    const health = await api("/api/health");
    console.log(`grafana ${health.version} database ${health.database}`);
    let active = [];
    const targetDeadline = Date.now() + 30000;
    while (Date.now() < targetDeadline) {
      const targets = await (await fetch(`${prometheus}/api/v1/targets`)).json();
      active = targets.data.activeTargets.map((t) => ({ url: t.scrapeUrl, health: t.health }));
      if (active.length > 0 && active.every((t) => t.health === "up")) break;
      await sleep(1000);
    }
    for (const t of active) console.log(`prometheus target ${t.url} ${t.health}`);
    if (active.filter((t) => t.url.includes(":430") && t.health === "up").length !== 3) failures.push("not all three nodes are scraped");
    const search = await api(`/api/search?query=${encodeURIComponent("Node Realtime Engine")}`);
    const hit = search.find((s) => s.uid === dashboardUid);
    if (hit === undefined) throw new Error("dashboard not found via /api/search");
    const { dashboard, meta } = await api(`/api/dashboards/uid/${dashboardUid}`);
    const fileDashboard = JSON.parse(readFileSync(join(root, "infra/grafana/dashboards/realtime.json"), "utf8"));
    console.log(`dashboard "${dashboard.title}" in folder "${meta.folderTitle}", provisioned=${meta.provisioned}, ${dashboard.panels.length} panels`);
    if (!meta.provisioned) failures.push("dashboard is not provisioned");
    if (dashboard.panels.length !== fileDashboard.panels.length) failures.push("panel count differs from infra/grafana/dashboards/realtime.json");
    const ds = await api("/api/datasources/uid/prometheus");
    console.log(`datasource ${ds.name} -> ${ds.url}`);

    const load = startLoad();
    console.log(`load: fanout-big-room via HAProxy, ${connections} connections, ${loadSeconds}s; checking after ${settleSeconds}s`);
    await sleep(settleSeconds * 1000);
    const panels = [];
    for (const panel of dashboard.panels) {
      const result = await queryPanel(panel);
      const nonZero = result.max !== null && result.max > 0;
      const ok = result.errors.length === 0 && result.series > 0 && (!mustBeNonZero.has(panel.title) || nonZero);
      if (!ok) failures.push(`panel "${panel.title}": series=${result.series} max=${result.max} errors=${result.errors.join(";")}`);
      panels.push({ id: panel.id, title: panel.title, ok, nonZero, ...result });
      console.log(
        `${ok ? "ok  " : "FAIL"} ${panel.title.padEnd(36)} series=${String(result.series).padStart(2)} points=${String(result.points).padStart(4)} max=${result.max === null ? "-" : Number(result.max.toPrecision(4))}`,
      );
    }
    await takeScreenshot();
    console.log(`screenshot ${screenshot}`);
    const { code, log } = await load.done;
    const lastLine = log.trim().split("\n").slice(-3).join(" | ");
    console.log(`load exited ${code}: ${lastLine}`);
    if (code !== 0) failures.push(`loadgen exited with ${code}`);
    const report = {
      at: new Date().toISOString(),
      grafana: { version: health.version, dashboard: dashboard.title, folder: meta.folderTitle, provisioned: meta.provisioned },
      prometheusTargets: active,
      load: { scenario: "fanout-big-room", via: "haproxy", connections, seconds: loadSeconds },
      panels,
      failures,
    };
    mkdirSync(join(root, "results"), { recursive: true });
    writeFileSync(reportFile, `${JSON.stringify(report, null, 2)}\n`);
  } finally {
    if (started) cluster("down");
  }
  if (failures.length > 0) {
    console.error(`observability check failed:\n- ${failures.join("\n- ")}`);
    process.exit(1);
  }
  console.log("observability check passed");
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
