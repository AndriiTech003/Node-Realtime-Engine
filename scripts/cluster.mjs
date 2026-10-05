#!/usr/bin/env node
import { execFileSync, spawn } from "node:child_process";
import { cpSync, existsSync, mkdirSync, openSync, readFileSync, readdirSync, realpathSync, rmSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";

const root = new URL("..", import.meta.url).pathname;
const runDir = join(root, ".run");
const logDir = join(runDir, "logs");
const stateFile = join(runDir, "cluster.json");
mkdirSync(logDir, { recursive: true });

const defaults = {
  REDIS_URL: process.env.REDIS_URL ?? "redis://127.0.0.1:6379/3",
  REDIS_PREFIX: process.env.REDIS_PREFIX ?? "rt:",
  JWT_SECRET: process.env.JWT_SECRET ?? "dev-jwt-secret-change-me",
  SERVER_API_KEY: process.env.SERVER_API_KEY ?? "dev-server-key-change-me",
  ALLOWED_ORIGINS:
    process.env.ALLOWED_ORIGINS ??
    "http://localhost:4320,http://127.0.0.1:4320,http://localhost:4321,http://127.0.0.1:4321",
};

function readState() {
  if (!existsSync(stateFile)) return { procs: {} };
  return JSON.parse(readFileSync(stateFile, "utf8"));
}

function writeState(state) {
  writeFileSync(stateFile, JSON.stringify(state, null, 2));
}

function alive(pid) {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

function parseArgs(argv) {
  const flags = { env: {}, nodes: 3, haproxy: true, auth: true, demo: false, prometheus: false, grafana: false, nodeArgs: [], foreground: false };
  const rest = [];
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (arg === "--env") {
      const [k, ...v] = argv[++i].split("=");
      flags.env[k] = v.join("=");
    } else if (arg === "--nodes") flags.nodes = Number(argv[++i]);
    else if (arg === "--no-haproxy") flags.haproxy = false;
    else if (arg === "--no-auth") flags.auth = false;
    else if (arg === "--demo") flags.demo = true;
    else if (arg === "--prometheus") flags.prometheus = true;
    else if (arg === "--grafana") {
      flags.grafana = true;
      flags.prometheus = true;
    } else if (arg === "--node-arg") flags.nodeArgs.push(argv[++i]);
    else if (arg === "--foreground") flags.foreground = true;
    else rest.push(arg);
  }
  return { flags, rest };
}

function launch(name, command, args, env, cwd = root) {
  const out = openSync(join(logDir, `${name}.log`), "a");
  const child = spawn(command, args, {
    cwd,
    env: { ...process.env, ...env },
    detached: true,
    stdio: ["ignore", out, out],
  });
  child.unref();
  const state = readState();
  state.procs[name] = { pid: child.pid, command, args, env };
  writeState(state);
  return child.pid;
}

async function waitHttp(url, timeoutMs = 20000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      const res = await fetch(url);
      if (res.status === 200) return true;
    } catch {
      await new Promise((r) => setTimeout(r, 100));
      continue;
    }
    await new Promise((r) => setTimeout(r, 100));
  }
  throw new Error(`timeout waiting for ${url}`);
}

function nodeEnv(n, extra) {
  return {
    ...defaults,
    NODE_ID: `node-${n}`,
    PORT: String(4300 + n),
    HOST: "127.0.0.1",
    DRAIN_NOTIFY_DELAY_MS: "2500",
    LOG_LEVEL: "info",
    ...extra,
  };
}

function ensureBuilt() {
  const required = ["packages/server/dist/main.js", "apps/auth-stub/dist/main.js"];
  for (const file of required) {
    if (!existsSync(join(root, file))) {
      console.error(`missing ${file}; run pnpm build first`);
      process.exit(1);
    }
  }
}

async function startNode(n, extra = {}, nodeArgs = []) {
  const pid = launch(`node-${n}`, process.execPath, ["--expose-gc", ...nodeArgs, "packages/server/dist/main.js"], nodeEnv(n, extra));
  await waitHttp(`http://127.0.0.1:${4300 + n}/health/ready`);
  console.log(`node-${n} ready on :${4300 + n} (pid ${pid})`);
}

async function up(flags) {
  ensureBuilt();
  const state = readState();
  for (const [name, proc] of Object.entries(state.procs)) {
    if (alive(proc.pid)) {
      console.error(`${name} already running (pid ${proc.pid}); run down first`);
      process.exit(1);
    }
  }
  writeState({ procs: {}, flags });
  await Promise.all(Array.from({ length: flags.nodes }, (_, i) => startNode(i + 1, flags.env, flags.nodeArgs)));
  if (flags.haproxy) {
    const env = {
      HAPROXY_BIND: "127.0.0.1:4300",
      HAPROXY_STATS_BIND: "127.0.0.1:4399",
      NODE1_ADDR: "127.0.0.1:4301",
      NODE2_ADDR: "127.0.0.1:4302",
      NODE3_ADDR: "127.0.0.1:4303",
    };
    const pid = launch("haproxy", "haproxy", ["-db", "-f", "infra/haproxy.cfg"], env);
    await waitHttp("http://127.0.0.1:4300/health/ready");
    console.log(`haproxy ready on :4300, stats on :4399 (pid ${pid})`);
  }
  if (flags.auth) {
    const pid = launch("auth-stub", process.execPath, ["apps/auth-stub/dist/main.js"], {
      ...defaults,
      PORT: "4310",
      HOST: "127.0.0.1",
      REALTIME_HTTP_URL: flags.haproxy ? "http://127.0.0.1:4300" : "http://127.0.0.1:4301",
      PUBLIC_WS_URL: flags.haproxy ? "ws://localhost:4300/v1/connect" : "ws://localhost:4301/v1/connect",
    });
    await waitHttp("http://127.0.0.1:4310/health");
    console.log(`auth-stub ready on :4310 (pid ${pid})`);
  }
  if (flags.demo) {
    if (!existsSync(join(root, "apps/demo/dist/index.html"))) {
      console.error("missing apps/demo/dist; run pnpm build first");
      process.exit(1);
    }
    const pid = launch(
      "demo",
      process.execPath,
      ["node_modules/vite/bin/vite.js", "preview", "--port", "4321", "--strictPort", "--host", "127.0.0.1"],
      { AUTH_URL: "http://127.0.0.1:4310" },
      join(root, "apps/demo"),
    );
    await waitHttp("http://127.0.0.1:4321/");
    console.log(`demo ready on http://localhost:4321 (pid ${pid})`);
  }
  if (flags.prometheus) {
    const pid = launch("prometheus", "prometheus", [
      "--config.file=infra/prometheus.local.yml",
      "--web.listen-address=127.0.0.1:4390",
      `--storage.tsdb.path=${join(runDir, "prometheus")}`,
    ], {});
    await waitHttp("http://127.0.0.1:4390/-/ready");
    console.log(`prometheus ready on :4390 (pid ${pid})`);
  }
  if (flags.grafana) {
    const home = grafanaHome();
    const dataDir = join(runDir, "grafana");
    for (const sub of ["data", "logs", "plugins"]) mkdirSync(join(dataDir, sub), { recursive: true });
    const provisioning = join(dataDir, "provisioning");
    rmSync(provisioning, { recursive: true, force: true });
    cpSync(join(root, "infra/grafana/provisioning"), provisioning, { recursive: true });
    for (const sub of ["plugins", "alerting"]) mkdirSync(join(provisioning, sub), { recursive: true });
    installPrometheusPlugin(join(dataDir, "plugins"));
    const pid = launch(
      "grafana",
      "grafana",
      [
        "server",
        `--homepath=${home}`,
        `--config=${join(root, "infra/grafana/grafana.local.ini")}`,
        `cfg:default.paths.data=${join(dataDir, "data")}`,
        `cfg:default.paths.logs=${join(dataDir, "logs")}`,
        `cfg:default.paths.plugins=${join(dataDir, "plugins")}`,
        `cfg:default.paths.provisioning=${provisioning}`,
      ],
      { PROMETHEUS_URL: "http://127.0.0.1:4390", GRAFANA_DASHBOARDS_DIR: join(root, "infra/grafana/dashboards") },
    );
    await waitHttp("http://127.0.0.1:4391/api/health", 60000);
    console.log(`grafana ready on http://127.0.0.1:4391 (pid ${pid})`);
  }
}

function prometheusPluginCandidates() {
  const out = [];
  if (process.env.GRAFANA_PROMETHEUS_PLUGIN) out.push(process.env.GRAFANA_PROMETHEUS_PLUGIN);
  out.push(join(grafanaHome(), "public/app/plugins/datasource/prometheus"));
  out.push("/opt/homebrew/var/lib/grafana/plugins/prometheus", "/usr/local/var/lib/grafana/plugins/prometheus");
  const parent = dirname(root.replace(/\/$/, ""));
  for (const entry of readdirSync(parent, { withFileTypes: true })) {
    if (!entry.isDirectory()) continue;
    for (const rel of [".observability/grafana/plugins/prometheus", ".run/grafana/plugins/prometheus", ".run/grafana/data/plugins/prometheus"]) {
      out.push(join(parent, entry.name, rel));
    }
  }
  return out;
}

function installPrometheusPlugin(pluginsDir) {
  const target = join(pluginsDir, "prometheus");
  if (existsSync(join(target, "plugin.json"))) return;
  const source = prometheusPluginCandidates().find((dir) => existsSync(join(dir, "plugin.json")) && dir !== target);
  if (source === undefined) {
    console.error("no local copy of the Grafana Prometheus datasource plugin found; set GRAFANA_PROMETHEUS_PLUGIN (plugin downloads are disabled)");
    return;
  }
  if (source.includes("public/app/plugins")) return;
  cpSync(source, target, { recursive: true });
  console.log(`grafana: copied local Prometheus datasource plugin from ${source}`);
}

function grafanaHome() {
  if (process.env.GRAFANA_HOME) return process.env.GRAFANA_HOME;
  try {
    return join(execFileSync("brew", ["--prefix", "grafana"], { encoding: "utf8" }).trim(), "share/grafana");
  } catch {
    const bin = realpathSync(execFileSync("which", ["grafana"], { encoding: "utf8" }).trim());
    return join(dirname(dirname(bin)), "share/grafana");
  }
}

async function stopProc(name, signal = "SIGTERM", timeoutMs = 25000) {
  const state = readState();
  const proc = state.procs[name];
  if (proc === undefined) return;
  if (alive(proc.pid)) {
    try {
      process.kill(proc.pid, signal);
    } catch {
      return;
    }
    const deadline = Date.now() + timeoutMs;
    while (alive(proc.pid) && Date.now() < deadline) await new Promise((r) => setTimeout(r, 100));
    if (alive(proc.pid)) process.kill(proc.pid, "SIGKILL");
  }
  const next = readState();
  delete next.procs[name];
  writeState(next);
}

async function down() {
  const state = readState();
  const names = Object.keys(state.procs);
  const order = ["demo", "grafana", "prometheus", "auth-stub", "haproxy", ...names.filter((n) => n.startsWith("node-"))];
  for (const name of order) {
    if (!names.includes(name)) continue;
    await stopProc(name, name.startsWith("node-") ? "SIGINT" : "SIGTERM", 10000);
    console.log(`stopped ${name}`);
  }
}

async function main() {
  const [command, ...argv] = process.argv.slice(2);
  const { flags, rest } = parseArgs(argv);
  switch (command) {
    case "up":
      await up(flags);
      if (flags.foreground) {
        console.log("cluster running; Ctrl+C to stop");
        process.on("SIGINT", () => down().then(() => process.exit(0)));
        setInterval(() => undefined, 1 << 30);
      }
      return;
    case "down":
      await down();
      return;
    case "kill": {
      const name = `node-${rest[0]}`;
      const proc = readState().procs[name];
      if (proc === undefined) throw new Error(`${name} not running`);
      process.kill(proc.pid, "SIGKILL");
      const next = readState();
      delete next.procs[name];
      writeState(next);
      console.log(`killed ${name} (pid ${proc.pid})`);
      return;
    }
    case "drain": {
      await stopProc(`node-${rest[0]}`, "SIGTERM", 40000);
      console.log(`drained node-${rest[0]}`);
      return;
    }
    case "start-node": {
      const state = readState();
      await startNode(Number(rest[0]), state.flags?.env ?? {}, state.flags?.nodeArgs ?? []);
      return;
    }
    case "status": {
      const state = readState();
      for (const [name, proc] of Object.entries(state.procs)) console.log(`${name}\tpid ${proc.pid}\t${alive(proc.pid) ? "running" : "dead"}`);
      return;
    }
    default:
      console.log("usage: cluster.mjs up|down|kill <n>|drain <n>|start-node <n>|status [--env K=V] [--nodes N] [--demo] [--prometheus] [--grafana] [--node-arg ARG] [--no-haproxy]");
  }
}

main().catch((error) => {
  console.error(error.message);
  process.exit(1);
});
