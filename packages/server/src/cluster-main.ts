#!/usr/bin/env node
import cluster from "node:cluster";
import { configFromEnv } from "./config.js";
import { RealtimeServer } from "./server.js";

const workers = Number(process.env["CLUSTER_WORKERS"] ?? 3);

if (cluster.isPrimary) {
  cluster.schedulingPolicy = cluster.SCHED_RR;
  const base = process.env["NODE_ID"] ?? "cluster";
  for (let i = 1; i <= workers; i++) cluster.fork({ NODE_ID: `${base}-w${i}` });
  cluster.on("exit", (worker, code) => {
    console.log(JSON.stringify({ level: "warn", msg: "cluster worker exited", pid: worker.process.pid, code }));
  });
  const stop = (signal: NodeJS.Signals) => {
    for (const worker of Object.values(cluster.workers ?? {})) worker?.process.kill(signal);
    setTimeout(() => process.exit(0), 20000).unref();
    cluster.on("exit", () => {
      if (Object.keys(cluster.workers ?? {}).length === 0) process.exit(0);
    });
  };
  process.on("SIGTERM", () => stop("SIGTERM"));
  process.on("SIGINT", () => stop("SIGINT"));
} else {
  const server = new RealtimeServer({ config: configFromEnv() });
  process.on("SIGTERM", () => {
    void server.drain().then(() => process.exit(0));
  });
  process.on("SIGINT", () => {
    void server.stop().then(() => process.exit(0));
  });
  server.start().catch((error: unknown) => {
    server.log.fatal({ err: error }, "failed to start");
    process.exit(1);
  });
}
