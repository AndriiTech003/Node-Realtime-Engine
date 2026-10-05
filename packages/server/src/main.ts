#!/usr/bin/env node
import { configFromEnv } from "./config.js";
import { RealtimeServer } from "./server.js";

const server = new RealtimeServer({ config: configFromEnv() });

let shuttingDown = false;

async function shutdown(signal: string): Promise<void> {
  if (shuttingDown) return;
  shuttingDown = true;
  server.log.info({ signal }, "shutdown requested");
  const hardExit = setTimeout(() => process.exit(1), server.config.drainCloseAfterMs + server.config.drainNotifyDelayMs + 10000);
  hardExit.unref();
  try {
    if (signal === "SIGTERM") await server.drain();
    else await server.stop();
    process.exit(0);
  } catch (error) {
    server.log.error({ err: error }, "shutdown failed");
    process.exit(1);
  }
}

process.on("SIGTERM", () => void shutdown("SIGTERM"));
process.on("SIGINT", () => void shutdown("SIGINT"));
process.on("unhandledRejection", (error) => server.log.error({ err: error }, "unhandled rejection"));

server.start().catch((error: unknown) => {
  server.log.fatal({ err: error }, "failed to start");
  process.exit(1);
});
