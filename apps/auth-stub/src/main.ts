#!/usr/bin/env node
import { configFromEnv, createAuthStub } from "./app.js";

const config = configFromEnv();
const server = createAuthStub(config);
server.listen(config.port, config.host, () => {
  console.log(JSON.stringify({ level: "info", msg: "auth-stub listening", port: config.port, realtime: config.realtimeHttpUrl }));
});

const stop = () => server.close(() => process.exit(0));
process.on("SIGTERM", stop);
process.on("SIGINT", stop);
