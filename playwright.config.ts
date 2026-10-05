import { defineConfig, devices } from "@playwright/test";

export default defineConfig({
  timeout: 60000,
  expect: { timeout: 15000 },
  fullyParallel: false,
  workers: 1,
  reporter: [["list"]],
  use: {
    baseURL: "http://127.0.0.1:4343",
    trace: "retain-on-failure",
    ...devices["Desktop Chrome"],
  },
  webServer: {
    command: "node scripts/e2e-stack.mjs",
    url: "http://127.0.0.1:4343/",
    reuseExistingServer: false,
    timeout: 60000,
    stdout: "pipe",
    gracefulShutdown: { signal: "SIGTERM", timeout: 5000 },
  },
  projects: [
    { name: "client-browser", testDir: "e2e/client-browser" },
    { name: "demo-e2e", testDir: "e2e/demo" },
  ],
});
