import { defineConfig } from "vitest/config";
import { alias } from "./vitest.shared.js";

export default defineConfig({
  resolve: { alias },
  test: {
    include: ["packages/*/test/integration/**/*.test.ts", "apps/*/test/integration/**/*.test.ts"],
    environment: "node",
    fileParallelism: false,
    testTimeout: 60000,
    hookTimeout: 60000,
    globalSetup: ["./test-support/global-setup.ts"],
    reporters: ["default"],
  },
});
