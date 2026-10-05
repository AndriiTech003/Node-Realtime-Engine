import { defineConfig } from "vitest/config";
import { alias } from "./vitest.shared.js";

export default defineConfig({
  resolve: { alias },
  test: {
    include: ["packages/*/test/unit/**/*.test.ts", "apps/*/test/unit/**/*.test.ts"],
    environment: "node",
    reporters: ["default"],
  },
});
