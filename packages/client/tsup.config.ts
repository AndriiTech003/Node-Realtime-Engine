import { defineConfig } from "tsup";

export default defineConfig({
  entry: ["src/index.ts"],
  format: ["esm", "cjs"],
  dts: { resolve: true },
  sourcemap: true,
  clean: true,
  target: "es2022",
  platform: "neutral",
  noExternal: ["@ashamrai/realtime-protocol"],
  external: ["msgpackr"],
  tsconfig: "tsconfig.build.json",
});
