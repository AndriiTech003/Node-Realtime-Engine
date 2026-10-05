import { fileURLToPath } from "node:url";
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

const authUrl = process.env["AUTH_URL"] ?? "http://127.0.0.1:4310";
const proxy = { "/api": { target: authUrl, changeOrigin: true } };

export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      "@ashamrai/realtime-client": fileURLToPath(new URL("../../packages/client/src/index.ts", import.meta.url)),
      "@ashamrai/realtime-protocol": fileURLToPath(new URL("../../packages/protocol/src/index.ts", import.meta.url)),
    },
  },
  server: { port: 4320, strictPort: true, host: "127.0.0.1", proxy },
  preview: { port: 4321, strictPort: true, host: "127.0.0.1", proxy },
  build: {
    outDir: "dist",
    sourcemap: true,
    rollupOptions: {
      input: {
        main: fileURLToPath(new URL("./index.html", import.meta.url)),
        harness: fileURLToPath(new URL("./client-test.html", import.meta.url)),
      },
    },
  },
});
