import { fileURLToPath } from "node:url";

const here = (p: string) => fileURLToPath(new URL(p, import.meta.url));

export const alias = {
  "@ashamrai/realtime-protocol": here("./packages/protocol/src/index.ts"),
  "@ashamrai/realtime-server": here("./packages/server/src/index.ts"),
  "@ashamrai/realtime-client": here("./packages/client/src/index.ts"),
};
