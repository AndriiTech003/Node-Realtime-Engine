import type * as sdk from "@ashamrai/realtime-client";

declare global {
  interface Window {
    sdk: typeof sdk;
    harnessReady: boolean;
  }
}

export {};
