import * as sdk from "../../../packages/client/dist/index.js";

declare global {
  interface Window {
    sdk: typeof sdk;
    harnessReady: boolean;
  }
}

window.sdk = sdk;
window.harnessReady = true;
