#!/usr/bin/env node
import { chromium } from "@playwright/test";

const base = process.env.DEMO_URL ?? "http://127.0.0.1:4321";
const out = process.argv[2] ?? "docs/assets/pulse-rooms.png";
const browser = await chromium.launch();

async function join(name) {
  const context = await browser.newContext({ viewport: { width: 1360, height: 820 }, deviceScaleFactor: 2 });
  const page = await context.newPage();
  await page.goto(base);
  await page.getByTestId("login-name").fill(name);
  await page.getByTestId("login-submit").click();
  await page.getByTestId("conn-state").and(page.locator('[data-state="open"]')).waitFor();
  return page;
}

const ann = await join("Ann Lee");
const bob = await join("Bob Stone");
const cleo = await join("Cleo Park");
await bob.getByTestId("composer-input").fill("Morning! Is the new build live?");
await bob.getByTestId("composer-send").click();
await cleo.getByTestId("composer-input").fill("Yes, deployed 5 minutes ago 🚀");
await cleo.getByTestId("composer-send").click();
await ann.getByTestId("lab-offline").click();
await bob.getByTestId("composer-input").fill("Ann, can you check the dashboard?");
await bob.getByTestId("composer-send").click();
await cleo.getByTestId("composer-input").fill("Latency looks great on node-2");
await cleo.getByTestId("composer-send").click();
await bob.getByTestId("composer-input").fill("p99 is under 10 ms");
await bob.getByTestId("composer-send").click();
await ann.getByTestId("conn-state").and(ann.locator('[data-state="open"]')).waitFor({ timeout: 20000 });
await ann.waitForTimeout(500);
const box = await bob.locator(".chat-body").boundingBox();
if (box !== null) {
  await bob.mouse.move(box.x + box.width * 0.62, box.y + box.height * 0.55, { steps: 4 });
  await bob.mouse.move(box.x + box.width * 0.66, box.y + box.height * 0.58, { steps: 4 });
}
const box2 = await cleo.locator(".chat-body").boundingBox();
if (box2 !== null) await cleo.mouse.move(box2.x + box2.width * 0.3, box2.y + box2.height * 0.75, { steps: 4 });
await bob.getByTestId("composer-input").pressSequentially("typing", { delay: 30 });
await ann.waitForTimeout(400);
await ann.screenshot({ path: out });
await browser.close();
console.log(`written ${out}`);
