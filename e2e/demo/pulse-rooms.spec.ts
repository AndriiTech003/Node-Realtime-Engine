import { expect, test, type Browser, type Page } from "@playwright/test";

async function join(browser: Browser, name: string): Promise<Page> {
  const context = await browser.newContext();
  const page = await context.newPage();
  await page.goto("/");
  await page.getByTestId("login-name").fill(name);
  await page.getByTestId("login-submit").click();
  await expect(page.getByTestId("conn-state")).toHaveAttribute("data-state", "open");
  return page;
}

async function openRoom(page: Page, room: string): Promise<void> {
  await page.locator(".custom-room input").fill(room);
  await page.locator(".custom-room input").press("Enter");
  await expect(page.locator(".chat-header h2")).toHaveText(`# ${room}`);
}

async function say(page: Page, text: string): Promise<void> {
  await page.getByTestId("composer-input").fill(text);
  await page.getByTestId("composer-send").click();
}

test("two browsers chat and see each other in presence", async ({ browser }) => {
  const room = `chat-${Date.now()}`;
  const ann = await join(browser, "Ann");
  const bob = await join(browser, "Bob");
  await openRoom(ann, room);
  await openRoom(bob, room);
  await expect(ann.getByTestId("presence-member")).toHaveCount(2);
  await expect(bob.getByTestId("presence-member")).toHaveCount(2);
  await expect(ann.locator('[data-testid="presence-member"][data-name="Bob"]')).toBeVisible();

  await say(ann, "hello from Ann");
  await expect(bob.getByTestId("message-item").filter({ hasText: "hello from Ann" })).toBeVisible();
  await say(bob, "hi Ann, Bob here");
  await expect(ann.getByTestId("message-item").filter({ hasText: "hi Ann, Bob here" })).toBeVisible();
  await expect(ann.getByTestId("message-item")).toHaveCount(2);
  await expect(ann.getByTestId("message-item").nth(0)).toHaveAttribute("data-seq", "1");
  await expect(ann.getByTestId("message-item").nth(1)).toHaveAttribute("data-seq", "2");

  await bob.getByTestId("composer-input").pressSequentially("typing something", { delay: 20 });
  await expect(ann.getByTestId("typing-indicator")).toContainText("Bob is typing");

  const box = await bob.locator(".chat-body").boundingBox();
  if (box === null) throw new Error("no chat body");
  await bob.mouse.move(box.x + box.width * 0.5, box.y + box.height * 0.5);
  await bob.mouse.move(box.x + box.width * 0.6, box.y + box.height * 0.4, { steps: 5 });
  await expect(ann.getByTestId("remote-cursor")).toBeVisible();

  await bob.context().close();
  await expect(ann.getByTestId("presence-member")).toHaveCount(1);
  await ann.context().close();
});

test("going offline for 10 s queues nothing lost: resume fills the gap and highlights it", async ({ browser }) => {
  const room = `resume-${Date.now()}`;
  const ann = await join(browser, "Ann");
  const bob = await join(browser, "Bob");
  await openRoom(ann, room);
  await openRoom(bob, room);
  await say(bob, "before offline");
  await expect(ann.getByTestId("message-item")).toHaveCount(1);

  await ann.getByTestId("lab-offline").click();
  await expect(ann.getByTestId("conn-state")).toHaveAttribute("data-state", "closed");
  for (let i = 1; i <= 5; i++) await say(bob, `missed ${i}`);
  await expect(bob.getByTestId("message-item")).toHaveCount(6);
  await say(ann, "written while offline");
  await expect(ann.getByTestId("stat-queued")).toHaveText("1");
  await expect(ann.getByTestId("message-item")).toHaveCount(1);

  await expect(ann.getByTestId("conn-state")).toHaveAttribute("data-state", "open", { timeout: 20000 });
  await expect(ann.getByTestId("message-item")).toHaveCount(7);
  const seqs = await ann.getByTestId("message-item").evaluateAll((els) => els.map((e) => Number(e.getAttribute("data-seq"))));
  expect(seqs).toEqual([1, 2, 3, 4, 5, 6, 7]);
  for (const seq of [2, 3, 4, 5, 6]) {
    await expect(ann.locator(`[data-testid="message-item"][data-seq="${seq}"]`)).toHaveAttribute("data-resumed", "true");
  }
  await expect(ann.locator('[data-testid="message-item"][data-seq="1"]')).toHaveAttribute("data-resumed", "false");
  await expect(ann.locator('[data-testid="seq-feed-item"][data-resumed="true"]').first()).toBeVisible();
  await expect(ann.getByTestId("stat-queued")).toHaveText("0");
  await expect(bob.getByTestId("message-item").filter({ hasText: "written while offline" })).toBeVisible();
  await expect(Number(await ann.getByTestId("stat-resumed").textContent())).toBeGreaterThanOrEqual(5);
  await ann.context().close();
  await bob.context().close();
});

test("kill connection reconnects automatically and keeps the history gapless", async ({ browser }) => {
  const room = `kill-${Date.now()}`;
  const ann = await join(browser, "Ann");
  const bob = await join(browser, "Bob");
  await openRoom(ann, room);
  await openRoom(bob, room);
  await say(bob, "one");
  await expect(ann.getByTestId("message-item")).toHaveCount(1);
  await ann.getByTestId("lab-kill").click();
  await say(bob, "two");
  await say(bob, "three");
  await expect(ann.getByTestId("message-item")).toHaveCount(3);
  await expect(ann.getByTestId("conn-state")).toHaveAttribute("data-state", "open");
  await expect(ann.getByTestId("stat-reconnects")).not.toHaveText("0");
  await ann.context().close();
  await bob.context().close();
});

test("simulated slow client keeps receiving messages after the stall", async ({ browser }) => {
  const room = `slow-${Date.now()}`;
  const ann = await join(browser, "Ann");
  const bob = await join(browser, "Bob");
  await openRoom(ann, room);
  await openRoom(bob, room);
  await ann.getByTestId("lab-slow").click();
  await say(bob, "during stall");
  await expect(ann.getByTestId("message-item").filter({ hasText: "during stall" })).toBeVisible({ timeout: 15000 });
  await ann.context().close();
  await bob.context().close();
});
