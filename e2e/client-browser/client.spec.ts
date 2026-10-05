import { expect, test, type Page } from "@playwright/test";

async function openHarness(page: Page): Promise<void> {
  await page.goto("/client-test.html");
  await page.waitForFunction(() => window.harnessReady === true);
  await page.evaluate(async () => {
    const config = (await (await fetch("/api/config")).json()) as { wsUrl: string };
    const w = window as unknown as Record<string, unknown>;
    w["makeClient"] = async (name: string, codec: "json" | "msgpack" = "json") => {
      const session = (await (
        await fetch("/api/session", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ name }) })
      ).json()) as { token: string; user: { id: string } };
      const client = new window.sdk.RealtimeClient({
        url: config.wsUrl,
        codec,
        reconnect: { baseMs: 50, maxMs: 300, jitter: "full" },
        getTicket: async () => {
          const res = await fetch("/api/ticket", { method: "POST", headers: { authorization: `Bearer ${session.token}` } });
          return ((await res.json()) as { ticket: string }).ticket;
        },
      });
      return { client, uid: session.user.id };
    };
    w["waitFor"] = (check: () => boolean, timeoutMs = 10000) =>
      new Promise<void>((resolve, reject) => {
        const started = Date.now();
        const tick = () => {
          if (check()) resolve();
          else if (Date.now() - started > timeoutMs) reject(new Error("waitFor timeout"));
          else setTimeout(tick, 20);
        };
        tick();
      });
  });
}

test.beforeEach(async ({ page }) => {
  await openHarness(page);
});

for (const codec of ["json", "msgpack"] as const) {
  test(`connects, subscribes and round-trips messages in a real browser (${codec})`, async ({ page }) => {
    const result = await page.evaluate(async (codecName) => {
      const w = window as unknown as Record<string, (...a: unknown[]) => Promise<unknown>>;
      const { client } = (await w["makeClient"]!(`browser-${codecName}`, codecName)) as { client: InstanceType<typeof window.sdk.RealtimeClient> };
      const room = client.subscribe(`room:browser-${codecName}-${Date.now()}`);
      const got: unknown[] = [];
      room.on("message", (m) => got.push(m.d));
      await new Promise((r) => room.once("subscribed", r));
      const acks = [];
      for (let i = 0; i < 5; i++) acks.push(await room.publish({ i, text: "héllo ✓" }));
      await w["waitFor"]!(() => got.length === 5);
      const out = { got, seqs: acks.map((a) => a.seq), state: client.state, codec: client.codecName };
      client.close();
      return out;
    }, codec);
    expect(result.state).toBe("open");
    expect(result.codec).toBe(codec);
    expect(result.seqs).toEqual([1, 2, 3, 4, 5]);
    expect(result.got).toEqual([0, 1, 2, 3, 4].map((i) => ({ i, text: "héllo ✓" })));
  });
}

test("resumes after the socket is killed and marks replayed messages", async ({ page }) => {
  const result = await page.evaluate(async () => {
    const w = window as unknown as Record<string, (...a: unknown[]) => Promise<unknown>>;
    type C = InstanceType<typeof window.sdk.RealtimeClient>;
    const ch = `room:browser-resume-${Date.now()}`;
    const { client: reader } = (await w["makeClient"]!("reader")) as { client: C };
    const { client: writer } = (await w["makeClient"]!("writer")) as { client: C };
    const r = reader.subscribe(ch);
    const wr = writer.subscribe(ch);
    const got: { seq: number; resumed: boolean }[] = [];
    r.on("message", (m) => got.push({ seq: m.seq, resumed: m.resumed }));
    await new Promise((res) => (r.state === "subscribed" ? res(null) : r.once("subscribed", res)));
    await new Promise((res) => (wr.state === "subscribed" ? res(null) : wr.once("subscribed", res)));
    await wr.publish({ n: 1 });
    await w["waitFor"]!(() => got.length === 1);
    reader.kill();
    for (let n = 2; n <= 6; n++) await wr.publish({ n });
    await w["waitFor"]!(() => got.length === 6);
    const stats = { ...reader.stats };
    reader.close();
    writer.close();
    return { got, stats };
  });
  expect(result.got.map((g) => g.seq)).toEqual([1, 2, 3, 4, 5, 6]);
  expect(result.got.slice(1).some((g) => g.resumed)).toBe(true);
  expect(result.stats.reconnects).toBeGreaterThanOrEqual(1);
  expect(result.stats.duplicates).toBe(0);
});

test("queues publishes while offline and flushes them once on reconnect", async ({ page }) => {
  const result = await page.evaluate(async () => {
    const w = window as unknown as Record<string, (...a: unknown[]) => Promise<unknown>>;
    type C = InstanceType<typeof window.sdk.RealtimeClient>;
    const ch = `room:browser-offline-${Date.now()}`;
    const { client } = (await w["makeClient"]!("offliner")) as { client: C };
    const { client: watcher } = (await w["makeClient"]!("watcher")) as { client: C };
    const room = client.subscribe(ch);
    const watch = watcher.subscribe(ch);
    const seen: unknown[] = [];
    watch.on("message", (m) => seen.push(m.d));
    await new Promise((res) => (room.state === "subscribed" ? res(null) : room.once("subscribed", res)));
    await new Promise((res) => (watch.state === "subscribed" ? res(null) : watch.once("subscribed", res)));
    client.disconnect();
    const pending = [room.publish("a"), room.publish("b"), room.publish("c")];
    const queued = client.stats.queued;
    client.connect();
    const acks = await Promise.all(pending);
    await w["waitFor"]!(() => seen.length === 3);
    await new Promise((r) => setTimeout(r, 200));
    client.close();
    watcher.close();
    return { queued, seqs: acks.map((a) => a.seq), seen };
  });
  expect(result.queued).toBe(3);
  expect(result.seqs).toEqual([1, 2, 3]);
  expect(result.seen).toEqual(["a", "b", "c"]);
});

test("tracks presence by user across two browser clients", async ({ page }) => {
  const result = await page.evaluate(async () => {
    const w = window as unknown as Record<string, (...a: unknown[]) => Promise<unknown>>;
    type C = InstanceType<typeof window.sdk.RealtimeClient>;
    const ch = `room:browser-presence-${Date.now()}`;
    const a = (await w["makeClient"]!("Ann")) as { client: C; uid: string };
    const b = (await w["makeClient"]!("Bob")) as { client: C; uid: string };
    const ra = a.client.subscribe(ch);
    await new Promise((res) => ra.once("subscribed", res));
    const rb = b.client.subscribe(ch);
    await w["waitFor"]!(() => ra.members.has(b.uid));
    await rb.setPresence({ name: "Bob", status: "away" });
    await w["waitFor"]!(() => (ra.members.get(b.uid) as { status?: string } | undefined)?.status === "away");
    const before = ra.memberList().length;
    b.client.close();
    await w["waitFor"]!(() => !ra.members.has(b.uid));
    const after = ra.memberList().length;
    a.client.close();
    return { before, after };
  });
  expect(result).toEqual({ before: 2, after: 1 });
});
