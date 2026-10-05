#!/usr/bin/env node
import { execFileSync } from "node:child_process";
import WebSocket from "ws";
import { Redis } from "ioredis";
import { RealtimeClient } from "../packages/client/dist/index.js";

const AUTH = process.env.AUTH_URL ?? "http://127.0.0.1:4310";
const HAPROXY_WS = "ws://127.0.0.1:4300/v1/connect";
const room = `room:smoke-${Date.now()}`;
const redis = new Redis(process.env.REDIS_URL ?? "redis://127.0.0.1:6379/3");
const prefix = process.env.REDIS_PREFIX ?? "rt:";
const steps = [];

function step(name) {
  steps.push(name);
  console.log(`✓ ${name}`);
}

function fail(message) {
  console.error(`✗ ${message}`);
  process.exit(1);
}

async function waitFor(check, label, timeoutMs = 15000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await check()) return;
    await new Promise((r) => setTimeout(r, 50));
  }
  fail(`timeout: ${label}`);
}

async function session(name) {
  const res = await fetch(`${AUTH}/api/session`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ name }) });
  if (!res.ok) fail(`session for ${name}: ${res.status}`);
  return res.json();
}

function makeClient(s, wsFor = () => HAPROXY_WS) {
  let attempt = 0;
  class RoutedSocket extends WebSocket {
    constructor(url, protocols) {
      const target = wsFor(attempt++);
      super(url.replace(HAPROXY_WS, target), protocols);
    }
  }
  return new RealtimeClient({
    url: HAPROXY_WS,
    WebSocket: RoutedSocket,
    reconnect: { baseMs: 200, maxMs: 2000, jitter: "full" },
    getTicket: async () => {
      const res = await fetch(`${AUTH}/api/ticket`, { method: "POST", headers: { authorization: `Bearer ${s.token}` } });
      if (!res.ok) throw new Error(`ticket ${res.status}`);
      return (await res.json()).ticket;
    },
  });
}

async function rawConnect(s, url) {
  const res = await fetch(`${AUTH}/api/ticket`, { method: "POST", headers: { authorization: `Bearer ${s.token}` } });
  if (!res.ok) fail(`ticket ${res.status}`);
  const { ticket } = await res.json();
  const ws = new WebSocket(`${url}?ticket=${ticket}`, "pulse.v1.json");
  return new Promise((resolve, reject) => {
    const result = { ws, node: null, code: null };
    ws.on("message", (raw) => {
      const frame = JSON.parse(raw.toString());
      if (frame.t === "hello") {
        result.node = frame.node;
        resolve(result);
      }
    });
    ws.on("close", (code) => {
      result.code = code;
      resolve(result);
    });
    ws.on("error", reject);
  });
}

async function userCounts(uid) {
  const raw = await redis.hgetall(`${prefix}user:{${uid}}:conns`);
  return Object.fromEntries(Object.entries(raw).map(([k, v]) => [k, Number(v)]));
}

function subscribed(channel) {
  return waitFor(() => channel.state === "subscribed", `subscribe ${channel.name}`);
}

const ann = await session("Ann");
const bob = await session("Bob");
const carl = await session("Carl");

const annClient = makeClient(ann);
const annRoom = annClient.subscribe(room);
await subscribed(annRoom);
step(`Ann connected through HAProxy to ${annClient.node}`);

const bobClient = makeClient(bob);
const bobRoom = bobClient.subscribe(room);
const bobGot = [];
bobRoom.on("message", (m) => bobGot.push(m));
await subscribed(bobRoom);
step(`Bob connected through HAProxy to ${bobClient.node}`);

const victim = bobClient.node;
const victimPort = 4300 + Number(String(victim).replace("node-", ""));
const carlClient = makeClient(carl, () => `ws://127.0.0.1:${victimPort}/v1/connect`);
const carlRoom = carlClient.subscribe(room);
await subscribed(carlRoom);
if (carlClient.node !== victim) fail(`Carl expected on ${victim}, got ${carlClient.node}`);
step(`Carl connected directly to ${victim}`);

const dana = await session("Dana");
const danaPinned = await rawConnect(dana, `ws://127.0.0.1:${victimPort}/v1/connect`);
const danaRest = [];
for (let i = 0; i < 9; i++) danaRest.push(await rawConnect(dana, HAPROXY_WS));
const danaExtra = await rawConnect(dana, HAPROXY_WS);
await waitFor(() => danaExtra.code !== null, "11th connection of Dana is closed");
if (danaExtra.code !== 4029) fail(`Dana's 11th connection closed with ${danaExtra.code}, expected 4029`);
if ([danaPinned, ...danaRest].some((c) => c.node === null || c.code !== null)) fail("Dana's first 10 connections should stay open");
const danaNodes = new Set([danaPinned, ...danaRest].map((c) => c.node));
const danaTotal = Object.values(await userCounts(dana.user.id)).reduce((a, b) => a + b, 0);
if (danaTotal !== 10) fail(`Dana's cluster-wide counter is ${danaTotal}, expected 10`);
step(`per-user limit: Dana's 10 connections on ${danaNodes.size} nodes are counted cluster-wide, the 11th got 4029`);
for (const c of danaRest) c.ws.close();
await waitFor(async () => JSON.stringify(await userCounts(dana.user.id)) === JSON.stringify({ [victim]: 1 }), "Dana's counter drops to the pinned connection");

await waitFor(() => annRoom.members.has(bob.user.id) && annRoom.members.has(carl.user.id), "Ann sees Bob and Carl in presence");
step("presence: Ann sees Bob and Carl");

for (let i = 1; i <= 3; i++) await annRoom.publish({ text: `before ${i}`, name: "Ann" });
await waitFor(() => bobGot.length === 3, "Bob receives 3 messages");
step("publish: Bob received seq 1..3 in order");

carlClient.close();
const nodeNumber = String(victim).replace("node-", "");
execFileSync(process.execPath, ["scripts/cluster.mjs", "kill", nodeNumber], { stdio: "inherit" });
step(`killed ${victim} with SIGKILL`);

for (let i = 4; i <= 8; i++) await annRoom.publish({ text: `during outage ${i}`, name: "Ann" });
step("Ann published seq 4..8 while Bob was disconnected");

await waitFor(() => bobGot.length === 8, "Bob resumes and receives 8 messages", 30000);
const seqs = bobGot.map((m) => m.seq);
if (JSON.stringify(seqs) !== JSON.stringify([1, 2, 3, 4, 5, 6, 7, 8])) fail(`Bob sequence wrong: ${seqs}`);
if (bobClient.node === victim) fail("Bob reconnected to the dead node");
if (bobClient.stats.duplicates !== 0 || bobClient.stats.gaps !== 0) fail(`Bob stats ${JSON.stringify(bobClient.stats)}`);
step(`resume: Bob reconnected to ${bobClient.node} and received seq 4..8 (${bobGot.filter((m) => m.resumed).length} replayed), no gaps, no duplicates`);

await waitFor(() => !annRoom.members.has(carl.user.id), "Carl removed from presence after his node died", 30000);
await waitFor(() => annRoom.members.has(bob.user.id), "Bob still present after reconnect");
step("presence: Carl (on the killed node) was expired by a surviving node; Bob is still present");

await waitFor(async () => (await userCounts(dana.user.id))[victim] === undefined, "Dana's connection on the killed node is released", 30000);
step(`per-user limit: Dana's connection on the killed ${victim} was released by a surviving node`);

const history = await fetch(`${AUTH}/api/history?ch=${encodeURIComponent(room)}&limit=10`, { headers: { authorization: `Bearer ${ann.token}` } });
const body = await history.json();
if (body.seq !== 8 || body.messages.length !== 8) fail(`history mismatch ${JSON.stringify(body).slice(0, 200)}`);
step("history over HTTP has 8 messages");

annClient.close();
bobClient.close();
redis.disconnect();
console.log(`smoke passed: ${steps.length} checks`);
process.exit(0);
