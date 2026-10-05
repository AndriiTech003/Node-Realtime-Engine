#!/usr/bin/env node
const base = process.env.REALTIME_HTTP_URL ?? "http://127.0.0.1:4300";
const key = process.env.SERVER_API_KEY ?? "dev-server-key-change-me";
const messages = {
  "room:lobby": ["Welcome to Pulse Rooms 👋", "Open a second browser window to see presence and live cursors.", "Try the Network lab: go offline for 10 s while someone else writes."],
  "room:design": ["Share mockups and feedback here."],
  "room:random": ["Anything goes."],
};
for (const [ch, list] of Object.entries(messages)) {
  for (const [i, text] of list.entries()) {
    const res = await fetch(`${base}/v1/publish`, {
      method: "POST",
      headers: { "x-api-key": key, "content-type": "application/json" },
      body: JSON.stringify({ ch, cmid: `seed-${ch.replace(/[^a-z0-9]/gi, "-")}-${i}`, d: { text, name: "Pulse bot", color: "#29335c" } }),
    });
    if (!res.ok) {
      console.error(`seed failed for ${ch}: ${res.status} ${await res.text()}`);
      process.exit(1);
    }
    const body = await res.json();
    console.log(`${ch} seq ${body.seq}${body.dup ? " (already seeded)" : ""}`);
  }
}
