# @ashamrai/realtime-client

Browser and Node client for the Pulse realtime protocol (`pulse.v1.json` / `pulse.v1.msgpack`).

- Reconnects with exponential backoff and full jitter; honours `drain`, `4001` (fresh ticket), `4008` (immediate resume) and `4029` (backoff).
- Resumes every channel from its last `seq` on any node, drops duplicates, detects gaps and re-subscribes.
- Queues `publish()` calls while offline and retries them with the same `cmid`, so the server deduplicates.
- Presence by user, throttled ephemeral messages, app-level ping for browsers.

```ts
import { RealtimeClient } from "@ashamrai/realtime-client";

const client = new RealtimeClient({
  url: "wss://realtime.example.com/v1/connect",
  getTicket: async () => (await (await fetch("/api/ticket", { method: "POST" })).json()).ticket,
  codec: "msgpack",
});

const room = client.subscribe("room:42", { history: 50 });
room.on("message", (m) => console.log(m.seq, m.d, m.resumed));
room.on("presence", (members) => console.log(members));
room.on("reset", ({ seq }) => reloadFromHttp(seq));
await room.publish({ text: "hello" });
room.sendEphemeral({ x: 0.4, y: 0.7 });
```

In Node 18–21 pass `WebSocket` from the `ws` package; Node 22+ and browsers use the global `WebSocket`.
