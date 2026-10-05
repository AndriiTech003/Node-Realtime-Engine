# What I learned building a WebSocket engine in plain Node.js

Every number below was measured with the scripts in this repository: §1–§9 on 2026-10-01, and §10–§12 plus the per-user limit on 2026-10-05. Raw JSON is in [`results/`](../results), the summary table is [`results/RESULTS.md`](../results/RESULTS.md), and `scripts/bench-all.sh` reruns everything.

**Environment, honestly.** One Apple Silicon laptop (8 CPU cores, 8 GB RAM, macOS 27), Node 26.7, Redis 8.10 on localhost (db 3), HAProxy 3.4 from Homebrew. The three server nodes, Redis, HAProxy and the load generator all run on the **same machine** and share its cores with a browser, an IDE, a game engine and another build agent that were running at the same time. There is no network: everything is loopback. Treat absolute numbers as "what a busy laptop does", and the before/after pairs as the interesting part. Pairs were run back to back with identical parameters; where a pair is still within noise, I say so.

Load sizes were chosen so that the OS limits of this machine are not hit (see §9): `kern.maxfilesperproc` is 10 240, `kern.maxfiles` is 30 720 for the whole system, and every loopback connection costs two descriptors.

---

## 1. What one WebSocket connection costs

`packages/server/bench/memory-per-connection.mjs` starts a server in a child process with `--expose-gc`, opens 4 000 connections, forces GC twice and reads `process.memoryUsage()` (`results/bench-memory.json`):

| Server variant (4 000 idle connections) | Heap / conn | RSS / conn |
|---|---|---|
| Bare `ws` server, `noServer`, nothing else | 2.8 KB | 6.9 KB |
| Full engine (ticket auth, `Connection`, 3 token buckets, registries) | 5.1 KB | 17.7 KB |
| + one subscription (`user:{id}`, local registry + `SSUBSCRIBE`) | 6.0 KB | 21.2 KB |
| + per-connection heartbeat timers instead of the shared one | 5.5 KB | 17.8 KB |
| + `permessage-deflate` enabled | 6.4 KB | **73.2 KB** (21 KB "external": zlib) |

The cluster-level `idle` scenario agrees: 9 000 connections over three nodes, each subscribed to its own `user:` channel, grew the nodes' heap by **5.5 KB per connection** (slope between 3 000 and 9 000 connections after forced GC). Connecting all 9 000 took 33.7 s wall time at a deliberate 1 000/s ramp, upgrade p99 10.7 ms, with 0 errors. Idle CPU with the default 25 s ping interval was 1.2 % per node at 3 000 connections each.

Where the memory goes: half of the heap is `ws` itself (socket wrapper, sender/receiver, event listeners). The engine adds the `Connection` object, its token buckets and the `subs` map, a pino child logger, and the closures for `message`/`close`/`pong`. RSS is dominated by the kernel-side and libuv socket structures, which is why RSS per connection is three times the heap figure. RSS on macOS is noisy (the allocator returns pages lazily), so I trust the heap slope more.

## 2. Serialize once

Without it, a fan-out to N local subscribers calls `JSON.stringify` N times, and `ws.send(string)` converts the string to a `Buffer` N more times. With it, the message becomes one `Buffer` per codec, and for JSON the frame is spliced around the payload stored in Redis without parsing it at all (`packages/server/src/outbound.ts`).

Micro-benchmark (`bench/serialize-once.mjs`, 10 000 fake sockets, chat-sized frame): encoding cost per message dropped from **7.8 ms to 0.009 ms** for JSON and from 10.0 ms to 0.01 ms for MessagePack. That isolates encoding; real sends cost more.

The real system (`fanout-big-room`, 6 000 subscribers on 3 nodes, 5 msg/s, `FANOUT_SERIALIZE_ONCE=false` vs default, `results/fanout-serialize-*.json`):

| | per subscriber | serialize once |
|---|---|---|
| Delivery latency p50 / p99 | 2 725 ms / 5 558 ms | **25 ms / 140 ms** |
| Event loop delay p99 (max over the run) | 736 ms | 24 ms |
| Event loop utilisation | 0.93 | 0.14 |
| CPU per node | 21.9 % | 9.9 % |
| Messages actually delivered per second | 26 095 (falling behind) | 29 987 |

At 2 000 subscribers per node, re-encoding per subscriber was enough to saturate the event loop: the node could not keep up with 5 msg/s and latency grew without bound. Serialize-once is not an optimisation here; it is the difference between working and not working.

## 3. One shared heartbeat timer vs a timer per connection

`bench/memory-per-connection.mjs` with `BENCH_HB_MS=1000` (ping every second to make the cost visible), 4 000 connections, 20 s (`results/bench-heartbeat.json`):

| | shared `setInterval` | `setInterval` + `setTimeout` per connection |
|---|---|---|
| CPU while idle | **25 ms/s** | 131 ms/s (5.2×) |
| Heap per connection | 5.1 KB | 6.2 KB |

With the default 25 s interval the difference shrinks proportionally, but it never goes away: 4 000 timers mean 4 000 entries in libuv's timer heap, a `Timeout` object each, and a separate callback invocation per tick instead of one loop over an array. One timer that walks the connection map also gives a single place to check JWT expiry (`4001`) and to send pings only when due, so pings are naturally spread instead of firing in lockstep.

## 4. Chunking a big fan-out with `setImmediate`

A single node with 6 000 subscribers in one room (all clients on node-1), 5 msg/s, plus 20 small "probe" rooms with their own publishers on the same node. Probe latency is the interesting number: it shows what one big room does to everyone else (`results/chunking-*-single-node.json`, `FANOUT_CHUNK_THRESHOLD=0` vs default 1000/500):

| | no chunking | chunks of 500 + `setImmediate` |
|---|---|---|
| Probe rooms latency p50 / p99 | 0.7 ms / **77 ms** | 0.8 ms / **28 ms** |
| Big room latency p50 / p99 | 26 ms / 84 ms | 25 ms / 41 ms |
| Event loop delay p99 | 75 ms | **5 ms** |
| CPU | 22 % | 19 % |

I expected the big room to pay for chunking with higher latency. It did not: p99 for the big room also improved, because the per-channel queue lets Redis messages, pings and other rooms interleave instead of piling up behind one 12 ms loop. Under heavy overload (20 msg/s with 1 KB payloads, `*-overload.json`) the trade-off appears: the big room's p50 went from 3.0 s to 9.1 s, but probe rooms went from 3.1 s / 6.9 s (p50/p99) to **17 ms / 33 ms** and event loop delay from 4.4 s to 12 ms. Chunking protects the rest of the node from one hot room; it does not create capacity.

## 5. JSON vs MessagePack

Sizes and single-thread throughput (`bench/codec-validator.mjs`, msgpackr with native extension):

| Frame | JSON bytes | MessagePack bytes | JSON encode / decode ops/s | MessagePack encode / decode ops/s |
|---|---|---|---|---|
| chat message | 216 | 180 (−17 %) | 3.2 M / 1.4 M | 1.8 M / 1.4 M |
| cursor (ephemeral) | 89 | 74 (−17 %) | 5.1 M / 1.8 M | 3.5 M / 3.6 M |
| presence join | 86 | 67 (−22 %) | 5.7 M / 1.8 M | 3.0 M / 3.8 M |
| 50-item payload | 2 914 | 2 154 (−26 %) | 204 k / 68 k | 86 k / 70 k |

`JSON.stringify` is faster to encode than msgpackr, MessagePack is faster to decode small frames. With serialize-once the server encodes once per message, so encoding speed barely matters on the server; decoding happens on the client.

End to end (`fanout-big-room`, 6 000 subscribers, `--codec msgpack` vs JSON in the same session): latency p50 22 ms vs 25 ms, p99 41 ms vs 140 ms, CPU per node 8.7 % vs 9.9 %. The p99 difference is mostly noise from the shared machine (the default JSON run in the first batch had p99 61 ms). Conclusion: MessagePack saves 17–26 % of bandwidth and is worth offering as a subprotocol; it is not a CPU win for this workload. JSON stays the default because it is debuggable in browser devtools.

## 6. permessage-deflate: why it is off

Per connection, enabling it added **55 KB of RSS** (73 KB vs 18 KB, two zlib streams per socket, §1). Under load (`fanout-big-room` with `PERMESSAGE_DEFLATE=true` and clients requesting it):

| | deflate off | deflate on |
|---|---|---|
| CPU per node | 9.9 % | **68.7 %** |
| Latency p50 / p99 | 25 ms / 140 ms | 767 ms / 15 876 ms |
| Peak RSS of a node | 136 MB | 499 MB |
| Delivered msgs/s (target 30 000) | 29 987 | 19 542 |

Deflate compresses per connection, so it destroys serialize-once: the same message is compressed 2 000 times per node. For small messages it also barely helps: a 216-byte chat frame deflates to 165 bytes in isolation. Context takeover would compress repeated keys better, at the price of keeping the zlib window per connection forever. The flag exists (`PERMESSAGE_DEFLATE`), but for chat/cursor traffic MessagePack gets most of the bandwidth win without the CPU and memory bill.

## 7. Reconnect storm: jitter vs no jitter

4 590 connections through HAProxy (4 500 subscribers in rooms of 50 plus 90 publishers at 1 msg/s), then `kill -9` of node-2; 1 530 clients lose their socket at once. Clients use exponential backoff starting at 1 s, either deterministic ("no jitter") or full jitter (`random(0, base·2^attempt)`). Ticket requests and upgrades both go through HAProxy (`results/reconnect-storm-*.json`, chart: [`docs/assets/reconnect-storm.svg`](assets/reconnect-storm.svg)).

![reconnect storm](assets/reconnect-storm.svg)

| | no jitter | full jitter |
|---|---|---|
| Failed upgrade attempts after the kill | **1 232** | **0** |
| 50 % / 95 % / 100 % reconnected after | 3.2 s / 15.5 s / 15.7 s | 0.7 s / 1.1 s / 1.2 s |
| Ticket request p99 | 369 ms | 25 ms |
| Peak successful upgrades in one 100 ms bucket | 209 | 215 |
| Gaps / duplicates in the message stream | 0 / 0 | 0 / 0 |

Without jitter, 1 530 clients hit HAProxy in the same few milliseconds. macOS's `kern.ipc.somaxconn` is 128, so the accept queues of HAProxy and the nodes overflowed and most attempts failed. Every failed client then waited exactly 2 s, 4 s, 8 s — in lockstep again — which produces the waves in the chart and a 15.7 s recovery. With full jitter the same 1 530 clients spread over about one second, nothing overflowed, and everyone was back in 1.2 s. Jitter did not lower the peak rate the servers saw; it removed the *failures*, and failures are what cause the next wave.

Two more things came out of this scenario:

- An early run showed ticket p99 of ~3 s. Ticket requests that HAProxy had already assigned to the dead node were retried against the same dead server. Adding `option redispatch` + `retries 3` fixed it (ADR 0003).
- Another early run never recovered: I had set `maxconn 1600` per server, so after losing one node the two survivors could only hold 3 200 of 4 590 clients and the rest queued in HAProxy until timing out. Capacity planning has to assume N−1 nodes.

Drain (SIGTERM) uses the same idea on the server side: every client gets `drain{after: random(0, 10 000)}`; the integration test checks that 40 clients receive 40 distinct delays with a mean in the middle of the window.

## 8. Why XADD and PUBLISH must be atomic

`bench/atomicity.mjs`: 8 publishers on separate Redis connections, 500 messages each, one subscriber on the shard channel (`results/bench-atomicity.json`):

| | `INCR`, `XADD`, `SPUBLISH` as separate commands | one Lua script |
|---|---|---|
| Messages delivered over pub/sub out of `seq` order | **1 205 of 4 000** | 0 |
| `XADD` rejected ("ID is equal or smaller than the target stream top item") | **993** | 0 |
| Throughput | 19 581 publishes/s | 38 018 publishes/s |

Between publisher A's `INCR` and its `XADD`, publisher B can `INCR` and `XADD` a higher id; A's `XADD` with the lower explicit id is then rejected, and its `SPUBLISH` arrives after B's. Subscribers on different nodes would see different orders and the history would have holes. Inside one script nothing else runs, so pub/sub order equals `seq` order equals stream order. It is also twice as fast because it is one round trip instead of three. The property test in `packages/server/test/integration/cluster.test.ts` checks the end-to-end guarantee: up to 6 concurrent publishers on different nodes, up to 6 subscribers spread over 3 nodes, 12 random runs, every subscriber sees the same gapless sequence.

## 9. Ephemeral ports and file descriptors: what the load generator ran into

- **Per-process descriptor limit.** `kern.maxfilesperproc` is 10 240 and cannot be raised without root. The `fd-limit-demo` run asked one load-generator process for 10 500 connections: it plateaued at **10 204** open connections (10 240 minus the process's own descriptors) and logged 4 250 `EMFILE` connect errors while retrying. The fix in production is several load-generator processes or containers; here I kept single runs below 10 000.
- **System-wide limit.** `kern.maxfiles` is 30 720 for the whole machine and ~4 500 were already in use by other apps. On loopback each connection needs a descriptor on both ends, and through HAProxy four (client, HAProxy front, HAProxy back, node). That is why the HAProxy scenarios use 4 590 connections and HAProxy runs with `fd-hard-limit 10240` and `maxconn 4800`.
- **Ephemeral ports.** macOS hands out 49 152–65 535 (16 384 ports) per source address, and loopback has one source address unless you add aliases (root again). With HAProxy in the middle every client connection also consumes a second ephemeral port for HAProxy→node. Descriptors ran out before ports did on this machine, but the reconnect storms leave sockets in `TIME_WAIT` (2 × MSL = 30 s on macOS), so repeated storm runs back to back would exhaust ports. The spec's answer — several load-generator containers with different IPs — is the right one; in `infra/docker-compose.yml` the loadgen service has `replicas: 2` and its own `nofile` ulimit.
- **Accept backlog.** `kern.ipc.somaxconn` = 128 caps the listen backlog no matter what Node passes to `listen()`. It is invisible in steady state and decisive in a reconnect storm (§7).
- **Ticket fetches.** The first storm runs limited the load generator's HTTP agent to 16 sockets per worker; ticket requests queued in the client, which accidentally acted like jitter and hid the storm. Measuring a thundering herd requires the load generator itself not to throttle.

## 10. Where the CPU actually goes: `--cpu-prof` of a fan-out run

Measured on 2026-10-05. The three nodes were started with `node scripts/cluster.mjs up --node-arg --cpu-prof --node-arg --cpu-prof-dir=.run/prof` and loaded with `fanout-big-room`: 3 000 subscribers (1 000 per node), 5 msg/s, 40 s, so about 15 000 frames/s out of the cluster. V8 writes the profile when the process exits. `scripts/cpuprofile-top.mjs` adds up self time per function and per category from the `samples`/`timeDeltas` arrays. The profile of node-1 is in `results/profiles/fanout-node-1.cpuprofile` (it opens in Chrome DevTools or speedscope), and the analysed output is in `fanout-node-1.top.txt`.

node-1 over 52.8 s of wall time: **busy for 3.68 s (7.0 %)**, idle for 93 %. Here is how the busy time splits by category:

| Category (self time) | Share of busy | ms |
|---|---|---|
| Native builtins, almost all of it the `writev` syscall | 37.1 % (writev alone **32.1 %**) | 1 368 |
| Node core JS (`net`, streams, timers, `nextTick`) | 25.5 % | 939 |
| `(program)` (native, not attributed) | 9.6 % | 354 |
| `ws` (framing, `send`, receiver) | 7.8 % | 289 |
| **The engine itself** (`packages/server`) | **6.9 %** | 254 |
| ioredis | 4.7 % | 172 |
| GC | 3.8 % | 140 |
| prom-client | 3.7 % | 136 |
| protocol (validator, codecs) | 0.7 % | 24 |
| pino | 0.1 % | 4 |

These are the top functions by self time: `writev` 32.1 %, `(program)` 9.6 %, GC 3.8 %, `ws` `Sender.send` 3.0 %, `WebSocket.send` 2.1 %, `listOnTimeout` 1.9 %, `createWriteWrap` 1.7 %, `writeUtf8String` 1.7 %, prom-client `Counter.setValue` 1.7 %, `processTimers` 1.6 %, `nextTick` 1.6 %, `Socket._writeGeneric` 1.4 %. The first engine function is `ChannelRegistry.deliver` at 0.8 %, followed by `Connection.sendBytes` at 0.6 %. Nodes 2 and 3 were busy for 3.68 s and 3.47 s, with the same split: engine 6.7 % / 7.5 %, `ws` 8.9 % / 7.3 %.

What this tells me:

- **After serialize-once, the cost of fan-out is the kernel and not JavaScript.** `ws` frames each message as `[header, payload]` inside `cork()`/`uncork()`. Every frame to every subscriber is therefore one `writev` syscall. node-1 sent about 200 000 frames during the run, which works out to about **6 µs of `writev` per frame** and about 18 µs of total busy time per frame (connect phase included). All of the engine's own code is less than a quarter of what `writev` costs.
- So the next real gain would not come from faster JS. It would come from fewer syscalls: coalescing several frames for the same socket into one write. That only helps when a subscriber gets more than one frame per tick (busy rooms, presence storms). It trades latency for throughput, so it is not on by default. Merging header and payload into one buffer would not help, because it is still one syscall.
- prom-client costs 3.7 %, almost all of it one `Counter.inc()` per outgoing frame. One `inc(n)` per channel delivery would remove most of that. It is a small, free improvement, but the effect is below the noise of this machine, so I have not claimed a number for it.
- The run with the profiler on, compared with an identical run without it (`results/profiles/fanout-3000-{with,without}-cpu-prof.json`): delivery p50 12.2 vs 13.5 ms, p99 28.1 vs 28.1 ms, CPU per node 7.8 % vs 6.1 %. **Event loop utilisation went from 0.07 to 0.61** with the profiler on. The sampling profiler interrupts the event loop's poll about every millisecond, so ELU from a profiled process is meaningless. Latency and CPU are trustworthy enough. Event loop utilisation is not.

## 11. Heap snapshots: what a connection is made of, and proof that it is released

`packages/server/bench/heap-snapshot-diff.mjs` (measured 2026-10-05) starts the engine in a child process with `--expose-gc`. It first runs a warm-up cycle of 200 connections so that JIT code and lazily built caches already exist. The child forces GC four times and calls `v8.writeHeapSnapshot()` at four points:

- A: baseline.
- B: 2 000 connections open. Each one is subscribed to a 20-user `room:` with presence and to its own `user:` channel, and has published one message.
- C: after all 2 000 have closed with a normal close handshake.
- D: after a second cycle of 2 000 that ended with `terminate()` (no close frame, the way a dead client looks).

The script parses the snapshot JSON itself: it groups nodes by constructor and compares the snapshots by class. Because node ids are stable inside one process, it can also list objects in C that did not exist in A (`results/heap-snapshot-diff.json`).

| Snapshot | Self size of all heap objects | `heapUsed` | `Connection` | `WebSocket` | `Receiver` / `Sender` | `TokenBucket` | `Socket` |
|---|---|---|---|---|---|---|---|
| A baseline | 17.25 MB | 11.8 MB | 0 | 1 | 0 / 0 | 0 | 56 |
| B 2 000 open | 31.62 MB | 24.8 MB | 2 000 | 2 001 | 2 000 / 2 000 | 6 000 | 2 056 |
| C all closed | 17.70 MB | 12.3 MB | **0** | 1 | **0 / 0** | **0** | 7 |
| D second cycle, terminated | 16.72 MB | 11.3 MB | **0** | 1 | **0 / 0** | **0** | 7 |

(The one `WebSocket` is the class prototype entry. The 56 baseline sockets are HTTP keep-alive sockets from the warm-up ticket requests, which the server closes after 5 s.)

**No leak.** Every per-connection object returns to its baseline count after a normal close and after an abrupt terminate. C is 0.45 MB above A, and a class-by-class diff shows what that is: 394 KB of `(code)` (more functions got optimised by the second phase) and internal `(array)` backing stores (Map/Set hash tables that do not shrink back). No objects from the engine or `ws` are in that difference. Going from C to D, the total went *down* by 0.98 MB. Of the 4 312 objects in C that did not exist in A, 2 788 are compiled code and 1 082 are hidden V8 internals. There are none of `Connection`, `Socket`, `Timeout` or a closure from the engine.

One connection in this setup is **7.2 KB** of heap (snapshot self size). §1's 5.1–6.0 KB had one subscription and no presence. Per connection, the snapshot diff shows:

- 1.15 KB of internal arrays (Map/Set backing stores of `subs`, listener arrays);
- 13 plain objects (844 B: subscription records, the parsed ticket claims, pino child bindings);
- 2 native handles (555 B: the `TCP` wrap and its stream);
- 8 closures (448 B: the `message`/`close`/`pong`/`error` handlers and `ws` internals);
- 13 arrays (418 B);
- 10 strings (370 B: connection id, user id, channel names);
- 5 closure contexts (272 B);
- the `ws` objects: `Socket` 328 B, `Receiver` 272 B, `WebSocket` 200 B, two `WritableState` 208 B;
- the engine's `Connection` (240 B) and its three `TokenBucket`s (168 B).

`ws` and `net` own about half of it, the same split §1 measured with `process.memoryUsage()`.

## 12. Observability, verified: Prometheus + Grafana on the running cluster

Run on 2026-10-05:

```bash
node scripts/cluster.mjs up --grafana
node scripts/verify-observability.mjs
```

The first command starts the three nodes, HAProxy, Prometheus on :4390 (project-local config and TSDB in `.run/`) and Grafana on :4391. Grafana runs from the Homebrew binary with a project-local `grafana.local.ini`, data dir and provisioning, and the existing `infra/grafana/dashboards/realtime.json` is provisioned from disk. The second command is the check (it can also start and stop that stack itself):

- through Grafana's HTTP API it confirms the dashboard exists (`/api/search`, `/api/dashboards/uid/realtime-engine`: provisioned, 16 panels, the same count as the JSON file) and that all four Prometheus targets are `up` (3 nodes plus the HAProxy exporter);
- it starts a short load: `fanout-big-room` through HAProxy, 600 subscribers, 5 msg/s, 60 s;
- after 35 s it runs **every panel's query** through `/api/ds/query` (the datasource proxy, the same path the browser uses). Every panel must return at least one series without errors. Ten panels that must move under this load (connections, msgs/s, frames in, fan-out p99, ELU, heap, RSS, Redis p99, upgrades, HAProxy sessions) must also be above zero;
- it takes a Playwright screenshot of the dashboard ([`docs/assets/grafana-dashboard.png`](assets/grafana-dashboard.png)) and writes `results/observability-check.json`.

![Grafana dashboard during the check](assets/grafana-dashboard.png)

Two cold runs (stack started from nothing each time) both passed, with 16 of 16 panels returning data. Peak values from the queries:

| Panel | Run 1 | Run 2 (the screenshot) |
|---|---|---|
| Connections per node / HAProxy sessions per server | 201 / 201 | 201 / 201 |
| Messages out / s (durable) | 2 980 | 3 000 |
| Fan-out duration p99 | 9.5 ms | **660 ms** |
| Event loop lag p99 (corrected) | 2.8 ms | 22 ms |
| Event loop utilisation | 6.6 % | **100 %** (one scrape, node-2) |
| GC pause p99 | 2.5 ms | **242 ms** (minor GC, node-3) |
| Redis command p99 | 23.5 ms | 473 ms (`node_alive`) |
| Heap / RSS per node | ≤ 19.1 MB / ≤ 70.7 MB | ≤ 22.6 MB / ≤ 54.2 MB |

Run 2 is the more instructive one, and it is why I kept that screenshot. Around 01:58:50, all panels show the same stall at once: a 240 ms *minor* GC on one node, one ELU sample at 100 %, a 470 ms Redis command and fan-out p99 jumping to 660 ms. Nothing in the workload changed. At that moment the laptop had 6.6 GB of its 8 GB swap in use (other agents were building other projects), and a scavenge that has to page in the young generation takes hundreds of milliseconds. Seen from inside, a stalled host looks like "everything got slow at the same instant". Correlated spikes across unrelated metrics point at the machine, not the code. A single latency graph would have sent me looking for a bug.

Three things went wrong on the way to a passing check, and each one was a real defect:

- **Counters that do not exist yet are invisible to `rate()`.** The first cold run failed: "WebSocket upgrades / s" was 0 although 600 clients had just connected. prom-client creates a labelled series on its first `inc()`, so the first scrape already saw `rt_upgrades_total{result="ok"} 200`. `rate()` has no earlier sample, so it can never count the burst that created the series. The engine now initialises every known label value of its counters to 0 at startup (`metrics.test.ts`). This matters most for exactly the events you care about: the first `user_limit` rejections and the first `4029`s.
- **The load generator ignored HAProxy for some scenarios.** `fanout-big-room --via haproxy` assigned clients to `target = i % 3` while only one target (HAProxy) existed, so two thirds of the clients never connected (201 of 600). It went unnoticed because every published HAProxy run used `reconnect-storm`, which already handled this. The worker now takes the target modulo the number of targets.
- **Grafana 13 no longer ships the Prometheus datasource in the binary.** It is "preinstalled" from grafana.com on first start. With plugin downloads disabled (`preinstall_disabled`, `public_key_retrieval_disabled`, and the grafana.com URLs pointed at localhost), the dashboard loaded but every query returned `plugin.notRegistered`. `cluster.mjs` now copies a signed copy of the plugin that is already on disk into `.run/grafana/plugins` (`GRAFANA_PROMETHEUS_PLUGIN`, or a local Grafana data dir it finds). I watched the Grafana process with `lsof` for a minute after start, and it made no outbound connections with this config.

## Other findings

- **Own validator vs zod** (`results/bench-codec-validator.json`): the 200-line validator in `packages/protocol` validates `pub` frames 6.2× faster (4.7 M vs 0.77 M/s), cursor `eph` frames 8.8× faster and `sub` frames 1.9× faster than an equivalent zod 4 schema (ADR 0007).
- **`monitorEventLoopDelay` reports the resolution.** With `resolution: 10` an idle loop reports p50 ≈ 10.3 ms, because the histogram measures the interval between ticks, not the delay beyond it. prom-client's `nodejs_eventloop_lag_p99_seconds` therefore never goes below ~10 ms. Setting the resolution to 1 ms fixed the number but pushed idle event loop utilisation from 0.5 % to 34 %. The engine keeps prom-client's metric and adds `rt_event_loop_lag_seconds{quantile}` with the resolution subtracted; all event-loop numbers in this document use the corrected metric.
- **ELU needs a minimum window.** Two scrapes a few milliseconds apart produced `rt_event_loop_utilization = 1`, because the only work in that window was serving the scrape. The gauge now only advances when at least 250 ms have passed.
- **Many small rooms with 10 Hz cursors is the expensive workload, not the big room.** `many-rooms` sustained 1 000 participants (165 rooms, 10 Hz cursors each, 57 000 frames/s out) at p50 8.7 ms, with p99 spikes to 560 ms when the laptop was busy. At 3 000 participants (81 600 frames/s out) the nodes' event loops were saturated (ELU 1.0, p50 384 ms) while Redis used 7 % CPU: the cost is one `ws.send` syscall per frame per recipient, which serialize-once cannot share. Ephemeral traffic grows with room size squared. 0 gaps in every run, including the overloaded ones.
- **Slow consumers** (`slow-consumers`: 1 000 clients, 5 % stop reading, scaled-down watermarks 16 KB / 64 KB / 512 KB and 10 s lag timeout, 2 KB chat messages at 1/s per participant): the server dropped 14 956 ephemeral frames to the slow clients, sent `lag`, closed 2 of them with `4008` via backpressure; the rest were cut by the heartbeat (a paused reader also stops answering pings) and show up as 1006 on the client. Normal clients had p50 7 ms / p99 243 ms, the same as or better than the baseline run without slow clients (p50 18 ms / p99 2.4 s, a noisier run). Node heap peaked at 73 MB vs 109 MB in the baseline. The integration test `cluster.test.ts › slow consumers` checks the same property deterministically: a fast client's p99 stays under 200 ms while the slow one is disconnected and later resumes without loss.
- **Resume under churn** (`resume`: 3 000 clients that randomly drop for 1–30 s and reconnect to a random node, 100 publishers at 5 msg/s for 120 s): 12 249 reconnects, 912 011 messages replayed from history, **0 gaps, 0 duplicates, 0 resets**. Live delivery stayed at p50 1.0 ms / p99 89 ms.
- **A per-user limit has to be cluster-wide** (ADR 0009). Without sticky sessions, HAProxy spreads one user's tabs over all nodes, so a per-node limit of 10 really meant 30. The counter is now a Redis hash per user (`nodeId → count`), checked and incremented in one Lua script. The hardest part is not the counting but the decrement nobody performs: after `kill -9` the dead node's counts are removed by the same sweeper that removes its presence. A node that was declared dead while it was only stalled (exactly the situation in §12) rewrites its counts and presence when it notices. The smoke test opens 10 connections for one user via HAProxy (spread over all 3 nodes), sees the 11th closed with `4029`, then kills a node and waits for that node's count to be released.
- **`node:cluster` vs separate processes** (3 000 subscribers, 5 msg/s, 30 s; re-measured on 2026-10-05). The run published on 2026-10-01 was wrong: because of the load-generator bug in §12, only 1 001 of the 3 000 clients reached HAProxy. With the fix, I ran the pair twice in opposite order (`results/fanout-{node-cluster-module,haproxy-processes}{,-run2}.json`). Three `cluster` workers (round-robin in the primary) delivered at p50 14.9 / 12.0 ms and p99 129 / 39 ms. Three processes behind HAProxy delivered at **p50 33.6 / 33.6 ms** and p99 441 / 80 ms. CPU per node was the same in both (5.3–6.2 %), and there were 0 gaps everywhere. The p99s swing with the machine (the first pair ran while the laptop was swapping), but the p50 gap is stable and comes from the architecture. Behind HAProxy every frame takes one more loopback hop through a single proxy process forwarding 15 000 frames/s. With `node:cluster` the primary only hands accepted sockets to the workers, and from then on each worker writes to its clients directly. On separate machines that hop is a LAN hop and the gap shrinks, but it does not disappear. Operationally, separate processes still win: with `node:cluster`, every `/metrics` scrape lands on a random worker, so per-worker metrics need a separate port or aggregation, and a draining worker cannot be taken out of rotation by a health check. My conclusion: separate processes behind a proxy, with the proxy hop counted in the latency budget.
