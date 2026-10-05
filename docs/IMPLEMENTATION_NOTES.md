# Implementation notes

Build of the spec in `README.md`, `docs/PROTOCOL.md`, `docs/SPEC.md`, `docs/ROADMAP.md` (M1–M4), following `devinfra/CONVENTIONS.md`.

## Layout

| Path | What |
|---|---|
| `packages/protocol` | Frame types, own validator (`validator.ts`), JSON + MessagePack codecs (msgpackr), close/error codes, channel parsing |
| `packages/server` | The engine: `node:http` + `ws` (`noServer`, manual `handleUpgrade`), ioredis, pino, prom-client. `src/server.ts` (HTTP routes, upgrade, frame handling, drain), `connection.ts` (backpressure), `channels.ts` (registry, fan-out queue, chunking), `outbound.ts` (serialize once), `redis.ts` + `lua.ts` (atomic publish, presence and user-limit scripts, sweeper, `reapNode`), `user-limit.ts` (cluster-wide per-user connection limit), `resume.ts`, `metrics.ts`, `token-bucket.ts`, `auth.ts`, `main.ts`, `cluster-main.ts` (node:cluster variant for the comparison). `bench/` holds the micro-benchmarks and `heap-snapshot-diff.mjs`. |
| `packages/client` | `@ashamrai/realtime-client` (tsup ESM + CJS + d.ts, protocol bundled in) |
| `apps/auth-stub` | Plain `node:http`: `POST /api/session` (JWT), `POST /api/ticket` (calls `/v1/tickets`), `GET /api/config`, `GET /api/history`, `POST /api/simulate-slow` |
| `apps/demo` | Pulse Rooms (React 19 + Vite 7): rooms, chat, presence avatars, live cursors, typing, Network lab, seq feed; `client-test.html` is the browser harness for SDK tests |
| `apps/loadgen` | `worker_threads` load generator, six scenarios, seq correctness, latency via embedded `ts`, JSON + Markdown results |
| `infra/` | `haproxy.cfg`, `prometheus.yml` (+ `.local.yml`), `docker-compose.yml`, Grafana provisioning (env-driven, shared by compose and the local run) + dashboard JSON + `grafana.local.ini` |
| `scripts/` | `cluster.mjs` (local 3-node cluster + HAProxy + auth-stub [+ demo, Prometheus, Grafana, extra node flags]), `verify-observability.mjs` (Grafana API + panel queries under load + screenshot), `cpuprofile-top.mjs` (self-time analysis of `.cpuprofile`), `smoke.sh` / `smoke-check.mjs`, `bench-all.sh`, `e2e-stack.mjs`, `seed.mjs`, `storm-chart.mjs`, `results-table.mjs`, `screenshot.mjs`, `purge-prefix.mjs`, `check-no-comments.mjs` |
| `docs/adr/` | ADR 0001–0009 |
| `results/` | Raw load-test and benchmark JSON, `RESULTS.md`, `observability-check.json`, `heap-snapshot-diff.json`, `profiles/` (CPU profile + analysis + paired runs) |

## Ports and resources (convention: 4300–4399, Redis db 3)

HAProxy 4300 (stats + Prometheus exporter 4399), nodes 4301–4303, auth-stub 4310, demo dev 4320 / preview 4321, integration-test nodes 4330–4389 (first free port), Playwright e2e stack 4341–4343, memory benchmark 4380, Prometheus 4390 and Grafana 4391 (both locally via `cluster.mjs up --grafana` and in compose; 4369 is RabbitMQ's epmd and is avoided). Redis `127.0.0.1:6379/3`; prefixes `rt:` (dev cluster), `rt:test:<run>:` (tests, purged before and after every run), `rt:bench:`, `rt:smoke:<ts>:` (purged by the scripts).

## How to run

```bash
/Users/asnh/Desktop/projects_for_git/devinfra/start.sh   # Redis on :6379
pnpm install && pnpm build

pnpm cluster:up                 # node-1..3 on 4301-4303, HAProxy :4300, auth-stub :4310
node scripts/cluster.mjs up --demo --prometheus   # plus demo :4321 and Prometheus :4390
node scripts/cluster.mjs up --grafana             # plus Prometheus :4390 and Grafana :4391 (admin / realtime-local, anonymous viewer)
pnpm verify:observability       # Grafana API: dashboard + every panel query returns data under a 60 s load, screenshot
node scripts/cluster.mjs up --no-auth --no-haproxy --node-arg --cpu-prof --node-arg --cpu-prof-dir=.run/prof
pnpm profile:top .run/prof/<file>.cpuprofile   # top self-time functions and categories
pnpm bench:heap                 # heap snapshots before/with/after 2 000 connections, class diff
pnpm seed                       # welcome messages through the server publish API
pnpm dev:demo                   # Vite dev server :4320 (proxies /api to auth-stub)
node scripts/cluster.mjs kill 2 # kill -9 a node; start-node 2 brings it back; drain 2 = SIGTERM
pnpm cluster:down

pnpm lint && pnpm typecheck
pnpm test:unit
pnpm test:int                   # local Redis db 3; TESTCONTAINERS=1 uses @testcontainers/redis instead
pnpm test:pw                    # Playwright: client-browser + demo-e2e (starts its own stack)
pnpm pack:client                # publint, attw, size-limit, pnpm pack
pnpm smoke                      # scripts/smoke.sh
pnpm bench                      # scripts/bench-all.sh (all load scenarios, ~1 h)
node apps/loadgen/dist/cli.js fanout-big-room --connections 2000 --duration 30
```

Docker: `docker compose -f infra/docker-compose.yml up --build` (3 nodes, HAProxy, Redis, auth-stub, demo, Prometheus, Grafana; `--profile seed`, `--profile load`). Written per spec but **not executed** — Docker is not installed on this machine.

## Verification actually run (last full pass 2026-10-05, after the gap-closing changes below)

| Command | Result |
|---|---|
| `rm -rf node_modules */*/node_modules */*/dist && pnpm install --frozen-lockfile` | ok (lockfile unchanged except `ioredis` moving to the server's runtime dependencies) |
| `pnpm build` | 6/6 packages built |
| `pnpm lint` (ESLint + no-comments checker) | exit 0, 93 files, 0 errors / 0 warnings, "no source comments found" |
| `pnpm typecheck` | 6 packages + root (tests, e2e) clean, `strict` everywhere |
| `pnpm test:unit` | **16 files, 99 tests passed** (protocol 19, server 45, client 26, auth-stub 4, loadgen 5) |
| `pnpm test:int` | **6 files, 60 tests passed** against local Redis (pubsub 21, resume/idempotency 8, cluster/presence/backpressure/drain 9 incl. the fast-check property test with 12 runs, heartbeat 3, client SDK in Node 10, **cluster-wide user limit 9**) |
| `pnpm test:pw` | **9 tests passed** (client-browser 5: JSON, MessagePack, resume after kill, offline queue, presence; demo-e2e 4: two-browser chat + presence + typing + cursors, offline 10 s → resume fills the gap with highlighted seqs, kill connection, simulated slow client) |
| `pnpm pack:client` | publint ok, attw "No problems found" (node10, node16 CJS/ESM, bundler), size 14.78 KB brotli (limit 20 KB), tarball in `.run/` |
| `pnpm smoke` | exit 0, **12 checks**: Ann/Bob through HAProxy, Carl pinned to Bob's node, **Dana's 10 connections spread over 3 nodes counted cluster-wide and the 11th closed with 4029**, presence, publish 1..3, `kill -9` of Bob's node, publish 4..8, Bob resumes on another node with 5 replayed messages / no gaps / no duplicates, Carl expired from presence by a surviving node, **Dana's connection on the killed node released by a surviving node**, HTTP history = 8 |
| `node scripts/verify-observability.mjs` (cold: starts and stops the stack itself) | exit 0: 4/4 Prometheus targets up, dashboard `realtime-engine` provisioned with 16 panels, **16/16 panel queries return data** under a 600-connection fan-out through HAProxy, 10/10 activity panels non-zero, screenshot `docs/assets/grafana-dashboard.png`, report `results/observability-check.json` |
| `--cpu-prof` fan-out run + `scripts/cpuprofile-top.mjs` | `results/profiles/` (LEARNINGS §10) |
| `pnpm bench:heap` | 2 000 connections, 4 snapshots, 0 leaked per-connection objects (LEARNINGS §11) |
| Load scenarios (`results/*.json`) | all six scenarios run on 2026-10-01, **0 gaps** in every run (see below) |

### Guarantees from README → tests

1. Same order for all subscribers on all nodes: `cluster.test.ts` (cross-node ordering + fast-check property test), `resume.test.ts` (history/live race), loadgen gap/duplicate tracking.
2. Resume from any node or explicit `reset`: `resume.test.ts` (other node, trimmed history, gap > limit, client ahead), `client-node.test.ts`, browser tests, demo e2e, smoke.
3. Same `clientMsgId` does not duplicate: `resume.test.ts › cmid idempotency` (sequential, across nodes, concurrent), client offline queue tests.
4. Slow client does not affect others, gets `4008`, resumes: `cluster.test.ts › slow consumers` (fast p99 < 200 ms, pending bounded, 4008 observed, gapless resume), `backpressure.test.ts` (watermarks unit), `client-node.test.ts` (4008 → immediate resume), `slow-consumers` load run.
5. Drain without a reconnect peak: `cluster.test.ts › drain` (readiness 503, new upgrades 503, 40 distinct jittered delays, stragglers closed 1001), `client-node.test.ts` (drain → reconnect to another node, gapless), `reconnect-storm` runs.

Also tested: the per-user limit from SPEC §4, now cluster-wide. Covered by `user-limit.test.ts` (integration, two nodes, 9 tests), `user-limit.test.ts` (unit, 5 tests) and two smoke checks.

## Measured numbers (laptop: Apple Silicon, 8 CPUs, 8 GB, local Redis, everything on one shared busy machine)

Load sizes were limited by `kern.maxfilesperproc` = 10 240 and `kern.maxfiles` = 30 720 (both ends of every loopback connection are on this machine). Details and before/after pairs: `docs/LEARNINGS.md`.

| Scenario (size run) | Result |
|---|---|
| idle, 9 000 connections over 3 nodes | 5.5 KB heap per connection (micro-bench: 5.1 KB heap / 17.7 KB RSS engine vs 2.8 / 6.9 KB bare `ws`), connect p99 10.7 ms, idle CPU 1.2 % per node |
| fanout-big-room, 6 000 subscribers, 5 msg/s (30 000 msgs/s out) | p50 22.6 ms, p99 60.8 ms, event loop p99 20.7 ms, 0 gaps |
| many-rooms, 1 000 participants in 165 rooms, 10 Hz cursors (57 000 msgs/s out) | p50 8.7 ms, p99 559 ms; 3 000 and 6 000 participants saturate the laptop (p50 384 ms / 321 ms), 0 gaps in all |
| slow-consumers, 1 000 clients, 5 % not reading, scaled watermarks | others p50 7 ms / p99 243 ms, heap 73 MB, 14 956 ephemeral drops, 4008 + heartbeat disconnects for the paused clients, 0 gaps |
| reconnect-storm, 4 590 via HAProxy, kill -9 of one node (1 530 dropped) | no jitter: 1 232 failed attempts, 95 % back after 15.5 s, ticket p99 369 ms; full jitter: 0 failures, 95 % after 1.1 s, ticket p99 25 ms; 0 gaps both |
| resume, 3 000 clients dropping for 1–30 s, 120 s | 12 249 reconnects, 912 011 messages replayed, 0 gaps, 0 duplicates, 0 resets |
| serialize once vs per subscriber (6 000 subs) | p50 25 ms vs 2 725 ms, CPU 9.9 % vs 21.9 % |
| chunked fan-out (6 000 subs on one node) | other rooms p99 28 ms vs 77 ms, event loop p99 5 ms vs 75 ms |
| shared heartbeat vs per-connection timers (4 000, 1 s ping) | 25 vs 131 ms CPU/s |
| permessage-deflate on | +55 KB RSS/conn, CPU 68.7 % vs 9.9 %, p50 767 ms |
| Lua publish vs separate commands | 0 vs 1 205 out-of-order deliveries, 38 018 vs 19 581 publishes/s |
| `--cpu-prof`, fan-out 3 000 subs, 15 000 frames/s (2026-10-05) | node busy 7.0 % of wall time; `writev` syscall 32.1 % of busy, Node core JS 25.5 %, `ws` 7.8 %, engine code 6.9 %, ioredis 4.7 %, GC 3.8 %, prom-client 3.7 %; profiler inflates ELU 0.07 → 0.61 |
| Heap snapshots, 2 000 connections (2026-10-05) | 7.2 KB/connection (2 subs + presence); after close and after terminate: 0 `Connection` / `Receiver` / `Sender` / `TokenBucket`, total heap back to baseline (+0.45 MB of JIT code/backing stores, then −0.98 MB on the next cycle) |
| Observability check, 600 subs via HAProxy (2026-10-05) | 16/16 panels with data; run 1 fan-out p99 9.5 ms, ELU ≤ 6.6 %; run 2 caught a host stall (240 ms minor GC, ELU 100 %, fan-out p99 660 ms) while the laptop was swapping |
| `node:cluster` vs processes behind HAProxy, 3 000 subs (re-measured 2026-10-05) | p50 14.9 / 12.0 ms vs 33.6 / 33.6 ms (two runs each, opposite order): the extra proxy hop; the 2026-10-01 numbers were invalid (loadgen bug, 1 001 of 3 000 clients) |

## Changes on 2026-10-05 (closing the remaining gaps)

1. **Observability run for real.** `cluster.mjs up --grafana` starts Prometheus (:4390, `infra/prometheus.local.yml` scraping the 3 nodes and HAProxy's exporter) and Grafana (:4391, project-local ini, data dir and provisioning, no plugin downloads; see deviation 17). The existing dashboard JSON is provisioned. `scripts/verify-observability.mjs` checks over Grafana's HTTP API that the dashboard exists and that every panel query returns data while a 60 s fan-out load runs through HAProxy. It also takes a Playwright screenshot (`docs/assets/grafana-dashboard.png`).
2. **Profiling.** A `--cpu-prof` run of `fanout-big-room` was analysed with `scripts/cpuprofile-top.mjs`. A heap-snapshot diff (`packages/server/bench/heap-snapshot-diff.mjs`) took snapshots before 2 000 connections, with them open, after a close and after a terminate cycle, and found no leak. Both are written up in LEARNINGS §10–§11 with the numbers.
3. **Cluster-wide per-user connection limit** (`user-limit.ts`, two Lua scripts, ADR 0009). It is a Redis hash per user with a field per node, checked and incremented atomically in the upgrade handler. Release is idempotent on every close path. A dead node's fields are reaped by the presence sweeper, a graceful stop clears its own, and a restart under the same id clears the previous incarnation. A node that was reaped while alive resyncs its counts. Covered by 9 integration tests across two nodes, 5 unit tests and 2 smoke checks.
4. **Fixed along the way:**
   - `ioredis` was a devDependency of `@ashamrai/realtime-server` although `redis.ts` imports it at runtime. The Dockerfile's `pnpm deploy --prod` would have produced an image that crashes on start. It is now a runtime dependency.
   - Labelled counters (`rt_upgrades_total`, `rt_messages_in_total`, `rt_messages_out_total`, `rt_publishes_total`, `rt_rate_limited_total`) are initialised to 0. Before, `rate()` could not see the burst that created a series, and a cold observability run showed 0 upgrades/s while 600 clients connected. Covered by `metrics.test.ts`.
   - The load generator now takes each client's target modulo the number of targets. With `--via haproxy`, `fanout-big-room` previously connected only a third of its clients. This invalidated the 2026-10-01 `node:cluster` vs HAProxy comparison, which was re-measured twice (LEARNINGS, Other findings).
   - A node that peers declared dead while it was only stalled now re-registers its presence entries, as well as its user counts, when its `SADD nodes` reports it was removed (integration test). Before, its users silently vanished from presence until they reconnected.
   - Lua scripts are `SCRIPT LOAD`ed when the node connects, so the first call of each script no longer takes the `NOSCRIPT` fallback.
5. **Reviewed and left as they are** (they cannot be changed locally, or they are deliberate): Docker/Testcontainers (no Docker here), lazy retention (documented decision), `4008` delivery behind a full TCP buffer (a TCP property, see deviation 6), load sizes (macOS fd limits need root), VPS/video/deploy/npm publish (not local).

## Deviations and decisions (with reasons)

1. **No Docker.** `docker-compose.yml`, four Dockerfiles and the compose-flavoured Prometheus/Grafana config are written but untested. Locally the cluster is three Node processes + Homebrew HAProxy (`scripts/cluster.mjs`); the same `infra/haproxy.cfg` is used (addresses via environment variables).
2. **Testcontainers** is wired (`TESTCONTAINERS=1` in `test-support/global-setup.ts`) but cannot run here; the default path uses local Redis db 3 with a per-run key prefix that is purged before and after the run.
3. **Extra Redis keys** beyond the spec table: `pres:{ch}:users` (user → connection count, O(1) join/leave decisions), `node:{id}:pres` and `nodes` (fast cleanup of dead nodes), `pres:index` (channels with presence for the sweeper), `user:{uid}:conns` (hash node → connections of the user, cluster-wide limit) and `node:{id}:users` (users counted by a node, for dead-node cleanup). See ADR 0008 and 0009.
4. **Additive protocol fields/endpoints:** `sub.presence: false` (opt out of presence, used by the big-room load test), `ok.pn` (total present users; the list is capped at 1 000), `ok.dup` on a duplicate `cmid`; `GET /v1/history` (the "reload via HTTP" that `reset` refers to); admin endpoints `POST /admin/debug` (per-connection debug logging, the spec's debug toggle), `GET /admin/connections`, `POST /admin/gc` (benchmarks), `POST /admin/simulate-slow` (corks the socket; drives the demo's "simulate slow client" button). Unknown fields are ignored by v1 clients, as the versioning section allows.
5. **Invalid ticket** → `HTTP 401` before upgrade (SPEC §1). Close code `4001` is used when the JWT behind a live connection expires; the SDK always fetches a fresh ticket per attempt anyway.
6. **`4008` delivery:** the close frame is queued behind the slow client's backlog, so a client that is still not reading only sees the socket drop (1006) after the 3 s `terminate()` fallback. A client that reads in time sees `4008` (tested). A paused reader also stops answering pings, so in the load test some slow clients were cut by the heartbeat before backpressure.
7. **Per-user connection limit (10)** is enforced cluster-wide through Redis (ADR 0009). After a `kill -9` the dead node's connections keep counting until the sweeper runs (TTL + sweep interval, 15–25 s with defaults). `MAX_CONNECTIONS_PER_USER=0` disables the limit.
8. **Control-frame token bucket** (sub/unsub/pres/ping: 50/s, burst 200) added next to the spec's pub/eph buckets; systematic violations (50 in 10 s) close with `4029`.
9. **24 h retention** is enforced lazily inside the publish script (trims up to 32 expired entries per publish); `MAXLEN ~ 10000` is the hard bound.
10. **Event loop metric:** prom-client's `nodejs_eventloop_lag_p99_seconds` is exported as specified but includes the 10 ms sampling interval; `rt_event_loop_lag_seconds{quantile}` (corrected) is added and used in all reported numbers.
11. **Ephemeral frames require a subscription** to the room (otherwise `NOT_SUBSCRIBED`) and are not echoed to the sending connection.
12. **Load sizes** are smaller than the spec's 10k–50k because of this machine's file-descriptor limits and shared CPU (sizes per run are in the table above and in each JSON). `many-rooms` with 10 Hz cursors is reported at 1 000 participants; 3 000 and 6 000 are kept as overload runs. The slow-consumer pair uses scaled-down watermarks (16 KB / 64 KB / 512 KB, 10 s), 2 KB messages at 1/s per participant, so that 5 % paused clients actually reach the watermarks within 90 s.
13. **HAProxy** runs with `fd-hard-limit 10240`, `maxconn 4800` (fd limit), `option redispatch` + `retries 3` (found during the storm runs), leastconn, no stickiness.
14. **Not done (outside a local build):** VPS runs, demo deployment, the 90-second video, npm publication (convention: verify only). `clinic flame` was not used: it is not installed, and the convention allows only strictly required installs. `--cpu-prof` plus `scripts/cpuprofile-top.mjs` covers the same question (where self time goes), and the `.cpuprofile` opens in Chrome DevTools or speedscope as a flame chart.
15. **Measurement noise:** the laptop was in use by other applications and another build agent during all runs; e.g. the slow-consumer baseline run had a p99 of 2.4 s while the slow-consumer run itself had 243 ms. Pairs were run back to back and conclusions only drawn where the gap is far beyond that noise.
16. LEARNINGS.md and README.public.md are in English (README.public.md must be; LEARNINGS is meant to be published).
17. **Grafana locally.** Homebrew Grafana 13.2.3 runs with a project-local `infra/grafana/grafana.local.ini` (127.0.0.1:4391, analytics, update checks, news, gravatar, plugin preinstall, plugin key retrieval, cloud migration and alerting off, grafana.com URLs pointed at localhost) and a data, logs, plugins and provisioning dir in `.run/grafana`. Grafana 13 no longer bundles the Prometheus datasource; it is preinstalled from grafana.com on first start, which counts as a plugin download and is disabled here. `cluster.mjs` therefore copies a signed copy that is already on disk (`GRAFANA_PROMETHEUS_PLUGIN`, or the first local Grafana data dir it finds; on this machine project 01's `.observability/grafana/plugins/prometheus`, v13.2.2, signature valid). No download happened. With this config `lsof` showed no outbound connections from Grafana. The provisioning files now take `${PROMETHEUS_URL}` and `${GRAFANA_DASHBOARDS_DIR}` from the environment, so compose and the local run share them.
18. **Measurement environment on 2026-10-05:** the laptop was swapping (6.6 GB of 8 GB swap in use) while agents for projects 02 and 05 were running. Runs were kept modest (≤ 3 000 connections). One observability run shows a host stall, which LEARNINGS §12 discusses.
