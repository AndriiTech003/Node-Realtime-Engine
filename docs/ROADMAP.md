# ROADMAP · Node Realtime Engine (3–4 недели)

## M1 — Один узел (1 неделя)
- [ ] `packages/protocol`: типы фреймов, JSON-кодек, свой валидатор, коды ошибок
- [ ] `packages/server`: `node:http` + `ws` (`noServer`), ticket auth на upgrade, Origin check, реестры соединений и каналов, sub/unsub/pub/eph, heartbeat (один таймер), token bucket, лимиты, graceful shutdown
- [ ] Redis: Lua publish (seq + stream + cmid + spublish), resume через XRANGE с буфером live
- [ ] `/metrics`, `/health/live`, `/health/ready`
- [ ] Unit: кодек, валидатор, token bucket, склейка истории и live (дубли, порядок)
- [ ] Integration (Testcontainers Redis): pub/sub/resume, идемпотентность cmid, `reset` при обрезанной истории

**Готово, когда:** два клиента в одной комнате обмениваются сообщениями, отключённый клиент догружает пропущенное.

## M2 — Кластер, presence, backpressure (1 неделя)
- [ ] 3 узла + HAProxy в compose
- [ ] Sharded pub/sub, подписка узла только на каналы с локальными подписчиками
- [ ] Presence (по пользователю, multi-tab, истечение при смерти узла)
- [ ] Backpressure: watermark'и, pending-очередь, `lag`, `4008`
- [ ] Drain с `drain{after}`
- [ ] Server publish API
- [ ] Integration: публикация на узле A → порядок на узлах B и C одинаковый; **property-тест**: N параллельных публикаторов на разных узлах, у всех подписчиков одинаковая последовательность; presence после `kill` узла; медленный клиент не влияет на быстрого
- [ ] ADR: атомарный Lua publish, ticket auth, без sticky sessions, `terminate` вместо `close` для мёртвых соединений

## M3 — Клиент SDK + демо (0.5–1 неделя)
- [ ] `@scope/realtime-client`: состояния (connecting / open / reconnecting / closed), backoff + full jitter, resume по каналам с `lastSeq`, дедуп по seq, детект пропусков → повторная подписка с `from`, офлайн-очередь `pub` с `cmid`, presence API, ephemeral с throttle, app-level ping, обработка `drain` / `4008` / `4001` (новый ticket)
- [ ] Тесты клиента против настоящего сервера (Node) и в браузере (Playwright)
- [ ] Публикация в npm (tsup, provenance, changesets) — переиспользовать пайплайн из проекта 02
- [ ] `apps/demo` «Pulse Rooms»: чат, аватары presence, живые курсоры, «печатает…», **Network lab** (кнопки: offline 10 сек, kill соединения, симулировать медленного клиента; лента seq с подсветкой догруженных сообщений)
- [ ] `apps/auth-stub`

## M4 — Нагрузка, измерения, LEARNINGS (1 неделя)
- [ ] `apps/loadgen` (worker_threads, несколько контейнеров), все 6 сценариев, проверка корректности seq
- [ ] Grafana-дашборд
- [ ] Прогоны на VPS, профилирование (`--cpu-prof`, `clinic flame`, heap snapshots)
- [ ] Минимум 3 оптимизации с цифрами до и после (serialize once, chunked fan-out, один heartbeat-таймер, msgpack)
- [ ] `docs/LEARNINGS.md` + публичный README (EN) с таблицей результатов и графиком reconnect storm
- [ ] Деплой демо (тот же VPS), видео 90 секунд

## Сценарий видео (90 сек)
1. Два окна Pulse Rooms: чат, курсоры, presence.
2. Network lab: offline на 10 сек → в другом окне пишут 5 сообщений → online → сообщения догружаются, в ленте seq видна подсветка resume.
3. Терминал: `docker kill node-2` → клиенты переподключаются к другим узлам, сообщения не теряются.
4. Grafana: loadgen `fanout-big-room`, 10k соединений, p99 латентности, event loop lag.
5. График reconnect storm: без jitter против с jitter.

## Highlights для README (EN)
- Plain Node.js: `node:http` + `ws`, no frameworks, ~2.5k lines you can read in an evening
- Total ordering per channel across nodes via an atomic Lua publish (INCR + XADD + SPUBLISH)
- Resume from any node after a disconnect: gapless history + live merge, verified by a property test
- Backpressure-aware fan-out: slow consumers are isolated and cannot exhaust node memory
- Graceful drain with jittered reconnects; reconnect storm measured with and without jitter
- Load-tested at N concurrent connections, fan-out p99 X ms; findings written up in LEARNINGS.md
