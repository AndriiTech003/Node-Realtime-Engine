# SPEC · Архитектура и производительность

## 1. Устройство одного узла

```text
                     ┌──────────────── Node process ─────────────────┐
HTTP upgrade ──────► │ http.Server ── upgrade ──► ws.WebSocketServer  │
POST /tickets        │   (noServer: true, ручной handleUpgrade:       │
POST /publish        │    auth ticket ДО апгрейда → 401 без WS)       │
GET /metrics,/health │                                                │
                     │ ConnectionRegistry  Map<connId, Conn>          │
                     │ ChannelRegistry     Map<channel, Set<Conn>>    │
                     │ Conn: outbound queue, token bucket, subs,      │
                     │       lastPong, state (active|lagging|closing) │
                     │                                                │
                     │ RedisBus: 1 conn для команд, 1 для SUBSCRIBE   │
                     └────────────────────────────────────────────────┘
```

Принципы:
- **Без фреймворков.** Роутинг HTTP — маленький `switch` по `method + pathname`. Весь серверный код ≈ 2–3 тыс. строк, и каждую строку можно объяснить.
- Зависимости: `ws`, `ioredis`, `pino`, `prom-client`, `msgpackr`. Больше ничего. Валидация фреймов — свой маленький валидатор (быстрее и нагляднее zod на горячем пути, это бенчмарк в LEARNINGS).
- Авторизация на этапе `upgrade`: невалидный ticket → HTTP 401 и `socket.destroy()`, WebSocket вообще не создаётся (экономия ресурсов при атаке).

## 2. Модель данных в Redis

| Ключ | Тип | Назначение |
|---|---|---|
| `ch:{name}:seq` | string (INCR) | Последний номер сообщения в канале |
| `ch:{name}:log` | stream, `MAXLEN ~ 10000` | История. ID записи = `0-{seq}`, поэтому `seq` непрерывен и совпадает с ID |
| `ch:{name}:cmid:{cmid}` | string, EX 300 | Идемпотентность публикации → хранит уже выданный `seq` |
| `pres:{name}` | hash `connId → {uid, meta, node}` | Presence по соединениям |
| `pres:{name}:exp` | zset `connId → expiresAt` | Истечение presence при «тихой смерти» узла |
| `ticket:{t}` | string, EX 30 | Одноразовые ticket'ы |
| `node:{id}:alive` | string, EX 15 | Heartbeat узла; при исчезновении его presence-записи убирает другой узел |

### Публикация (атомарно, Lua)

```lua
-- KEYS: seqKey, logKey, cmidKey ; ARGV: payload(json без seq), ts, channel
local existing = redis.call('GET', KEYS[3])
if existing then return {tonumber(existing), 1} end        -- дубль → вернуть прежний seq
local seq = redis.call('INCR', KEYS[1])
redis.call('XADD', KEYS[2], 'MAXLEN', '~', 10000, '0-' .. seq, 'p', ARGV[1], 'ts', ARGV[2])
redis.call('SET', KEYS[3], seq, 'EX', 300)
redis.call('SPUBLISH', 'fan:' .. ARGV[3], seq .. '|' .. ARGV[2] .. '|' .. ARGV[1])
return {seq, 0}
```

**Почему это важно:** INCR, XADD и PUBLISH выполняются в одном атомарном скрипте. Поэтому порядок в pub/sub совпадает с порядком `seq` для всех узлов. Если бы XADD и PUBLISH были отдельными командами от разных узлов, они могли бы перемешаться. Это ключевой ADR проекта.

Ограничение: sharded pub/sub (`SPUBLISH`) + Lua требуют, чтобы все ключи канала были в одном hash slot → имена ключей `ch:{room:42}:seq` с hash tag `{…}`. Проект готов к Redis Cluster, хотя в демо один инстанс.

### Fan-out на узле

1. Узел подписывается (`SSUBSCRIBE fan:{ch}`), когда на канале появляется первый локальный подписчик, и отписывается, когда уходит последний.
2. Пришло сообщение → **сериализуется один раз** в Buffer (JSON или msgpack; для узла с обоими кодеками — лениво по одному разу на кодек) → тот же Buffer отправляется всем локальным подписчикам.
3. Отправка идёт через outbound-очередь соединения с учётом backpressure (раздел 4).
4. Для больших комнат (> 1000 локальных подписчиков) отправка разбивается на чанки по 500 с `setImmediate` между ними, чтобы не блокировать event loop на десятки миллисекунд. Замер до и после — в LEARNINGS.

### Resume (история + live без гонки)

```text
sub(ch, from=F):
  1. SSUBSCRIBE fan:{ch} (если ещё нет) и начать буферизовать live-сообщения для этого соединения
  2. XRANGE ch:{ch}:log (0-(F+1) + COUNT 1000  (повторять, пока не догоним)
     если первая запись > F+1 (история обрезана) → отправить reset{seq: текущий} и перейти к live
  3. отправить историю
  4. отправить буфер live, отбросив seq ≤ последнего отправленного
  5. ok{seq}; дальше обычный live
```

Ограничение resume: не больше 5000 сообщений. Если отставание больше → `reset` (клиент перезагружает состояние через HTTP). Это защита от «тяжёлых» resume после долгого офлайна.

## 3. Presence

- Join: `HSET pres:{ch} connId {uid, meta, node}` + `ZADD pres:{ch}:exp` (now + 45s) + событие `pj` через pub/sub, **если** это первое соединение пользователя в канале. Решение принимает Lua-скрипт, который считает соединения пользователя.
- Heartbeat соединения каждые 15 сек продлевает `exp` (батчем по всем каналам узла одним pipeline).
- Leave при нормальном закрытии: удаление + `pl`, если это было последнее соединение пользователя.
- Смерть узла: другие узлы раз в 10 сек проверяют `pres:*:exp` на просроченные записи (с распределённой блокировкой на сборщик) и рассылают `pl`.
- Клиент получает presence **по пользователям**, а не по соединениям: три вкладки = один аватар.

## 4. Backpressure и медленные клиенты

```text
send(conn, buf, kind):
  if conn.state == closing: return
  if ws.bufferedAmount > HARD (4 MB)           → close(4008), метрика slow_consumer_disconnects
  if ws.bufferedAmount > HIGH (1 MB):
      kind == ephemeral → отбросить (+ однократно lag{ch})
      kind == durable   → положить в conn.pending (ограничение 2000 сообщений, иначе close 4008)
      conn.state = lagging
  else ws.send(buf)

таймер 50 мс по lagging-соединениям:
  пока bufferedAmount < LOW (256 KB) и pending не пуст → отправлять
  если pending пуст → state = active
  если lagging дольше 30 сек → close(4008)
```

- Durable-сообщение, не доставленное из-за 4008, клиент получит при resume. Это не потеря, а перенос ответственности на протокол.
- Сценарий в loadgen: 5% клиентов «замораживают» чтение (`socket.pause()`). Ожидание: p99 латентности остальных не меняется, heap узла ограничен, медленные отключаются.

### Входящий поток
- Token bucket на соединение: 20 `pub` в секунду (burst 40), 60 `eph` в секунду. Превышение → `err RATE_LIMITED`, при систематическом превышении — close `4029`.
- Лимит подписок: 100 на соединение.
- Лимит соединений на пользователя: 10 (остальные → close `4029`).
- Глобальный лимит соединений на узел (конфиг) → HTTP 503 на upgrade, чтобы узел не умер под нагрузкой (load shedding).

## 5. Heartbeat и мёртвые соединения

- Сервер: WS `ping` каждые 25 сек, нет `pong` за 10 сек → `terminate()` (а не `close()`: half-open соединение не ответит на close handshake).
- Одна общая `setInterval` на весь узел, **не** таймер на каждое соединение (10k таймеров против 1 — замер памяти и CPU в LEARNINGS).
- Клиент: app-level `ping` каждые 20 сек; нет `pong` 10 сек → считает соединение мёртвым и переподключается. Нужно для браузера, который не видит WS ping, и для «тихо умерших» мобильных сетей.

## 6. Масштабирование и drain

```text
                ┌──────── HAProxy (leastconn, без sticky) ────────┐
clients ──────► │  node-1     node-2     node-3                    │
                └─────┬──────────┬──────────┬──────────────────────┘
                      └──────────┴──────────┴──► Redis (streams + sharded pub/sub)
```

- Sticky sessions не нужны: состояние канала в Redis, resume работает с любого узла.
- Несколько процессов на машине: отдельные контейнеры или `node:cluster`. Сравнить в LEARNINGS (cluster распределяет round-robin через primary-процесс; отдельные процессы за HAProxy проще и прозрачнее).
- **Drain** (SIGTERM):
  1. readiness → 503 (HAProxy перестаёт направлять новые соединения);
  2. всем клиентам `drain{after: random(0, 10000)}` → клиенты сами переподключаются с разбросом;
  3. через 15 сек оставшимся `close(1001)`;
  4. выход.
- **Сценарий «гроза переподключений»**: `docker kill node-2` с 10k соединений → все переподключаются к node-1 и node-3. Сравнить: без jitter (пик upgrade/сек, ошибки, p99 ticket-запросов) против экспоненциального backoff с full jitter. График в README — сильная иллюстрация.

## 7. Безопасность

- Проверка `Origin` при upgrade (allowlist).
- Ticket одноразовый, TTL 30 сек.
- Авторизация канала при `sub` и `pub` (хук `authorize(user, action, channel)`).
- `maxPayload`, лимиты подписок, rate limits, лимит соединений.
- Валидация всех входящих фреймов; неизвестный тип → `4400`.
- Данные сообщений не логируются (только метаданные).
- Server publish API — отдельный серверный ключ, сравнение через `timingSafeEqual`.

## 8. Метрики и наблюдаемость

Prometheus (`/metrics`):

| Метрика | Тип |
|---|---|
| `rt_connections` | gauge (по состоянию: active / lagging) |
| `rt_subscriptions`, `rt_channels_local` | gauge |
| `rt_messages_in_total{t}`, `rt_messages_out_total{kind}` | counter |
| `rt_fanout_duration_seconds` | histogram (от получения из Redis до отправки последнему подписчику) |
| `rt_ws_buffered_bytes` | histogram (сэмплирование раз в 5 сек) |
| `rt_slow_consumer_disconnects_total`, `rt_ephemeral_dropped_total` | counter |
| `rt_resume_messages` | histogram (сколько сообщений догружено при resume) |
| `rt_redis_command_seconds{cmd}` | histogram |
| `nodejs_eventloop_lag_p99_seconds`, `rt_event_loop_utilization` | gauge (`perf_hooks.monitorEventLoopDelay`, `performance.eventLoopUtilization`) |
| `nodejs_heap_*`, `rt_gc_pause_seconds{kind}` | через `PerformanceObserver('gc')` |

Grafana-дашборд: соединения по узлам, msgs/s, fan-out p99, event loop lag, heap, медленные клиенты, Redis-латентность.

Логи: pino, уровень `info` без логирования каждого сообщения. `debug` включается на конкретное соединение через admin-endpoint (без перезапуска).

## 9. Генератор нагрузки (`apps/loadgen`)

- Node + `worker_threads`: каждый воркер держит до ~10k соединений. Несколько контейнеров loadgen с разными IP, чтобы не упереться в ~28k эфемерных портов на пару src/dst IP. Это полезная деталь для README.
- Латентность считается через `ts` в сообщении: и публикующий, и принимающий клиент в одном процессе loadgen с общими часами.
- Сценарии:

| Сценарий | Параметры | Что меряем |
|---|---|---|
| `idle` | Наращивать до 50k соединений без сообщений | Память на соединение (heap + RSS), время установки, CPU на heartbeat |
| `fanout-big-room` | 1 комната, 10k подписчиков на 3 узлах, 5 msg/s | Латентность доставки p50/p95/p99, fan-out duration, event loop lag |
| `many-rooms` | 5000 комнат по 2–10 участников, 1 msg/10s на участника + курсоры 10 Hz | Суммарные msgs/s out, CPU, латентность |
| `slow-consumers` | как `many-rooms` + 5% клиентов не читают | Влияние на остальных, heap, отключения 4008 |
| `reconnect-storm` | 10k соединений, kill узла, jitter off/on | Пик upgrade/s, ошибки, время до полного восстановления |
| `resume` | Клиенты случайно отключаются на 1–30 сек | Корректность (0 пропусков по seq), объём догрузки |

- Проверка корректности во всех сценариях: loadgen отслеживает `seq` на каждый канал и клиент, считает пропуски и дубли. **Цель: 0 пропусков, дубли только в пределах resume.**

### Формат результатов в README

```text
Environment: 3 × node (1 vCPU / 1 GB each, Docker), Redis 7 (1 vCPU), Hetzner CPX41, Node 22.x
| Scenario             | Connections | Msgs out/s | p50   | p99    | Event loop p99 | Heap / conn |
| idle                 | 50,000      | —          | —     | —      | 3 ms           | ~9 KB       |
| fanout-big-room      | 10,000      | 50,000     | 11 ms | 48 ms  | 22 ms          | —           |
| many-rooms           | 20,000      | 120,000    | 6 ms  | 31 ms  | 14 ms          | —           |
```
(цифры выше — формат, реальные значения будут после замеров)

## 10. LEARNINGS.md (статья в репозитории)

Раздел, который превращает проект из «ещё одного WS-сервера» в демонстрацию глубины. Темы (каждая с цифрами до и после):
1. Сколько памяти стоит WebSocket-соединение в Node и из чего она складывается.
2. Serialize once: fan-out 10k без него и с ним.
3. Один общий heartbeat-таймер против таймера на соединение.
4. Разбиение большого fan-out на чанки через `setImmediate`: влияние на event loop lag и latency других комнат.
5. JSON против MessagePack: размер, CPU, итоговая латентность.
6. permessage-deflate: почему выключен.
7. Reconnect storm: jitter против его отсутствия (график).
8. Почему XADD и PUBLISH нужно делать атомарно (демонстрация перемешивания без Lua).
9. Эфемерные порты и файловые дескрипторы: во что упёрся генератор нагрузки и как это решено.

Эту статью можно опубликовать на dev.to или Хабре и дать ссылку в резюме.
