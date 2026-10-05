# ADR 0004: `terminate()` instead of `close()` for dead connections, one shared heartbeat timer

- Status: accepted
- Date: 2026-10-01

## Context

Half-open TCP connections (mobile network switch, laptop sleep, NAT timeout) never answer a close handshake. `ws.close()` waits for the peer's close frame; on a dead peer the socket and its buffers linger until the OS gives up (minutes).

## Decision

- One `setInterval` per node (tick = min(5 s, timeout / 2)) walks all connections. It sends a WebSocket `ping` when 25 s have passed since the last pong and calls `ws.terminate()` when a ping is unanswered for 10 s. It also closes connections whose JWT has expired with `4001`.
- `terminate()` destroys the socket immediately. `close(code)` is used only when the peer is alive and should learn the reason (`4008`, `4029`, `1001`); even then a 3 s fallback `terminate()` follows.
- A per-connection timer mode (`HEARTBEAT_MODE=per-connection`) exists only to measure the difference.

## Consequences

- Dead connections are released within 10–15 s, which bounds memory and file descriptors.
- Browsers cannot see WebSocket pings, so the SDK additionally sends an app-level `ping` every 20 s and treats a missing `pong` within 10 s as a dead connection.
- Measured with 4000 connections and a 1 s ping interval (`results/bench-heartbeat.json`): see LEARNINGS §3 for the CPU and heap difference between one shared timer and per-connection timers.
