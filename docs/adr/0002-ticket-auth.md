# ADR 0002: One-time tickets for WebSocket authentication

- Status: accepted
- Date: 2026-10-01

## Context

The browser WebSocket API cannot set headers. Putting a long-lived JWT in the query string leaks it into proxy logs and browser history. Authentication must happen before the upgrade so an attacker cannot make the server allocate WebSocket state.

## Decision

- `POST /v1/tickets` with `Authorization: Bearer <JWT>` returns a random 32-byte base64url ticket stored as `ticket:{t}` with `EX 30`.
- The client connects to `/v1/connect?ticket=…`. In the `upgrade` handler, before `handleUpgrade`, the server runs `GETDEL` (atomic, single use). A missing or used ticket gets `HTTP 401` and `socket.destroy()`; no `WebSocket` object is created.
- `Origin` is checked against an allowlist before the ticket is consumed; the node-wide connection limit returns `503` before any Redis call.
- The SDK fetches a fresh ticket on every (re)connect through a user-supplied `getTicket()`.
- If the JWT behind a ticket expires while the socket is open, the shared heartbeat closes it with `4001`, and the SDK fetches a new ticket.

## Consequences

- A leaked URL is useless after 30 s or after first use.
- Tickets live in Redis, so any node can consume a ticket issued by another node, which keeps sticky sessions unnecessary (ADR 0003).
- Every connection costs one extra HTTP round trip and two Redis commands. In the reconnect storm this is the call whose p99 we report.
