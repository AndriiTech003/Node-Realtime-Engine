# ADR 0001: Atomic Lua publish (INCR + XADD + SPUBLISH in one script)

- Status: accepted
- Date: 2026-10-01

## Context

Every durable channel needs one total order that all subscribers on all nodes observe, plus a history that a reconnecting client can replay from any node. Any node can accept a publish. The order must survive concurrent publishers on different nodes.

## Decision

A single Lua script (`packages/server/src/lua.ts`, `PUBLISH_SCRIPT`) does, atomically inside Redis:

1. `GET ch:{name}:cmid:{cmid}`; if present return the stored `seq|mid` (idempotent retry).
2. `INCR ch:{name}:seq`.
3. `XADD ch:{name}:log MAXLEN ~ 10000 0-{seq} p <payload> ts <ts>`; the stream id *is* the sequence number.
4. `SET ch:{name}:cmid:{cmid} seq|mid EX 300`.
5. `SPUBLISH fan:{name} seq|ts|payload`.
6. Amortised 24 h retention: look at the 32 oldest entries and `XTRIM MINID` past the expired ones.

All keys share the `{name}` hash tag, so the script is valid on Redis Cluster too.

## Consequences

- The pub/sub order equals the `seq` order for everyone, because nothing else can run between `INCR` and `SPUBLISH`.
- `XADD` with an explicit id can never be rejected for being "smaller than the top item".
- Measured (`packages/server/bench/atomicity.mjs`, 8 concurrent publishers × 500 messages): the same three commands issued separately produced 1205 out-of-order deliveries and 993 rejected `XADD`s; the Lua version produced 0 and 0, and was 1.9× faster (38 018 vs 19 581 publishes/s) because it is one round trip.
- Cost: one script execution per publish holds the Redis event loop for a few microseconds; that is the throughput ceiling of a single channel shard. Channels are spread over slots, so a Redis Cluster scales it horizontally.
