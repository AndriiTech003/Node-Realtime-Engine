# ADR 0003: No sticky sessions

- Status: accepted
- Date: 2026-10-01

## Context

The cluster runs three nodes behind HAProxy. Sticky sessions would let a node keep per-user state in memory, but they would also pin a reconnecting client to the node that just died or is draining, and they skew load after a node restart.

## Decision

- HAProxy uses `balance leastconn` without stickiness, `option httpchk GET /health/ready` with `inter 1s fall 2`, and `option redispatch` + `retries 3`.
- All state needed to continue a session lives in Redis: tickets, sequence counters, history streams, cmid idempotency keys, presence hashes.
- A client resumes on any node by sending `sub {from: lastSeq}`.

## Consequences

- `kill -9` of a node is invisible to correctness: the smoke test kills the node a client is on and verifies gapless resume on another node (`scripts/smoke-check.mjs`), and the reconnect-storm runs report 0 gaps.
- A node only subscribes to the shard channels of the channels its local clients use, so cross-node traffic stays proportional to real interest.
- Without `option redispatch`, a ticket request that HAProxy had already routed to the dead node waited for three connection retries; that showed up as a ~3 s ticket p99 in an early storm run. With redispatch the retry goes to a live node.
