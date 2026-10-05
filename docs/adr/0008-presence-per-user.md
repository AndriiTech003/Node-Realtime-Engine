# ADR 0008: Presence by user, with a per-channel user counter

- Status: accepted
- Date: 2026-10-01

## Context

The spec stores presence per connection (`pres:{ch}` hash and `pres:{ch}:exp` zset) and emits join/leave per user ("three tabs = one avatar"). Deciding "is this the first/last connection of the user" by scanning the hash is O(n) per join and becomes quadratic for big rooms.

## Decision

- Add `pres:{ch}:users` (hash `uid → connection count`) in the same hash slot. The join/leave Lua scripts `HINCRBY` it and publish `pj`/`pl` only on 0→1 and 1→0.
- A per-node set `node:{id}:pres` lists the node's presence entries. Every node refreshes `node:{id}:alive` (PX 15 s). A sweeper guarded by a `SET NX PX` lock removes entries of nodes whose alive key vanished and entries whose `exp` score passed (45 s TTL, refreshed every 15 s in one pipeline).
- The presence list in `ok` is capped (`presenceListLimit`, 1000 users) and carries the total `pn`. Clients can opt out with `sub {presence: false}` (an additive, backwards-compatible field).

## Consequences

- O(1) join/leave decisions; a killed node's users disappear in ~TTL + sweep interval (about 3–5 s with the smoke-test settings, about 15–25 s with defaults).
- One extra key per channel compared with the spec table; documented in IMPLEMENTATION_NOTES.
