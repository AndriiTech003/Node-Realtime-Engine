# ADR 0009: Cluster-wide per-user connection limit in Redis

- Status: accepted
- Date: 2026-10-05

## Context

SPEC §4 limits each user to 10 connections; any extra connection is closed with `4029`. The first build counted connections in a per-node `Map`. Without sticky sessions (ADR 0003), HAProxy spreads one user's tabs over every node, so with three nodes a user could hold 30 connections. The counter has to live in Redis. It must also survive the cases where nobody decrements it: a `kill -9` of a node, and a node that stalls long enough for its peers to declare it dead.

## Decision

- `user:{uid}:conns` is a hash of `nodeId → connections of this user on that node`, with the user id as the hash tag. The total for the user is the sum of the fields.
- **Acquire.** A Lua script sums the fields. If the sum is below the limit, it `HINCRBY`s the node's field and admits the connection; otherwise it rejects. The check and the increment are atomic, so two nodes cannot both admit the 10th connection. Acquire runs in the upgrade handler after the ticket is consumed and before `handleUpgrade`. A rejected client still gets a WebSocket and then `close(4029)`, as the spec and the SDK's backoff expect. If the socket goes away before the upgrade completes, the slot is released.
- **Release.** A Lua script `HINCRBY -1`s the field and deletes it at 0, never going below 0. It runs from the `ws` `close` event, which fires for client close, server close, `terminate()`, heartbeat timeouts and `4008` alike. Each connection releases at most once (`holdsUserSlot`).
- **Node index.** `node:{id}:users` is a set of the uids that have a field from that node. Each node adds a uid when its local count goes 0→1 and removes it after 1→0. Both commands go on the same Redis connection as the scripts, so they keep their order.
- **Node death.** The presence sweeper already detects nodes whose `node:{id}:alive` key has expired. For each dead node it `HDEL`s that node's field from every user in `node:{id}:users`, then deletes the set (`RedisStore.reapNode`). A graceful `stop()` does the same for its own node. A node restarted under the same id clears its previous incarnation's fields before it listens.
- **Self-healing.** If a release fails because Redis is unavailable, or a node finds out at its heartbeat refresh that it was reaped while alive (its `SADD nodes` returns 1), it rewrites its fields from the local counts in one pipeline. The rewrite is skipped while an acquire is in flight, because a pending reservation is not yet a confirmed count. The same rejoin also re-registers the node's presence entries.

## Consequences

- One user's limit holds across all nodes. Integration tests cover: 2+2 connections on two nodes followed by rejection on either node; concurrent connects through two nodes that admit exactly the limit; decrement on close, client terminate and server terminate; crash cleanup by a surviving node's sweeper; graceful stop; restart under the same id; recovery after being reaped while alive, including presence.
- Every connect costs one `SADD` and one `EVALSHA`, and every disconnect one `EVALSHA` (plus an `SREM` for the user's last connection on that node). These are small next to the ticket `GETDEL` the upgrade already does.
- After a crash, the dead node's connections still count against their users until the sweeper runs: up to TTL plus sweep interval, 15–25 s with the defaults. Clients of the dead node usually reconnect sooner than that. They get `4029` only if the stale count plus the reconnecting tabs exceeds the limit (for example 6 tabs on the dead node and a limit of 10). The SDK then backs off and retries, and the retry succeeds after the sweep.
- Setting `MAX_CONNECTIONS_PER_USER=0` disables the limit and skips Redis entirely.
