# ADR 0006: Backpressure policy for slow consumers

- Status: accepted
- Date: 2026-10-01

## Context

`ws.send` never refuses data: if a client reads slower than we write, the bytes pile up in the socket's user-space buffer and the node's heap grows until it dies. One slow client must not hurt the others.

## Decision

Per connection, using `ws.bufferedAmount`:

- above HARD (4 MB): close `4008` immediately;
- above HIGH (1 MB) or already lagging: drop ephemeral frames (send one `lag{ch}` per channel per episode) and queue durable frames in a `pending` array (max 2000, else `4008`);
- a single 50 ms timer drains `pending` while `bufferedAmount < LOW (256 KB)`, returns the connection to `active` when empty, and closes `4008` after 30 s of lagging.

A `4008` is not message loss: the SDK reconnects immediately and resumes from its last `seq`.

## Consequences

- Memory per connection is bounded by HARD + pending, and queued buffers are the shared serialize-once buffers, so a queued message costs a pointer, not a copy.
- Ordering is preserved because once lagging, new durable frames always go behind `pending`.
- The thresholds are configuration; the slow-consumers load run uses scaled-down watermarks (16 KB / 64 KB / 512 KB, 10 s) because the laptop-sized traffic would otherwise need minutes to fill 1 MB.
