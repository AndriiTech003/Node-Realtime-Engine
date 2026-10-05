# ADR 0005: Serialize once per codec, chunk big fan-outs with setImmediate

- Status: accepted
- Date: 2026-10-01

## Context

A message to a room with N local subscribers used to cost N `JSON.stringify` calls and N string-to-Buffer conversions inside `ws.send`. For a 10 000-subscriber room the synchronous loop also blocks the event loop for tens of milliseconds, delaying every other room on the node.

## Decision

- A message from Redis becomes an `OutboundFrame` that lazily produces one `Buffer` per codec (JSON, MessagePack) and hands the same `Buffer` to every subscriber. For JSON the frame is spliced from the stored payload without parsing it at all.
- If a channel has more than 1000 local subscribers, delivery runs in chunks of 500 with `setImmediate` between chunks. A per-channel queue keeps message order while a chunked delivery is in progress.
- Both behaviours have switches (`FANOUT_SERIALIZE_ONCE`, `FANOUT_CHUNK_THRESHOLD`) used only for the before/after measurements in LEARNINGS.

## Consequences

- Encoding cost per message no longer grows with room size.
- Chunking trades a little latency for the big room itself for bounded event loop delay for everyone else.
