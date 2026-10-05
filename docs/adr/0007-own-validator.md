# ADR 0007: Own frame validator instead of zod

- Status: accepted
- Date: 2026-10-01

## Context

Every inbound frame must be validated. The allowed dependencies are `ws`, `ioredis`, `pino`, `prom-client` and `msgpackr`. Validation runs on the hot path for every `pub` and every cursor update.

## Decision

`packages/protocol/src/validator.ts` provides ~200 lines of combinators (`str`, `int`, `json`, `obj`, `opt`, `discriminated`) with static type inference. Client frames are a discriminated union on `t`; unknown types close the socket with `4400`, a known type with bad fields gets `err BAD_REQUEST`.

## Consequences

- Measured (`results/bench-codec-validator.json`): 1.9–8.8× more frames per second than an equivalent zod 4 schema, with no dependency.
- The validator is shared with the SDK, which validates server frames and ignores unknown frame types (forward compatibility).
