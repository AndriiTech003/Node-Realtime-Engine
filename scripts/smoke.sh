#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

export REDIS_PREFIX="rt:smoke:$(date +%s):"
export NODE_ALIVE_TTL_MS=3000
export NODE_ALIVE_REFRESH_MS=1000
export PRESENCE_SWEEP_MS=1000
export DRAIN_NOTIFY_DELAY_MS=0

cleanup() {
  node scripts/cluster.mjs down >/dev/null 2>&1 || true
  node scripts/purge-prefix.mjs "$REDIS_PREFIX" >/dev/null 2>&1 || true
}
trap cleanup EXIT

redis-cli -p 6379 ping >/dev/null || { echo "redis on 127.0.0.1:6379 is required (devinfra/start.sh)"; exit 1; }
command -v haproxy >/dev/null || { echo "haproxy binary is required (brew install haproxy)"; exit 1; }

for f in packages/server/dist/main.js apps/auth-stub/dist/main.js packages/client/dist/index.js; do
  [ -f "$f" ] || { echo "building workspace"; pnpm build >/dev/null; break; }
done

node scripts/cluster.mjs up
node scripts/smoke-check.mjs
