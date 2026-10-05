#!/usr/bin/env bash
set -uo pipefail
cd "$(dirname "$0")/.."

OUT="${OUT:-results}"
ONLY="${ONLY:-}"
mkdir -p "$OUT"
export REDIS_PREFIX="rt:bench:"
export LOG_LEVEL=warn
LG="node apps/loadgen/dist/cli.js"

want() { [ -z "$ONLY" ] || [[ ",$ONLY," == *",$1,"* ]]; }

cluster_up() {
  node scripts/cluster.mjs down >/dev/null 2>&1 || true
  node scripts/purge-prefix.mjs "$REDIS_PREFIX" >/dev/null 2>&1 || true
  local args=()
  for kv in "$@"; do args+=(--env "$kv"); done
  node scripts/cluster.mjs up --no-auth ${args[@]+"${args[@]}"} >/dev/null
}

cleanup() {
  node scripts/cluster.mjs down >/dev/null 2>&1 || true
  node scripts/purge-prefix.mjs "$REDIS_PREFIX" >/dev/null 2>&1 || true
}
trap cleanup EXIT

if want micro; then
  echo "== micro benchmarks"
  (cd packages/server && BENCH_CONNECTIONS=4000 BENCH_HOLD_MS=30000 BENCH_OUT="../../$OUT/bench-memory.json" node bench/memory-per-connection.mjs)
  (cd packages/server && BENCH_CONNECTIONS=4000 BENCH_HOLD_MS=20000 BENCH_HB_MS=1000 BENCH_MODES=engine,engine-per-connection-timers BENCH_OUT="../../$OUT/bench-heartbeat.json" node bench/memory-per-connection.mjs)
  (cd packages/server && BENCH_OUT="../../$OUT/bench-serialize-once.json" node --expose-gc bench/serialize-once.mjs)
  (cd packages/server && BENCH_OUT="../../$OUT/bench-codec-validator.json" node bench/codec-validator.mjs)
  (cd packages/server && BENCH_OUT="../../$OUT/bench-atomicity.json" node bench/atomicity.mjs)
fi

if want default; then
  echo "== default cluster"
  cluster_up
  $LG idle --connections 9000 --duration 60 --steps 3 --label idle --out "$OUT"
  cluster_up
  $LG fanout-big-room --connections 6000 --duration 60 --probes 20 --label fanout-big-room --out "$OUT"
  cluster_up
  $LG many-rooms --connections 1000 --duration 60 --label many-rooms --out "$OUT"
  cluster_up
  $LG many-rooms --connections 3000 --duration 60 --label many-rooms-3000-overload --out "$OUT"
  cluster_up
  $LG many-rooms --connections 6000 --duration 60 --label many-rooms-6000-overload --out "$OUT"
  cluster_up
  $LG resume --connections 3000 --duration 120 --label resume --out "$OUT"
fi

if want manyrooms; then
  cluster_up
  $LG many-rooms --connections 1000 --duration 60 --label many-rooms --out "$OUT"
fi

if want storm; then
  echo "== reconnect storm"
  cluster_up
  $LG reconnect-storm --connections 4500 --duration 30 --ticket-concurrency 128 --jitter off --label reconnect-storm-no-jitter --out "$OUT"
  cluster_up
  $LG reconnect-storm --connections 4500 --duration 30 --ticket-concurrency 128 --jitter on --label reconnect-storm-jitter --out "$OUT"
fi

if want slow; then
  echo "== slow consumers with scaled watermarks"
  SCALED=(BP_LOW_BYTES=16384 BP_HIGH_BYTES=65536 BP_HARD_BYTES=524288 BP_LAG_TIMEOUT_MS=10000)
  cluster_up "${SCALED[@]}"
  $LG many-rooms --connections 1000 --duration 90 --rate 1 --payload 2048 --label many-rooms-scaled-watermarks --out "$OUT"
  cluster_up "${SCALED[@]}"
  $LG slow-consumers --connections 1000 --duration 90 --rate 1 --payload 2048 --label slow-consumers --out "$OUT"
fi

if want optimizations; then
  echo "== optimisation comparisons"
  cluster_up FANOUT_SERIALIZE_ONCE=false
  $LG fanout-big-room --connections 6000 --duration 45 --label fanout-serialize-per-subscriber --out "$OUT"
  cluster_up
  $LG fanout-big-room --connections 6000 --duration 45 --label fanout-serialize-once --out "$OUT"
  cluster_up FANOUT_CHUNK_THRESHOLD=0
  $LG fanout-big-room --connections 6000 --duration 45 --probes 20 --nodes http://127.0.0.1:4301 --label chunking-off-single-node --out "$OUT"
  cluster_up
  $LG fanout-big-room --connections 6000 --duration 45 --probes 20 --nodes http://127.0.0.1:4301 --label chunking-on-single-node --out "$OUT"
  cluster_up FANOUT_CHUNK_THRESHOLD=0
  $LG fanout-big-room --connections 6000 --duration 45 --probes 20 --rate 20 --payload 1024 --nodes http://127.0.0.1:4301 --label chunking-off-single-node-overload --out "$OUT"
  cluster_up
  $LG fanout-big-room --connections 6000 --duration 45 --probes 20 --rate 20 --payload 1024 --nodes http://127.0.0.1:4301 --label chunking-on-single-node-overload --out "$OUT"
  cluster_up
  $LG fanout-big-room --connections 6000 --duration 45 --codec msgpack --label fanout-msgpack --out "$OUT"
  cluster_up PERMESSAGE_DEFLATE=true
  $LG fanout-big-room --connections 6000 --duration 45 --deflate --label fanout-deflate --out "$OUT"
fi

echo "done; results in $OUT"
