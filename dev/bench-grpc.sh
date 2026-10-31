#!/usr/bin/env bash
# The gRPC load run: the release `icetray serve` over the benchmark
# program's local tables (crates/icetray-testkit/examples/bench.rs), driven
# by ghz. Three runs of RUN_SECONDS each against the long-key table: `Get`
# at a fixed rate below saturation, `Get` at saturation, and `MultiGet` of
# MULTI_GET_KEYS keys at saturation, after one warm-up that fills the page
# cache. Results are ghz's JSON, one file per run, and the server's metrics
# scrape, under OUT_DIR; the script prints where they went.
#
# ICETRAY_BENCH_PROFILE=1 builds the profiling profile instead of release
# and records the server under `samply record` for the whole run, saving
# the profile beside the results.
#
# ghz and the server share the machine, so saturation here is the pair's,
# not the server's alone; the fixed-rate run, which leaves both with idle
# cores, is the one to read for latency.
set -euo pipefail

cd "$(dirname "$0")/.."

# Ports beside the defaults (7433 and 9433), so a server a developer already
# runs on the defaults does not collide with this one.
GRPC_PORT=17433
METRICS_PORT=19433

# Seconds per measured run: long enough that the 99th percentile rests on
# hundreds of thousands of requests at the rates this server reaches.
RUN_SECONDS=30

# Seconds of the unmeasured warm-up: at tens of thousands of uniform keys a
# second it asks every one of the table's two thousand value pages, so the
# measured runs start with the page cache holding them all.
WARMUP_SECONDS=15

# Requests per second of the fixed-rate run: about a third of the 60,000
# Gets per second this script measured at saturation on an 11-core Apple M3
# Pro, so no request queues behind another and the latency is the server's
# own.
FIXED_RPS=20000

# Concurrent requests and connections of the saturating runs: many more
# requests in flight than the machine has cores, spread over several HTTP/2
# connections so one connection's stream handling is not the limit; far
# below `server.max_in_flight`, so nothing is refused.
SATURATION_CONCURRENCY=128
SATURATION_CONNECTIONS=16

# One worker per request in flight for the fixed-rate run, so ghz can keep
# its schedule when a response is slow.
FIXED_CONCURRENCY=64

# Keys per MultiGet, the batch the in-process benchmark also measures.
MULTI_GET_KEYS=100

# The cache budget (`server.cache_bytes`): a gibibyte holds every value page
# of the four prepared tables (about 200 MB each). The run sets
# `server.row_cache_share = 0`, which gives the row cache no part of the
# budget: every table skips the row cache, so no lookup reads or fills it,
# and the page cache's memory level holds the whole budget, as under the
# in-process benchmark's `--row-cache off` and as
# docs/analysis/2026-09-26-performance-sweep.md measured; after the warm-up
# every hit is warm and no run pays for a page read.
CACHE_BYTES=1073741824

# Seconds to wait for /ready: the first loads of the four tables take well
# under a second from local files; two minutes bounds a start that has
# wedged.
READY_TIMEOUT_SECONDS=120

# The long-key fixture's key is three times its id (KEY_STEP in
# crates/icetray-testkit/src/keys.rs), so a template that multiplies a
# random id by it asks only keys the table holds.
KEY_STEP=3

OUT_DIR="target/bench/grpc/$(date -u +%Y%m%dT%H%M%SZ)"
TABLES_DIR="target/bench/tables"
PROTO="crates/icetray-proto/proto/icetray.proto"
mkdir -p "$OUT_DIR"

log() { echo "==> $*" >&2; }

if [[ "${ICETRAY_BENCH_PROFILE:-0}" == "1" ]]; then
  CARGO_PROFILE=profiling
else
  CARGO_PROFILE=release
fi

log "cargo build --profile $CARGO_PROFILE (icetray, bench)"
cargo build --profile "$CARGO_PROFILE" -p icetray --bin icetray
cargo build --profile "$CARGO_PROFILE" -p icetray-testkit --example bench
SERVER="target/$CARGO_PROFILE/icetray"
BENCH="target/$CARGO_PROFILE/examples/bench"

log "tables under $TABLES_DIR"
# The four key-value tables of random and text values: the cache budget above
# is sized for them, the protobuf tables' second column is not served here,
# and the ordered table needs an order column the entries below omit.
"$BENCH" --prepare --table long,string --values random,text --data-dir "$TABLES_DIR" >"$OUT_DIR/tables.tsv"

CONFIG="$OUT_DIR/icetray.toml"
{
  echo "[server]"
  echo "listen = \"127.0.0.1:$GRPC_PORT\""
  echo "metrics_listen = \"127.0.0.1:$METRICS_PORT\""
  echo "cache_bytes = $CACHE_BYTES"
  echo "row_cache_share = 0"
  while IFS=$'\t' read -r name _key_type _rows location; do
    echo
    echo "[[tables]]"
    echo "name = \"$name\""
    echo "ref = \"main\""
    echo "key_column = \"key\""
    echo "value_columns = [\"value\"]"
    echo
    echo "[tables.source]"
    echo "type = \"static\""
    echo "location = \"$location\""
  done <"$OUT_DIR/tables.tsv"
} >"$CONFIG"
ROWS="$(awk -F'\t' '$1 == "long" { print $3 }' "$OUT_DIR/tables.tsv")"

SERVER_LOG="$OUT_DIR/server.log"
PROFILE_FILE="$OUT_DIR/server.json.gz"
if [[ "$CARGO_PROFILE" == "profiling" ]]; then
  log "serving under samply"
  samply record --save-only --unstable-presymbolicate -o "$PROFILE_FILE" -- \
    "$SERVER" serve --config "$CONFIG" >"$SERVER_LOG" 2>&1 &
else
  log "serving"
  "$SERVER" serve --config "$CONFIG" >"$SERVER_LOG" 2>&1 &
fi
WRAPPER_PID=$!

# The server's own pid: the background job itself, or samply's child.
server_pid() {
  if [[ "$CARGO_PROFILE" == "profiling" ]]; then
    pgrep -P "$WRAPPER_PID" || true
  else
    echo "$WRAPPER_PID"
  fi
}

stop_server() {
  local pid
  pid="$(server_pid)"
  if [[ -n "$pid" ]]; then
    kill -TERM "$pid" 2>/dev/null || true
  fi
  wait "$WRAPPER_PID" 2>/dev/null || true
}
trap stop_server EXIT

log "waiting for /ready"
deadline=$((SECONDS + READY_TIMEOUT_SECONDS))
until curl -fsS "http://127.0.0.1:$METRICS_PORT/ready" >/dev/null 2>&1; do
  if ((SECONDS > deadline)); then
    echo "the server was not ready within $READY_TIMEOUT_SECONDS s; see $SERVER_LOG" >&2
    exit 1
  fi
  if ! kill -0 "$WRAPPER_PID" 2>/dev/null; then
    echo "the server exited before it was ready; see $SERVER_LOG" >&2
    exit 1
  fi
  sleep 0.5
done

# ghz expands templates inside JSON string values only, so each key is a
# string, which protobuf's JSON mapping accepts for an int64; a MultiGet's
# keys are written out one template each.
KEY_TEMPLATE="{\"long\":\"{{mul $KEY_STEP (randomInt 0 $ROWS)}}\"}"
GET_DATA="{\"table\":\"long\",\"key\":$KEY_TEMPLATE,\"columns\":[{\"name\":\"value\"}]}"
MULTI_KEYS="$KEY_TEMPLATE"
for ((i = 1; i < MULTI_GET_KEYS; i++)); do
  MULTI_KEYS="$MULTI_KEYS,$KEY_TEMPLATE"
done
MULTI_DATA="{\"table\":\"long\",\"keys\":[$MULTI_KEYS],\"columns\":[{\"name\":\"value\"}]}"

# --duration-stop wait: ghz's default, close, ends a run by closing its
# connections with calls still in flight and reports each as Unavailable,
# and the server sees those calls abandoned; waiting lets them finish, so
# every call a run sends is answered and counted on both sides.
ghz_run() {
  local name="$1"
  shift
  log "ghz $name"
  ghz --duration-stop wait --insecure --proto "$PROTO" --format json \
    --output "$OUT_DIR/$name.json" "$@" "127.0.0.1:$GRPC_PORT"
}

ghz_run warmup --call icetray.v1.Icetray.Get --data "$GET_DATA" \
  --duration "${WARMUP_SECONDS}s" --concurrency "$SATURATION_CONCURRENCY" \
  --connections "$SATURATION_CONNECTIONS"
ghz_run get-fixed-rate --call icetray.v1.Icetray.Get --data "$GET_DATA" \
  --duration "${RUN_SECONDS}s" --rps "$FIXED_RPS" --concurrency "$FIXED_CONCURRENCY" \
  --connections "$SATURATION_CONNECTIONS"
ghz_run get-saturation --call icetray.v1.Icetray.Get --data "$GET_DATA" \
  --duration "${RUN_SECONDS}s" --concurrency "$SATURATION_CONCURRENCY" \
  --connections "$SATURATION_CONNECTIONS"
ghz_run multi-get-saturation --call icetray.v1.Icetray.MultiGet --data "$MULTI_DATA" \
  --duration "${RUN_SECONDS}s" --concurrency "$SATURATION_CONCURRENCY" \
  --connections "$SATURATION_CONNECTIONS"

curl -fsS "http://127.0.0.1:$METRICS_PORT/metrics" >"$OUT_DIR/metrics.txt"
stop_server
trap - EXIT

echo "results: $OUT_DIR"
echo "  ghz: $OUT_DIR/{get-fixed-rate,get-saturation,multi-get-saturation}.json"
echo "  server log: $SERVER_LOG; metrics scrape: $OUT_DIR/metrics.txt"
if [[ "$CARGO_PROFILE" == "profiling" ]]; then
  echo "  profile: $PROFILE_FILE"
fi
