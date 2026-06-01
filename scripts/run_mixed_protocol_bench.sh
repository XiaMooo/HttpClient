#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

PRESET="${PRESET:-o2}"
REQUESTS="${REQUESTS:-10000}"
CONCURRENCY="${CONCURRENCY:-128}"
DELAY_MS="${DELAY_MS:-5}"
RESPONSE_BYTES="${RESPONSE_BYTES:-1024}"
H2_PORT="${H2_PORT:-8643}"
H1_PORT="${H1_PORT:-8645}"
H2_SESSIONS="${H2_SESSIONS:-4}"
H2_MAX_STREAMS="${H2_MAX_STREAMS:-128}"
ORIGIN_WAITERS="${ORIGIN_WAITERS:-128}"
MAX_CACHED_ORIGINS="${MAX_CACHED_ORIGINS:-4096}"
ORIGIN_CACHE_TTL_SEC="${ORIGIN_CACHE_TTL_SEC:-300}"
H2_FAILURE_TTL_SEC="${H2_FAILURE_TTL_SEC:-30}"
H1_SHARDS="${H1_SHARDS:-0}"
H1_MAX_CONNECTIONS_PER_ORIGIN="${H1_MAX_CONNECTIONS_PER_ORIGIN:-0}"
H1_MAX_ORIGINS_PER_SHARD="${H1_MAX_ORIGINS_PER_SHARD:-4096}"
H1_ORIGIN_IDLE_TTL_SEC="${H1_ORIGIN_IDLE_TTL_SEC:-300}"
H1_ACTOR_CONNECTIONS="${H1_ACTOR_CONNECTIONS:-8}"
BODY_CASES="${BODY_CASES:-0 1024}"
WARMUP_PER_URL="${WARMUP_PER_URL:-0}"
CONCURRENT_WARMUP="${CONCURRENT_WARMUP:-0}"
STRICT_DETECT="${STRICT_DETECT:-0}"
STRIPE_H1_ORIGIN_SHARDS="${STRIPE_H1_ORIGIN_SHARDS:-0}"
DISABLE_LIGHTWEIGHT_H1="${DISABLE_LIGHTWEIGHT_H1:-0}"
H1_ACTOR="${H1_ACTOR:-0}"

H2_URL="https://127.0.0.1:${H2_PORT}/ping"
H1_URL="https://127.0.0.1:${H1_PORT}/ping"

pids=()
cleanup() {
  for pid in "${pids[@]:-}"; do
    kill "$pid" >/dev/null 2>&1 || true
  done
  wait >/dev/null 2>&1 || true
}
trap cleanup EXIT

wait_for_server() {
  local url="$1"
  for _ in $(seq 1 100); do
    if curl -ksS --noproxy '*' "$url" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.05
  done
  echo "server did not become ready: $url" >&2
  return 1
}

cmake --preset "$PRESET" -DHTTPCLIENT_ENABLE_CURL_BASELINE=OFF >/dev/null
cmake --build --preset "$PRESET" --target httpclient_bench -j >/dev/null

(
  cd "$ROOT_DIR"
  PORT="$H2_PORT" DELAY_MS="$DELAY_MS" RESPONSE_BYTES="$RESPONSE_BYTES" \
    go run tools/go_compare/bench_https_server.go
) >/tmp/httpclient-mixed-h2.log 2>&1 &
pids+=("$!")

(
  cd "$ROOT_DIR"
  PORT="$H1_PORT" HTTP1_ONLY=1 DELAY_MS="$DELAY_MS" RESPONSE_BYTES="$RESPONSE_BYTES" \
    go run tools/go_compare/bench_https_server.go
) >/tmp/httpclient-mixed-h1.log 2>&1 &
pids+=("$!")

wait_for_server "$H2_URL"
wait_for_server "$H1_URL"

echo "mixed_protocol_bench preset=$PRESET requests=$REQUESTS concurrency=$CONCURRENCY delay_ms=$DELAY_MS response_bytes=$RESPONSE_BYTES"
echo "h2_url=$H2_URL h1_url=$H1_URL h2_sessions=$H2_SESSIONS h2_max_streams=$H2_MAX_STREAMS origin_waiters=$ORIGIN_WAITERS warmup_per_url=$WARMUP_PER_URL concurrent_warmup=$CONCURRENT_WARMUP strict_detect=$STRICT_DETECT"
echo "origin_cache max=$MAX_CACHED_ORIGINS ttl_sec=$ORIGIN_CACHE_TTL_SEC h2_failure_ttl_sec=$H2_FAILURE_TTL_SEC h1_shards=$H1_SHARDS h1_max_conn=$H1_MAX_CONNECTIONS_PER_ORIGIN h1_max_origins_per_shard=$H1_MAX_ORIGINS_PER_SHARD h1_origin_idle_ttl_sec=$H1_ORIGIN_IDLE_TTL_SEC"
echo

strict_args=()
if [[ "$STRICT_DETECT" == "1" ]]; then
  strict_args+=(--strict-detect)
fi

warmup_args=()
if [[ "$CONCURRENT_WARMUP" == "1" ]]; then
  warmup_args+=(--concurrent-warmup)
fi

h1_extra_args=()
if [[ "$STRIPE_H1_ORIGIN_SHARDS" == "1" ]]; then
  h1_extra_args+=(--stripe-h1-origin-shards)
fi
if [[ "$DISABLE_LIGHTWEIGHT_H1" == "1" ]]; then
  h1_extra_args+=(--disable-lightweight-h1)
fi
if [[ "$H1_ACTOR" == "1" ]]; then
  h1_extra_args+=(--h1-actor)
fi

for body_bytes in $BODY_CASES; do
  echo "== body_bytes=$body_bytes =="
  echo "-- cpp httpclient callback"
  "$ROOT_DIR/build-$PRESET/httpclient_bench" \
    --mixed --mixed-shuffle \
    --url "$H2_URL" \
    --url-alt "$H1_URL" \
    --requests "$REQUESTS" \
    --concurrency "$CONCURRENCY" \
    --body-bytes "$body_bytes" \
    --warmup-per-url "$WARMUP_PER_URL" \
    "${warmup_args[@]}" \
    --insecure --no-proxy \
    "${strict_args[@]}" \
    --h2-sessions "$H2_SESSIONS" \
    --h2-max-streams "$H2_MAX_STREAMS" \
    --origin-waiters "$ORIGIN_WAITERS" \
    --max-cached-origins "$MAX_CACHED_ORIGINS" \
    --origin-cache-ttl-sec "$ORIGIN_CACHE_TTL_SEC" \
    --h2-failure-ttl-sec "$H2_FAILURE_TTL_SEC" \
    --h1-shards "$H1_SHARDS" \
    --h1-max-connections-per-origin "$H1_MAX_CONNECTIONS_PER_ORIGIN" \
    --h1-max-origins-per-shard "$H1_MAX_ORIGINS_PER_SHARD" \
    --h1-origin-idle-ttl-sec "$H1_ORIGIN_IDLE_TTL_SEC" \
    --h1-actor-connections "$H1_ACTOR_CONNECTIONS" \
    "${h1_extra_args[@]}"

  echo "-- cpp httpclient asyncx gather"
  "$ROOT_DIR/build-$PRESET/httpclient_bench" \
    --mixed --mixed-shuffle \
    --gather \
    --url "$H2_URL" \
    --url-alt "$H1_URL" \
    --requests "$REQUESTS" \
    --concurrency "$CONCURRENCY" \
    --body-bytes "$body_bytes" \
    --warmup-per-url "$WARMUP_PER_URL" \
    "${warmup_args[@]}" \
    --insecure --no-proxy \
    "${strict_args[@]}" \
    --h2-sessions "$H2_SESSIONS" \
    --h2-max-streams "$H2_MAX_STREAMS" \
    --origin-waiters "$ORIGIN_WAITERS" \
    --max-cached-origins "$MAX_CACHED_ORIGINS" \
    --origin-cache-ttl-sec "$ORIGIN_CACHE_TTL_SEC" \
    --h2-failure-ttl-sec "$H2_FAILURE_TTL_SEC" \
    --h1-shards "$H1_SHARDS" \
    --h1-max-connections-per-origin "$H1_MAX_CONNECTIONS_PER_ORIGIN" \
    --h1-max-origins-per-shard "$H1_MAX_ORIGINS_PER_SHARD" \
    --h1-origin-idle-ttl-sec "$H1_ORIGIN_IDLE_TTL_SEC" \
    --h1-actor-connections "$H1_ACTOR_CONNECTIONS" \
    "${h1_extra_args[@]}"

  echo "-- go net/http worker"
  (
    cd "$ROOT_DIR"
    go run tools/go_compare/bench_client.go \
      --mixed --mixed-shuffle \
      --url "$H2_URL" \
      --url-alt "$H1_URL" \
      --requests "$REQUESTS" \
      --concurrency "$CONCURRENCY" \
      --body-bytes "$body_bytes" \
      --warmup-per-url "$WARMUP_PER_URL" \
      --insecure --no-proxy
  )

  echo "-- go net/http gather"
  (
    cd "$ROOT_DIR"
    go run tools/go_compare/bench_client.go \
      --mixed --mixed-shuffle \
      --gather \
      --url "$H2_URL" \
      --url-alt "$H1_URL" \
      --requests "$REQUESTS" \
      --concurrency "$CONCURRENCY" \
      --body-bytes "$body_bytes" \
      --warmup-per-url "$WARMUP_PER_URL" \
      --insecure --no-proxy
  )
  echo
done
