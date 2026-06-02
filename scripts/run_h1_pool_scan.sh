#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

PRESET="${PRESET:-o2}"
REQUESTS="${REQUESTS:-10000}"
CONCURRENCY="${CONCURRENCY:-512}"
DELAY_MS="${DELAY_MS:-5}"
RESPONSE_BYTES="${RESPONSE_BYTES:-1024}"
WARMUP_PER_URL="${WARMUP_PER_URL:-128}"
MAX_CONN_CASES="${MAX_CONN_CASES:-64 128 256 512}"
PORT="${PORT:-8945}"
RUN_ASYNCX="${RUN_ASYNCX:-1}"

URL="https://127.0.0.1:${PORT}/ping"

pids=()
cleanup() {
  for pid in "${pids[@]:-}"; do
    kill "$pid" >/dev/null 2>&1 || true
  done
  wait >/dev/null 2>&1 || true
}
trap cleanup EXIT

extract_field() {
  local key="$1"
  awk -v k="$key" '
    BEGIN { RS="[ \n]"; FS="=" }
    $1 == k { print $2; found=1; exit }
    END { if (!found) exit 1 }
  ' || true
}

print_row() {
  local max_conn="$1"
  local mode="$2"
  local output="$3"
  local wall p50 p95 p99 cpu_user cpu_system rss peak created idle_hit
  wall="$(printf '%s\n' "$output" | extract_field wall_ms)"
  p50="$(printf '%s\n' "$output" | extract_field p50_us)"
  p95="$(printf '%s\n' "$output" | extract_field p95_us)"
  p99="$(printf '%s\n' "$output" | extract_field p99_us)"
  cpu_user="$(printf '%s\n' "$output" | extract_field cpu_user_ms)"
  cpu_system="$(printf '%s\n' "$output" | extract_field cpu_system_ms)"
  rss="$(printf '%s\n' "$output" | extract_field rss_kb)"
  peak="$(printf '%s\n' "$output" | extract_field peak_rss_kb)"
  created="$(printf '%s\n' "$output" | extract_field h1_conn_created)"
  idle_hit="$(printf '%s\n' "$output" | extract_field h1_idle_hit)"
  printf '| %s | %s | %s | %s/%s/%s | %s/%s | %s | %s | %s | %s |\n' \
    "$max_conn" "$mode" "${wall:-error}" "${p50:--}" "${p95:--}" "${p99:--}" \
    "${cpu_user:--}" "${cpu_system:--}" "${rss:--}" "${peak:--}" \
    "${created:--}" "${idle_hit:--}"
}

run_case() {
  local max_conn="$1"
  local mode="$2"
  local gather_arg=()
  if [[ "$mode" == "cpp-asyncx" ]]; then
    gather_arg+=(--gather)
  fi
  "$ROOT_DIR/build-$PRESET/httpclient_bench" \
    "${gather_arg[@]}" \
    --url "$URL" \
    --requests "$REQUESTS" \
    --concurrency "$CONCURRENCY" \
    --warmup-per-url "$WARMUP_PER_URL" \
    --concurrent-warmup \
    --strict-detect \
    --insecure --no-proxy \
    --h1-max-connections-per-origin "$max_conn"
}

cmake --preset "$PRESET" -DHTTPCLIENT_ENABLE_CURL_BASELINE=OFF >/dev/null
cmake --build --preset "$PRESET" --target httpclient_bench -j >/dev/null

(
  cd "$ROOT_DIR"
  PORT="$PORT" HTTP1_ONLY=1 DELAY_MS="$DELAY_MS" RESPONSE_BYTES="$RESPONSE_BYTES" \
    go run tools/go_compare/bench_https_server.go
) >/tmp/httpclient-h1-pool-scan.log 2>&1 &
pids+=("$!")

for _ in $(seq 1 100); do
  if curl -ksS --noproxy '*' "$URL" >/dev/null 2>&1; then
    break
  fi
  sleep 0.05
done

echo "h1_pool_scan preset=$PRESET requests=$REQUESTS concurrency=$CONCURRENCY delay_ms=$DELAY_MS response_bytes=$RESPONSE_BYTES warmup_per_url=$WARMUP_PER_URL"
echo
echo "| max_conn | mode | wall_ms | p50/p95/p99_us | cpu_user/system_ms | rss_kb | peak_rss_kb | h1_created | h1_idle_hit |"
echo "|---:|---|---:|---|---|---:|---:|---:|---:|"

for max_conn in $MAX_CONN_CASES; do
  output="$(run_case "$max_conn" cpp-callback 2>&1)" || true
  print_row "$max_conn" cpp-callback "$output"
  if [[ "$RUN_ASYNCX" == "1" ]]; then
    output="$(run_case "$max_conn" cpp-asyncx 2>&1)" || true
    print_row "$max_conn" cpp-asyncx "$output"
  fi
done
