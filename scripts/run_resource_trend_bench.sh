#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

PRESET="${PRESET:-o2}"
REQUEST_CASES="${REQUEST_CASES:-1000 5000 10000}"
CONCURRENCY_CASES="${CONCURRENCY_CASES:-32 64 128 256}"
INCLUDE_1E6="${INCLUDE_1E6:-0}"
BODY_CASES="${BODY_CASES:-0 1024}"
FIXED_REQUESTS="${FIXED_REQUESTS:-10000}"
FIXED_CONCURRENCY="${FIXED_CONCURRENCY:-128}"
DELAY_MS="${DELAY_MS:-5}"
RESPONSE_BYTES="${RESPONSE_BYTES:-1024}"
PROFILE="${PROFILE:-auto}"
WARMUP_PER_URL="${WARMUP_PER_URL:-512}"
CONCURRENT_WARMUP="${CONCURRENT_WARMUP:-1}"
STRICT_DETECT="${STRICT_DETECT:-1}"
H2_SESSIONS="${H2_SESSIONS:-4}"
H2_SHARDS="${H2_SHARDS:-0}"
H2_MAX_STREAMS="${H2_MAX_STREAMS:-128}"
ORIGIN_WAITERS="${ORIGIN_WAITERS:-128}"
H1_SHARDS="${H1_SHARDS:-0}"
H1_MAX_CONNECTIONS_PER_ORIGIN="${H1_MAX_CONNECTIONS_PER_ORIGIN:-0}"
STRIPE_H1_ORIGIN_SHARDS="${STRIPE_H1_ORIGIN_SHARDS:-0}"
RUN_CPP_CALLBACK="${RUN_CPP_CALLBACK:-1}"
RUN_CPP_ASYNCX="${RUN_CPP_ASYNCX:-1}"
RUN_GO_WORKER="${RUN_GO_WORKER:-1}"
RUN_GO_GATHER="${RUN_GO_GATHER:-1}"
H2_PORT="${H2_PORT:-8643}"
H1_PORT="${H1_PORT:-8645}"

if [[ "$INCLUDE_1E6" == "1" && "$REQUEST_CASES" != *1000000* ]]; then
  REQUEST_CASES="$REQUEST_CASES 1000000"
fi

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

extract_field() {
  local key="$1"
  awk -v k="$key" '
    BEGIN { RS="[ \n]"; FS="=" }
    $1 == k { print $2; found=1; exit }
    END { if (!found) exit 1 }
  ' || true
}

run_cpp() {
  local mode="$1"
  local requests="$2"
  local concurrency="$3"
  local body_bytes="$4"
  local gather_arg=()
  if [[ "$mode" == "cpp-asyncx" ]]; then
    gather_arg+=(--gather)
  fi
  local warmup_args=()
  if [[ "$CONCURRENT_WARMUP" == "1" ]]; then
    warmup_args+=(--concurrent-warmup)
  fi
  local strict_args=()
  if [[ "$STRICT_DETECT" == "1" ]]; then
    strict_args+=(--strict-detect)
  fi
  local profile_args=()
  case "$PROFILE" in
    auto)
      profile_args+=(--auto-profile)
      ;;
    balanced)
      profile_args+=(--balanced)
      ;;
    throughput)
      profile_args+=(--throughput)
      ;;
    *)
      echo "unknown PROFILE=$PROFILE" >&2
      exit 1
      ;;
  esac
  local h1_extra_args=()
  if [[ "$STRIPE_H1_ORIGIN_SHARDS" == "1" ]]; then
    h1_extra_args+=(--stripe-h1-origin-shards)
  fi

  "$ROOT_DIR/build-$PRESET/httpclient_bench" \
    --mixed --mixed-shuffle \
    "${gather_arg[@]}" \
    --url "$H2_URL" \
    --url-alt "$H1_URL" \
    --requests "$requests" \
    --concurrency "$concurrency" \
    --body-bytes "$body_bytes" \
    "${profile_args[@]}" \
    --warmup-per-url "$WARMUP_PER_URL" \
    "${warmup_args[@]}" \
    --insecure --no-proxy \
    "${strict_args[@]}" \
    --h2-sessions "$H2_SESSIONS" \
    --h2-shards "$H2_SHARDS" \
    --h2-max-streams "$H2_MAX_STREAMS" \
    --origin-waiters "$ORIGIN_WAITERS" \
    --h1-shards "$H1_SHARDS" \
    --h1-max-connections-per-origin "$H1_MAX_CONNECTIONS_PER_ORIGIN" \
    "${h1_extra_args[@]}"
}

run_go() {
  local mode="$1"
  local requests="$2"
  local concurrency="$3"
  local body_bytes="$4"
  local gather_arg=()
  if [[ "$mode" == "go-gather" ]]; then
    gather_arg+=(--gather)
  fi
  (
    cd "$ROOT_DIR"
    go run tools/go_compare/bench_client.go \
      --mixed --mixed-shuffle \
      "${gather_arg[@]}" \
      --url "$H2_URL" \
      --url-alt "$H1_URL" \
      --requests "$requests" \
      --concurrency "$concurrency" \
      --body-bytes "$body_bytes" \
      --warmup-per-url "$WARMUP_PER_URL" \
      --insecure --no-proxy
  )
}

print_cpp_row() {
  local axis="$1"
  local value="$2"
  local mode="$3"
  local requests="$4"
  local concurrency="$5"
  local body_bytes="$6"
  local output="$7"
  local wall p50 p95 p99 cpu_user cpu_system rss peak h1 h2 slot_waits cancels
  wall="$(printf '%s\n' "$output" | extract_field wall_ms)"
  p50="$(printf '%s\n' "$output" | extract_field p50_us)"
  p95="$(printf '%s\n' "$output" | extract_field p95_us)"
  p99="$(printf '%s\n' "$output" | extract_field p99_us)"
  cpu_user="$(printf '%s\n' "$output" | extract_field cpu_user_ms)"
  cpu_system="$(printf '%s\n' "$output" | extract_field cpu_system_ms)"
  rss="$(printf '%s\n' "$output" | extract_field rss_kb)"
  peak="$(printf '%s\n' "$output" | extract_field peak_rss_kb)"
  h1="$(printf '%s\n' "$output" | extract_field h1)"
  h2="$(printf '%s\n' "$output" | extract_field h2)"
  slot_waits="$(printf '%s\n' "$output" | extract_field h2_stream_slot_waits)"
  cancels="$(printf '%s\n' "$output" | extract_field h1_cancelled)"
  printf '| %s | %s | %s | %s | %s | %s | %s | %s/%s/%s | %s/%s | %s | %s | %s | %s | %s | %s | %s | %s | %s |\n' \
    "$axis" "$value" "$mode" "$body_bytes" "$requests" "$concurrency" \
    "${wall:-error}" "${p50:--}" "${p95:--}" "${p99:--}" \
    "${cpu_user:--}" "${cpu_system:--}" \
    "${rss:--}" "${peak:--}" "-" "-" "-" "$h1" "$h2" \
    "${slot_waits:-0}" "${cancels:-0}"
  if [[ -z "$wall" ]]; then
    printf '<details><summary>%s %s=%s output</summary>\n\n```text\n%s\n```\n</details>\n\n' \
      "$mode" "$axis" "$value" "$output"
  fi
}

print_go_row() {
  local axis="$1"
  local value="$2"
  local mode="$3"
  local requests="$4"
  local concurrency="$5"
  local body_bytes="$6"
  local output="$7"
  local wall p50 p95 p99 cpu_user cpu_system alloc sys heap stack gc h1 h2
  wall="$(printf '%s\n' "$output" | extract_field wall_ms)"
  p50="$(printf '%s\n' "$output" | extract_field p50_us)"
  p95="$(printf '%s\n' "$output" | extract_field p95_us)"
  p99="$(printf '%s\n' "$output" | extract_field p99_us)"
  cpu_user="$(printf '%s\n' "$output" | extract_field cpu_user_ms)"
  cpu_system="$(printf '%s\n' "$output" | extract_field cpu_system_ms)"
  alloc="$(printf '%s\n' "$output" | extract_field alloc_kb)"
  sys="$(printf '%s\n' "$output" | extract_field sys_kb)"
  heap="$(printf '%s\n' "$output" | extract_field heap_alloc_kb)"
  stack="$(printf '%s\n' "$output" | extract_field stack_inuse_kb)"
  gc="$(printf '%s\n' "$output" | extract_field num_gc)"
  h1="$(printf '%s\n' "$output" | extract_field h1)"
  h2="$(printf '%s\n' "$output" | extract_field h2)"
  printf '| %s | %s | %s | %s | %s | %s | %s | %s/%s/%s | %s/%s | - | - | %s | %s | %s/%s/%s | %s | %s | - | - |\n' \
    "$axis" "$value" "$mode" "$body_bytes" "$requests" "$concurrency" \
    "${wall:-error}" "${p50:--}" "${p95:--}" "${p99:--}" \
    "${cpu_user:--}" "${cpu_system:--}" \
    "${alloc:--}" "${sys:--}" "${heap:--}" "${stack:--}" \
    "${gc:--}" "$h1" "$h2"
  if [[ -z "$wall" ]]; then
    printf '<details><summary>%s %s=%s output</summary>\n\n```text\n%s\n```\n</details>\n\n' \
      "$mode" "$axis" "$value" "$output"
  fi
}

run_case() {
  local axis="$1"
  local value="$2"
  local requests="$3"
  local concurrency="$4"
  local body_bytes="$5"
  local output

  if [[ "$RUN_CPP_CALLBACK" == "1" ]]; then
    output="$(run_cpp cpp-callback "$requests" "$concurrency" "$body_bytes" 2>&1)" || true
    print_cpp_row "$axis" "$value" "cpp-callback" "$requests" "$concurrency" \
      "$body_bytes" "$output"
  fi

  if [[ "$RUN_CPP_ASYNCX" == "1" ]]; then
    output="$(run_cpp cpp-asyncx "$requests" "$concurrency" "$body_bytes" 2>&1)" || true
    print_cpp_row "$axis" "$value" "cpp-asyncx" "$requests" "$concurrency" \
      "$body_bytes" "$output"
  fi

  if [[ "$RUN_GO_WORKER" == "1" ]]; then
    output="$(run_go go-worker "$requests" "$concurrency" "$body_bytes" 2>&1)" || true
    print_go_row "$axis" "$value" "go-worker" "$requests" "$concurrency" \
      "$body_bytes" "$output"
  fi

  if [[ "$RUN_GO_GATHER" == "1" ]]; then
    output="$(run_go go-gather "$requests" "$concurrency" "$body_bytes" 2>&1)" || true
    print_go_row "$axis" "$value" "go-gather" "$requests" "$concurrency" \
      "$body_bytes" "$output"
  fi
}

cmake --preset "$PRESET" -DHTTPCLIENT_ENABLE_CURL_BASELINE=OFF >/dev/null
cmake --build --preset "$PRESET" --target httpclient_bench -j >/dev/null

(
  cd "$ROOT_DIR"
  PORT="$H2_PORT" DELAY_MS="$DELAY_MS" RESPONSE_BYTES="$RESPONSE_BYTES" \
    go run tools/go_compare/bench_https_server.go
) >/tmp/httpclient-trend-h2.log 2>&1 &
pids+=("$!")

(
  cd "$ROOT_DIR"
  PORT="$H1_PORT" HTTP1_ONLY=1 DELAY_MS="$DELAY_MS" RESPONSE_BYTES="$RESPONSE_BYTES" \
    go run tools/go_compare/bench_https_server.go
) >/tmp/httpclient-trend-h1.log 2>&1 &
pids+=("$!")

wait_for_server "$H2_URL"
wait_for_server "$H1_URL"

echo "resource_trend_bench preset=$PRESET profile=$PROFILE delay_ms=$DELAY_MS response_bytes=$RESPONSE_BYTES warmup_per_url=$WARMUP_PER_URL"
echo "h2_sessions=$H2_SESSIONS h2_shards=$H2_SHARDS h2_max_streams=$H2_MAX_STREAMS origin_waiters=$ORIGIN_WAITERS strict_detect=$STRICT_DETECT h1_shards=$H1_SHARDS h1_max_conn=$H1_MAX_CONNECTIONS_PER_ORIGIN stripe_h1_origin_shards=$STRIPE_H1_ORIGIN_SHARDS"
echo
echo "| axis | value | mode | body | requests | concurrency | wall_ms | p50/p95/p99_us | cpu_user/system_ms | cpp_rss_kb | cpp_peak_rss_kb | go_alloc_kb | go_sys_kb | go_heap/stack/gc | h1 | h2 | h2_slot_waits | h1_cancels |"
echo "|---|---:|---|---:|---:|---:|---:|---|---|---:|---:|---:|---:|---|---:|---:|---:|---:|"

for body_bytes in $BODY_CASES; do
  for concurrency in $CONCURRENCY_CASES; do
    run_case concurrency "$concurrency" "$FIXED_REQUESTS" "$concurrency" "$body_bytes"
  done
done

for body_bytes in $BODY_CASES; do
  for requests in $REQUEST_CASES; do
    run_case requests "$requests" "$requests" "$FIXED_CONCURRENCY" "$body_bytes"
  done
done
