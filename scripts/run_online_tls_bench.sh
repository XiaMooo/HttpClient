#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

PRESET="${PRESET:-o2}"
URL_H2="${URL_H2:-https://www.google.com/generate_204}"
URL_H1="${URL_H1:-$URL_H2}"
REQUESTS_COLD="${REQUESTS_COLD:-8}"
REQUESTS_WARM="${REQUESTS_WARM:-32}"
CONCURRENCY_COLD="${CONCURRENCY_COLD:-1}"
CONCURRENCY_WARM="${CONCURRENCY_WARM:-4}"
WARMUP="${WARMUP:-4}"
H2_SESSIONS="${H2_SESSIONS:-1}"
H2_MAX_STREAMS="${H2_MAX_STREAMS:-128}"
BODY_BYTES="${BODY_BYTES:-0}"
INSECURE="${INSECURE:-0}"
DISABLE_PROXY="${DISABLE_PROXY:-0}"
RUN_CURL="${RUN_CURL:-1}"
RUN_LIBCURL="${RUN_LIBCURL:-0}"
RUN_ASIO_H1="${RUN_ASIO_H1:-1}"
RUN_H2="${RUN_H2:-1}"
RUN_HTTPCLIENT="${RUN_HTTPCLIENT:-1}"
RUN_GO="${RUN_GO:-1}"
RUNS="${RUNS:-1}"

insecure_args=()
curl_insecure_args=()
if [[ "$INSECURE" == "1" ]]; then
  insecure_args+=(--insecure)
  curl_insecure_args+=(-k)
fi

body_args=()
if [[ "$BODY_BYTES" != "0" ]]; then
  body_args+=(--body-bytes "$BODY_BYTES")
fi

proxy_args=()
curl_proxy_args=()
if [[ "$DISABLE_PROXY" == "1" ]]; then
  proxy_args+=(--no-proxy)
  curl_proxy_args+=(--noproxy '*')
fi

bench_args_common=(--requests "$REQUESTS_WARM" --concurrency "$CONCURRENCY_WARM" "${proxy_args[@]}" "${insecure_args[@]}" "${body_args[@]}")
cold_args_common=(--requests "$REQUESTS_COLD" --concurrency "$CONCURRENCY_COLD" "${proxy_args[@]}" "${insecure_args[@]}" "${body_args[@]}")

extract_field() {
  local key="$1"
  awk -v k="$key" '
    BEGIN { RS="[ \n]"; FS="=" }
    $1 == k { print $2; found=1 }
    END { if (!found) exit 1 }
  ' || true
}

run_capture() {
  local name="$1"
  shift
  local output
  local rc=0
  local walls=()
  local ok_total=0
  local fail_total=0
  local h1_total=0
  local h2_total=0
  for _ in $(seq 1 "$RUNS"); do
    rc=0
    output="$("$@" 2>&1)" || rc=$?
    local wall ok fail h1 h2
    wall="$(printf '%s\n' "$output" | extract_field wall_ms)"
    ok="$(printf '%s\n' "$output" | extract_field ok)"
    fail="$(printf '%s\n' "$output" | extract_field fail)"
    h1="$(printf '%s\n' "$output" | extract_field h1)"
    h2="$(printf '%s\n' "$output" | extract_field h2)"
    if [[ "$rc" != "0" || -z "$wall" ]]; then
      [[ -n "$ok" ]] || ok="-"
      [[ -n "$fail" ]] || fail="-"
      [[ -n "$h1" ]] || h1="-"
      [[ -n "$h2" ]] || h2="-"
      printf '| %s | error | - | - | %s | %s | %s | %s |\n' "$name" "$ok" "$fail" "$h1" "$h2"
      printf '<details><summary>%s output</summary>\n\n```text\n%s\n```\n</details>\n\n' "$name" "$output"
      return 0
    fi
    walls+=("$wall")
    ok_total=$((ok_total + ${ok:-0}))
    fail_total=$((fail_total + ${fail:-0}))
    h1_total=$((h1_total + ${h1:-0}))
    h2_total=$((h2_total + ${h2:-0}))
  done
  print_stats_row "$name" "$ok_total" "$fail_total" "$h1_total" "$h2_total" "${walls[@]}"
}

run_repeat_single() {
  local name="$1"
  local repeats="$2"
  shift 2
  local ok_total=0
  local fail_total=0
  local h1_total=0
  local h2_total=0
  local walls=()
  local output
  local rc=0
  for _ in $(seq 1 "$RUNS"); do
    local total=0
    for _ in $(seq 1 "$repeats"); do
      rc=0
      output="$("$@" 2>&1)" || rc=$?
      local wall ok fail h1 h2
      wall="$(printf '%s\n' "$output" | extract_field wall_ms)"
      ok="$(printf '%s\n' "$output" | extract_field ok)"
      fail="$(printf '%s\n' "$output" | extract_field fail)"
      h1="$(printf '%s\n' "$output" | extract_field h1)"
      h2="$(printf '%s\n' "$output" | extract_field h2)"
      if [[ "$rc" != "0" || -z "$wall" ]]; then
        printf '| %s | error | - | - | - | - | - |\n' "$name"
        printf '<details><summary>%s output</summary>\n\n```text\n%s\n```\n</details>\n\n' "$name" "$output"
        return 0
      fi
      total=$((total + wall))
      ok_total=$((ok_total + ${ok:-0}))
      fail_total=$((fail_total + ${fail:-0}))
      h1_total=$((h1_total + ${h1:-0}))
      h2_total=$((h2_total + ${h2:-0}))
    done
    walls+=("$total")
  done
  print_stats_row "$name" "$ok_total" "$fail_total" "$h1_total" "$h2_total" "${walls[@]}"
}

print_stats_row() {
  local name="$1"
  local ok="$2"
  local fail="$3"
  local h1="$4"
  local h2="$5"
  shift 5
  local sorted
  sorted="$(printf '%s\n' "$@" | sort -n)"
  local count
  count="$(printf '%s\n' "$sorted" | wc -l | tr -d ' ')"
  local mid=$(((count + 1) / 2))
  local median min max
  median="$(printf '%s\n' "$sorted" | sed -n "${mid}p")"
  min="$(printf '%s\n' "$sorted" | sed -n '1p')"
  max="$(printf '%s\n' "$sorted" | sed -n '$p')"
  local h1_cell="$h1"
  local h2_cell="$h2"
  [[ "$h1" != "0" ]] || h1_cell="-"
  [[ "$h2" != "0" ]] || h2_cell="-"
  printf '| %s | %s | %s | %s | %s | %s | %s | %s |\n' \
    "$name" "$median" "$min" "$max" "$ok" "$fail" "$h1_cell" "$h2_cell"
}

curl_timing() {
  local name="$1"
  local url="$2"
  local http_arg="$3"
  local fresh_arg=()
  if [[ "$name" == *fresh* ]]; then
    fresh_arg+=(--no-keepalive --connect-timeout 5)
  fi
  local output
  output="$(curl -sS "${curl_proxy_args[@]}" "${curl_insecure_args[@]}" "${fresh_arg[@]}" "$http_arg" \
    -o /dev/null \
    -w 'code=%{http_code} http=%{http_version} dns=%{time_namelookup} connect=%{time_connect} tls=%{time_appconnect} first=%{time_starttransfer} total=%{time_total}\n' \
    "$url" 2>&1)" || true
  printf '| %s | `%s` |\n' "$name" "$output"
}

echo "online_tls_bench preset=$PRESET"
echo "url_h2=$URL_H2"
echo "url_h1=$URL_H1"
echo "cold_requests=$REQUESTS_COLD cold_concurrency=$CONCURRENCY_COLD warm_requests=$REQUESTS_WARM warm_concurrency=$CONCURRENCY_WARM warmup=$WARMUP body_bytes=$BODY_BYTES disable_proxy=$DISABLE_PROXY runs=$RUNS"
echo

cmake --preset "$PRESET" \
  -DHTTPCLIENT_ENABLE_CURL_BASELINE="$([[ "$RUN_LIBCURL" == "1" ]] && echo ON || echo OFF)" \
  >/dev/null
build_targets=(httpclient_bench asio_httpclient_bench h2_httpclient_bench)
if [[ "$RUN_LIBCURL" == "1" ]]; then
  build_targets+=(curl_httpclient_bench)
fi
cmake --build --preset "$PRESET" --target "${build_targets[@]}" -j >/dev/null

if [[ "$RUN_CURL" == "1" ]]; then
  echo "## curl single-request timing"
  echo
  echo "| Case | Timing |"
  echo "|---|---|"
  curl_timing "curl h2 fresh" "$URL_H2" "--http2"
  curl_timing "curl h1 fresh" "$URL_H1" "--http1.1"
  echo
fi

echo "## cold-ish client runs"
echo
echo "| Case | median_ms | min_ms | max_ms | ok | fail | h1 | h2 |"
echo "|---|---:|---:|---:|---:|---:|---:|---:|"
if [[ "$RUN_LIBCURL" == "1" ]]; then
  run_capture "libcurl h2 fresh" "$ROOT_DIR/build-$PRESET/curl_httpclient_bench" \
    --url "$URL_H2" "${cold_args_common[@]}" --fresh-connect --forbid-reuse \
    --max-connections "$CONCURRENCY_COLD" --max-host-connections "$CONCURRENCY_COLD" \
    --discard-response
  run_capture "libcurl h1 fresh" "$ROOT_DIR/build-$PRESET/curl_httpclient_bench" \
    --url "$URL_H1" "${cold_args_common[@]}" --http1 --fresh-connect --forbid-reuse \
    --max-connections "$CONCURRENCY_COLD" --max-host-connections "$CONCURRENCY_COLD" \
    --discard-response
fi
if [[ "$RUN_GO" == "1" ]]; then
  run_capture "go h2 fresh" env GOCACHE="$ROOT_DIR/tools/go_compare/.cache/go-build" \
    go run "$ROOT_DIR/tools/go_compare/bench_client.go" \
    --url "$URL_H2" "${cold_args_common[@]}" --fresh-connect
  run_capture "go h1 fresh" env GOCACHE="$ROOT_DIR/tools/go_compare/.cache/go-build" \
    go run "$ROOT_DIR/tools/go_compare/bench_client.go" \
    --url "$URL_H1" "${cold_args_common[@]}" --fresh-connect --http1
fi
if [[ "$RUN_H2" == "1" ]]; then
  run_repeat_single "cpp h2 cold" "$REQUESTS_COLD" \
    "$ROOT_DIR/build-$PRESET/h2_httpclient_bench" \
    --url "$URL_H2" --requests 1 --concurrency 1 "${proxy_args[@]}" "${insecure_args[@]}" "${body_args[@]}" \
    --sessions "$H2_SESSIONS" \
    --max-streams "$H2_MAX_STREAMS" --discard-response --callback
fi
if [[ "$RUN_ASIO_H1" == "1" ]]; then
  run_repeat_single "cpp asio h1 cold" "$REQUESTS_COLD" \
    "$ROOT_DIR/build-$PRESET/asio_httpclient_bench" \
    --url "$URL_H1" --requests 1 --concurrency 1 "${proxy_args[@]}" "${insecure_args[@]}" "${body_args[@]}" \
    --discard-response
fi
if [[ "$RUN_HTTPCLIENT" == "1" ]]; then
  run_repeat_single "cpp httpclient force-h1 cold" "$REQUESTS_COLD" \
    "$ROOT_DIR/build-$PRESET/httpclient_bench" \
    --url "$URL_H1" --requests 1 --concurrency 1 "${proxy_args[@]}" "${insecure_args[@]}" "${body_args[@]}" \
    --h2-sessions "$H2_SESSIONS" --h2-max-streams "$H2_MAX_STREAMS" \
    --origin-waiters 128 --strict-detect --force-h1
fi
echo

echo "## warm keepalive runs"
echo
echo "| Case | median_ms | min_ms | max_ms | ok | fail | h1 | h2 |"
echo "|---|---:|---:|---:|---:|---:|---:|---:|"
if [[ "$RUN_LIBCURL" == "1" ]]; then
  run_capture "libcurl h2 warm" "$ROOT_DIR/build-$PRESET/curl_httpclient_bench" \
    --url "$URL_H2" "${bench_args_common[@]}" --warmup "$WARMUP" \
    --max-connections "$CONCURRENCY_WARM" --max-host-connections "$CONCURRENCY_WARM" \
    --discard-response
  run_capture "libcurl h1 warm" "$ROOT_DIR/build-$PRESET/curl_httpclient_bench" \
    --url "$URL_H1" "${bench_args_common[@]}" --http1 --warmup "$WARMUP" \
    --max-connections "$CONCURRENCY_WARM" --max-host-connections "$CONCURRENCY_WARM" \
    --discard-response
fi
if [[ "$RUN_GO" == "1" ]]; then
  run_capture "go h2 warm" env GOCACHE="$ROOT_DIR/tools/go_compare/.cache/go-build" \
    go run "$ROOT_DIR/tools/go_compare/bench_client.go" \
    --url "$URL_H2" "${bench_args_common[@]}" --warmup-per-url "$WARMUP"
  run_capture "go h1 warm" env GOCACHE="$ROOT_DIR/tools/go_compare/.cache/go-build" \
    go run "$ROOT_DIR/tools/go_compare/bench_client.go" \
    --url "$URL_H1" "${bench_args_common[@]}" --warmup-per-url "$WARMUP" --http1
fi
if [[ "$RUN_H2" == "1" ]]; then
  run_capture "cpp h2 warm" "$ROOT_DIR/build-$PRESET/h2_httpclient_bench" \
    --url "$URL_H2" "${bench_args_common[@]}" --warmup "$WARMUP" \
    --sessions "$H2_SESSIONS" --max-streams "$H2_MAX_STREAMS" --discard-response --callback
fi
if [[ "$RUN_ASIO_H1" == "1" ]]; then
  run_capture "cpp asio h1 warm" "$ROOT_DIR/build-$PRESET/asio_httpclient_bench" \
    --url "$URL_H1" "${bench_args_common[@]}" --warmup "$WARMUP" \
    --discard-response --concurrent-warmup
fi
if [[ "$RUN_HTTPCLIENT" == "1" ]]; then
  run_capture "cpp httpclient force-h1 warm" "$ROOT_DIR/build-$PRESET/httpclient_bench" \
    --url "$URL_H1" "${bench_args_common[@]}" --warmup-per-url "$WARMUP" \
    --h2-sessions "$H2_SESSIONS" --h2-max-streams "$H2_MAX_STREAMS" \
    --origin-waiters 128 --strict-detect --concurrent-warmup --force-h1
  run_capture "cpp httpclient auto warm" "$ROOT_DIR/build-$PRESET/httpclient_bench" \
    --url "$URL_H2" "${bench_args_common[@]}" --warmup-per-url "$WARMUP" \
    --h2-sessions "$H2_SESSIONS" --h2-max-streams "$H2_MAX_STREAMS" \
    --origin-waiters 128 --strict-detect --concurrent-warmup
  run_capture "cpp httpclient force-h2 warm" "$ROOT_DIR/build-$PRESET/httpclient_bench" \
    --url "$URL_H2" "${bench_args_common[@]}" --warmup-per-url "$WARMUP" \
    --h2-sessions "$H2_SESSIONS" --h2-max-streams "$H2_MAX_STREAMS" \
    --origin-waiters 128 --strict-detect --concurrent-warmup --force-h2
fi
