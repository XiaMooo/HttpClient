#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

PRESET="${PRESET:-o2}"
REQUESTS="${REQUESTS:-10000}"
CONCURRENCY="${CONCURRENCY:-512}"
DELAY_MS="${DELAY_MS:-5}"
RESPONSE_BYTES="${RESPONSE_BYTES:-1024}"
BODY_BYTES="${BODY_BYTES:-0}"
WARMUP_PER_URL="${WARMUP_PER_URL:-128}"
PRECONNECT_PER_URL="${PRECONNECT_PER_URL:-0}"
H1_ACTOR="${H1_ACTOR:-0}"
H1_ACTOR_CONNECTIONS="${H1_ACTOR_CONNECTIONS:-32}"
MAX_CONN_CASES="${MAX_CONN_CASES:-64 128 256 512}"
PORT="${PORT:-8945}"
RUN_ASYNCX="${RUN_ASYNCX:-1}"
ROUNDS="${ROUNDS:-1}"
CSV_FILE="${CSV_FILE:-}"
PRINT_AGGREGATE="${PRINT_AGGREGATE:-1}"
PROFILE="${PROFILE:-auto}"

URL="https://127.0.0.1:${PORT}/ping"

pids=()
tmp_files=()
cleanup() {
  for pid in "${pids[@]:-}"; do
    kill "$pid" >/dev/null 2>&1 || true
  done
  wait >/dev/null 2>&1 || true
  for file in "${tmp_files[@]:-}"; do
    rm -f "$file"
  done
}
trap cleanup EXIT

CSV_TARGET="$CSV_FILE"
if [[ -z "$CSV_TARGET" && "$ROUNDS" -gt 1 && "$PRINT_AGGREGATE" == "1" ]]; then
  CSV_TARGET="$(mktemp)"
  tmp_files+=("$CSV_TARGET")
fi

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
  local round="$3"
  local output="$4"
  local wall p50 p95 p99 cpu_user cpu_system rss peak created idle_hit
  local pool_wait_avg pool_wait_max connect_avg connect_max acquire_avg acquire_max
  local write_avg write_max read_headers_avg read_headers_max exchange_avg exchange_max
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
  pool_wait_avg="$(printf '%s\n' "$output" | extract_field h1_pool_wait_avg_us)"
  pool_wait_max="$(printf '%s\n' "$output" | extract_field h1_pool_wait_max_seen_us)"
  connect_avg="$(printf '%s\n' "$output" | extract_field h1_connect_avg_us)"
  connect_max="$(printf '%s\n' "$output" | extract_field h1_connect_max_seen_us)"
  acquire_avg="$(printf '%s\n' "$output" | extract_field h1_acquire_avg_us)"
  acquire_max="$(printf '%s\n' "$output" | extract_field h1_acquire_max_seen_us)"
  write_avg="$(printf '%s\n' "$output" | extract_field h1_write_avg_us)"
  write_max="$(printf '%s\n' "$output" | extract_field h1_write_max_seen_us)"
  read_headers_avg="$(printf '%s\n' "$output" | extract_field h1_read_headers_avg_us)"
  read_headers_max="$(printf '%s\n' "$output" | extract_field h1_read_headers_max_seen_us)"
  exchange_avg="$(printf '%s\n' "$output" | extract_field h1_exchange_avg_us)"
  exchange_max="$(printf '%s\n' "$output" | extract_field h1_exchange_max_seen_us)"
  printf '| %s | %s | %s | %s | %s/%s/%s | %s/%s | %s | %s | %s | %s | %s/%s | %s/%s | %s/%s | %s/%s | %s/%s | %s/%s |\n' \
    "$round" "$max_conn" "$mode" "${wall:-error}" "${p50:--}" "${p95:--}" "${p99:--}" \
    "${cpu_user:--}" "${cpu_system:--}" "${rss:--}" "${peak:--}" \
    "${created:--}" "${idle_hit:--}" \
    "${pool_wait_avg:--}" "${pool_wait_max:--}" \
    "${connect_avg:--}" "${connect_max:--}" \
    "${acquire_avg:--}" "${acquire_max:--}" \
    "${write_avg:--}" "${write_max:--}" \
    "${read_headers_avg:--}" "${read_headers_max:--}" \
    "${exchange_avg:--}" "${exchange_max:--}"
  if [[ -n "$CSV_TARGET" ]]; then
    printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
      "$round" "$max_conn" "$mode" "${wall:-}" "${p50:-}" "${p95:-}" \
      "${p99:-}" "${cpu_user:-}" "${cpu_system:-}" "${rss:-}" "${peak:-}" \
      "${created:-}" "${idle_hit:-}" \
      "${pool_wait_avg:-}" "${pool_wait_max:-}" \
      "${connect_avg:-}" "${connect_max:-}" \
      "${acquire_avg:-}" "${acquire_max:-}" \
      "${write_avg:-}" "${write_max:-}" \
      "${read_headers_avg:-}" "${read_headers_max:-}" \
      "${exchange_avg:-}" "${exchange_max:-}" >>"$CSV_TARGET"
  fi
}

run_case() {
  local max_conn="$1"
  local mode="$2"
  local gather_arg=()
  local profile_arg=()
  local h1_actor_arg=()
  if [[ "$mode" == "cpp-asyncx" ]]; then
    gather_arg+=(--gather)
  fi
  if [[ "$H1_ACTOR" == "1" ]]; then
    h1_actor_arg+=(--h1-actor --h1-actor-connections "$H1_ACTOR_CONNECTIONS")
  fi
  case "$PROFILE" in
    auto)
      profile_arg+=(--auto-profile)
      ;;
    balanced)
      profile_arg+=(--balanced)
      ;;
    throughput)
      profile_arg+=(--throughput)
      ;;
    *)
      echo "unknown PROFILE=$PROFILE" >&2
      exit 1
      ;;
  esac
  "$ROOT_DIR/build-$PRESET/httpclient_bench" \
    "${gather_arg[@]}" \
    "${profile_arg[@]}" \
    --url "$URL" \
    --requests "$REQUESTS" \
    --concurrency "$CONCURRENCY" \
    --body-bytes "$BODY_BYTES" \
    --warmup-per-url "$WARMUP_PER_URL" \
    --preconnect-per-url "$PRECONNECT_PER_URL" \
    --concurrent-warmup \
    --strict-detect \
    --insecure --no-proxy \
    "${h1_actor_arg[@]}" \
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

server_ready=0
for _ in $(seq 1 100); do
  if curl -ksS --noproxy '*' "$URL" >/dev/null 2>&1; then
    server_ready=1
    break
  fi
  sleep 0.05
done
if [[ "$server_ready" != "1" ]]; then
  echo "server did not become ready: $URL" >&2
  echo "server log:" >&2
  sed -n '1,120p' /tmp/httpclient-h1-pool-scan.log >&2 || true
  exit 1
fi

if [[ -n "$CSV_TARGET" ]]; then
  mkdir -p "$(dirname "$CSV_TARGET")"
  printf 'round,max_conn,mode,wall_ms,p50_us,p95_us,p99_us,cpu_user_ms,cpu_system_ms,rss_kb,peak_rss_kb,h1_created,h1_idle_hit,h1_pool_wait_avg_us,h1_pool_wait_max_seen_us,h1_connect_avg_us,h1_connect_max_seen_us,h1_acquire_avg_us,h1_acquire_max_seen_us,h1_write_avg_us,h1_write_max_seen_us,h1_read_headers_avg_us,h1_read_headers_max_seen_us,h1_exchange_avg_us,h1_exchange_max_seen_us\n' >"$CSV_TARGET"
fi

echo "h1_pool_scan preset=$PRESET profile=$PROFILE requests=$REQUESTS concurrency=$CONCURRENCY delay_ms=$DELAY_MS response_bytes=$RESPONSE_BYTES body_bytes=$BODY_BYTES warmup_per_url=$WARMUP_PER_URL preconnect_per_url=$PRECONNECT_PER_URL h1_actor=$H1_ACTOR h1_actor_connections=$H1_ACTOR_CONNECTIONS rounds=$ROUNDS"
if [[ -n "$CSV_FILE" ]]; then
  echo "csv_file=$CSV_FILE"
fi
echo
echo "| round | max_conn | mode | wall_ms | p50/p95/p99_us | cpu_user/system_ms | rss_kb | peak_rss_kb | h1_created | h1_idle_hit | pool_wait avg/max | connect avg/max | acquire avg/max | write avg/max | read_headers avg/max | exchange avg/max |"
echo "|---:|---:|---|---:|---|---|---:|---:|---:|---:|---|---|---|---|---|---|"

for round in $(seq 1 "$ROUNDS"); do
  for max_conn in $MAX_CONN_CASES; do
    output="$(run_case "$max_conn" cpp-callback 2>&1)" || true
    print_row "$max_conn" cpp-callback "$round" "$output"
    if [[ "$RUN_ASYNCX" == "1" ]]; then
      output="$(run_case "$max_conn" cpp-asyncx 2>&1)" || true
      print_row "$max_conn" cpp-asyncx "$round" "$output"
    fi
  done
done

if [[ "$ROUNDS" -gt 1 && "$PRINT_AGGREGATE" == "1" && -n "$CSV_TARGET" ]]; then
  python3 - "$CSV_TARGET" <<'PY'
import csv
import math
import statistics
import sys
from collections import defaultdict

path = sys.argv[1]
groups = defaultdict(list)
with open(path, newline="") as fh:
    for row in csv.DictReader(fh):
        if row.get("wall_ms"):
            groups[(int(row["max_conn"]), row["mode"])].append(row)

def values(rows, key):
    return [int(row[key]) for row in rows if row.get(key)]

def median(rows, key):
    vals = values(rows, key)
    return "" if not vals else str(int(statistics.median(vals)))

def percentile(rows, key, p):
    vals = sorted(values(rows, key))
    if not vals:
        return ""
    idx = max(0, min(len(vals) - 1, math.ceil(len(vals) * p) - 1))
    return str(vals[idx])

print()
print("h1_pool_scan_aggregate")
print()
print("| max_conn | mode | samples | wall_median_ms | wall_p95_ms | p95_median_us | p99_median_us | rss_median_kb | h1_created_median |")
print("|---:|---|---:|---:|---:|---:|---:|---:|---:|")
for (max_conn, mode), rows in sorted(groups.items()):
    print(
        f"| {max_conn} | {mode} | {len(rows)} | "
        f"{median(rows, 'wall_ms')} | {percentile(rows, 'wall_ms', 0.95)} | "
        f"{median(rows, 'p95_us')} | {median(rows, 'p99_us')} | "
        f"{median(rows, 'rss_kb')} | {median(rows, 'h1_created')} |"
    )
print()
print("| max_conn | mode | pool_wait_avg_us | connect_avg_us | acquire_avg_us | write_avg_us | read_headers_avg_us | exchange_avg_us |")
print("|---:|---|---:|---:|---:|---:|---:|---:|")
for (max_conn, mode), rows in sorted(groups.items()):
    print(
        f"| {max_conn} | {mode} | "
        f"{median(rows, 'h1_pool_wait_avg_us')} | "
        f"{median(rows, 'h1_connect_avg_us')} | "
        f"{median(rows, 'h1_acquire_avg_us')} | "
        f"{median(rows, 'h1_write_avg_us')} | "
        f"{median(rows, 'h1_read_headers_avg_us')} | "
        f"{median(rows, 'h1_exchange_avg_us')} |"
    )
PY
fi
