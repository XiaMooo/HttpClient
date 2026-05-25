#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${1:-${ROOT_DIR}/build-o2}"
H2_PORT="${H2_PORT:-8743}"
H1_PORT="${H1_PORT:-8745}"
DELAY_MS="${DELAY_MS:-1}"
RESPONSE_BYTES="${RESPONSE_BYTES:-128}"

H2_URL="https://127.0.0.1:${H2_PORT}/ping"
H1_URL="https://127.0.0.1:${H1_PORT}/ping"
BENCH="${BUILD_DIR}/httpclient_bench"

if [[ ! -x "$BENCH" ]]; then
  echo "missing benchmark executable: $BENCH" >&2
  exit 2
fi

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

(
  cd "$ROOT_DIR"
  PORT="$H2_PORT" DELAY_MS="$DELAY_MS" RESPONSE_BYTES="$RESPONSE_BYTES" \
    go run tools/go_compare/bench_https_server.go
) >/tmp/httpclient-smoke-h2.log 2>&1 &
pids+=("$!")

(
  cd "$ROOT_DIR"
  PORT="$H1_PORT" HTTP1_ONLY=1 DELAY_MS="$DELAY_MS" RESPONSE_BYTES="$RESPONSE_BYTES" \
    go run tools/go_compare/bench_https_server.go
) >/tmp/httpclient-smoke-h1.log 2>&1 &
pids+=("$!")

wait_for_server "$H2_URL"
wait_for_server "$H1_URL"

run_case() {
  local name="$1"
  shift
  echo "== $name =="
  local output
  output="$("$BENCH" "$@" --insecure --no-proxy)"
  echo "$output"
  if ! grep -q "fail=0" <<<"$output"; then
    echo "case failed: $name" >&2
    return 1
  fi
  case "$name" in
    h1-only)
      grep -Eq "h1=[1-9][0-9]*" <<<"$output"
      ;;
    h2-only)
      grep -Eq "h2=[1-9][0-9]*" <<<"$output"
      ;;
    mixed|reset-after-warmup)
      grep -Eq "h1=[1-9][0-9]*" <<<"$output"
      grep -Eq "h2=[1-9][0-9]*" <<<"$output"
      ;;
  esac
}

run_case h1-only \
  --url "$H1_URL" --requests 128 --concurrency 16 --strict-detect

run_case h2-only \
  --url "$H2_URL" --requests 128 --concurrency 16 --strict-detect \
  --h2-sessions 2

run_case mixed \
  --mixed --mixed-shuffle --url "$H2_URL" --url-alt "$H1_URL" \
  --requests 256 --concurrency 32 --warmup-per-url 16 --concurrent-warmup \
  --strict-detect --h2-sessions 2 --origin-waiters 64

run_case reset-after-warmup \
  --mixed --mixed-shuffle --url "$H2_URL" --url-alt "$H1_URL" \
  --requests 128 --concurrency 16 --warmup-per-url 8 --concurrent-warmup \
  --reset-connections-after-warmup --strict-detect --h2-sessions 2 \
  --origin-waiters 64
