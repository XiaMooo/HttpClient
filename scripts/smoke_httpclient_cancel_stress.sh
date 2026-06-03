#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${1:-${ROOT_DIR}/build-o2}"
H2_PORT="${H2_PORT:-8943}"
H1_PORT="${H1_PORT:-8945}"
REQUESTS="${REQUESTS:-48}"
CONCURRENCY="${CONCURRENCY:-16}"

H2_URL="https://127.0.0.1:${H2_PORT}/echo"
H1_URL="https://127.0.0.1:${H1_PORT}/echo"
TEST_BIN="${BUILD_DIR}/httpclient_cancel_stress_test"

if [[ ! -x "$TEST_BIN" ]]; then
  echo "missing test executable: $TEST_BIN" >&2
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
  PORT="$H2_PORT" go run tools/go_compare/bench_https_server.go
) >/tmp/httpclient-cancel-stress-h2.log 2>&1 &
pids+=("$!")

(
  cd "$ROOT_DIR"
  PORT="$H1_PORT" HTTP1_ONLY=1 go run tools/go_compare/bench_https_server.go
) >/tmp/httpclient-cancel-stress-h1.log 2>&1 &
pids+=("$!")

wait_for_server "$H2_URL"
wait_for_server "$H1_URL"

"$TEST_BIN" --h1-url "$H1_URL" --h2-url "$H2_URL" \
  --requests "$REQUESTS" --concurrency "$CONCURRENCY"
