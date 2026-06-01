#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${1:-${ROOT_DIR}/build-o2}"
HTTP_PORT="${HTTP_PORT:-8980}"
HTTPS_PORT="${HTTPS_PORT:-8985}"
H2_PORT="${H2_PORT:-8986}"
PROXY_PORT="${PROXY_PORT:-8899}"
AUTH_PROXY_PORT="${AUTH_PROXY_PORT:-8898}"
SOCKS5_PORT="${SOCKS5_PORT:-8897}"
HTTPS_PROXY_PORT="${HTTPS_PROXY_PORT:-8896}"

HTTP_URL="http://127.0.0.1:${HTTP_PORT}/echo"
HTTPS_URL="https://127.0.0.1:${HTTPS_PORT}/echo"
H2_URL="https://127.0.0.1:${H2_PORT}/echo"
PROXY_URL="http://127.0.0.1:${PROXY_PORT}"
AUTH_PROXY_URL="http://user:pass@127.0.0.1:${AUTH_PROXY_PORT}"
SOCKS5_URL="socks5://127.0.0.1:${SOCKS5_PORT}"
HTTPS_PROXY_URL="https://127.0.0.1:${HTTPS_PROXY_PORT}"
TEST_BIN="${BUILD_DIR}/proxy_transport_test"

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
  PORT="$HTTP_PORT" go run tools/go_compare/bench_server.go
) >/tmp/httpclient-proxy-http.log 2>&1 &
pids+=("$!")

(
  cd "$ROOT_DIR"
  PORT="$HTTPS_PORT" HTTP1_ONLY=1 go run tools/go_compare/bench_https_server.go
) >/tmp/httpclient-proxy-https.log 2>&1 &
pids+=("$!")

(
  cd "$ROOT_DIR"
  PORT="$H2_PORT" go run tools/go_compare/bench_https_server.go
) >/tmp/httpclient-proxy-h2.log 2>&1 &
pids+=("$!")

(
  cd "$ROOT_DIR"
  PROXY_PORT="$PROXY_PORT" go run tools/go_compare/http_proxy.go
) >/tmp/httpclient-proxy.log 2>&1 &
pids+=("$!")

(
  cd "$ROOT_DIR"
  PROXY_PORT="$AUTH_PROXY_PORT" PROXY_AUTH="Basic dXNlcjpwYXNz" \
    go run tools/go_compare/http_proxy.go
) >/tmp/httpclient-auth-proxy.log 2>&1 &
pids+=("$!")

(
  cd "$ROOT_DIR"
  PROXY_PORT="$SOCKS5_PORT" SOCKS5=1 go run tools/go_compare/http_proxy.go
) >/tmp/httpclient-socks5-proxy.log 2>&1 &
pids+=("$!")

(
  cd "$ROOT_DIR"
  PROXY_PORT="$HTTPS_PROXY_PORT" HTTPS_PROXY=1 go run tools/go_compare/http_proxy.go
) >/tmp/httpclient-https-proxy.log 2>&1 &
pids+=("$!")

wait_for_server "$HTTP_URL"
wait_for_server "$HTTPS_URL"
wait_for_server "$H2_URL"
wait_for_server "${PROXY_URL}/__proxy_stats"
wait_for_server "http://127.0.0.1:${AUTH_PROXY_PORT}/__proxy_stats"
wait_for_server "${HTTPS_PROXY_URL}/__proxy_stats"

"$TEST_BIN" --http-url "$HTTP_URL" --https-url "$HTTPS_URL" --h2-url "$H2_URL" --proxy-url "$PROXY_URL" --auth-proxy-url "$AUTH_PROXY_URL" --socks5-url "$SOCKS5_URL" --https-proxy-url "$HTTPS_PROXY_URL"
