#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
URL="${URL:-https://127.0.0.1:8443/ping}"
REQUESTS="${REQUESTS:-1000}"
CONCURRENCY="${CONCURRENCY:-16}"
SHARDS="${SHARDS:-0}"
H2_SESSIONS="${H2_SESSIONS:-1}"
BODY_BYTES="${BODY_BYTES:-0}"
PRESETS="${PRESETS:-o2 o3 o0 relwithdebinfo}"
RUN_LIBCURL="${RUN_LIBCURL:-0}"
RUN_ASIO_H1="${RUN_ASIO_H1:-1}"
RUN_H2="${RUN_H2:-1}"
RUN_GO="${RUN_GO:-1}"

common_args=(--url "$URL" --requests "$REQUESTS" --concurrency "$CONCURRENCY" --insecure --no-proxy)
if [[ "$BODY_BYTES" != "0" ]]; then
  common_args+=(--body-bytes "$BODY_BYTES")
fi

echo "url=$URL requests=$REQUESTS concurrency=$CONCURRENCY"
echo "cpp_presets=$PRESETS"
echo

for preset in $PRESETS; do
  build_dir="$ROOT_DIR/build-$preset"
  echo "== cpp preset=$preset =="
  cmake --preset "$preset" \
    -DHTTPCLIENT_ENABLE_CURL_BASELINE="$([[ "$RUN_LIBCURL" == "1" ]] && echo ON || echo OFF)" \
    >/dev/null
  cmake --build --preset "$preset" -j >/dev/null

  if [[ "$RUN_LIBCURL" == "1" ]]; then
    echo "-- libcurl_multi"
    "$build_dir/curl_httpclient_bench" "${common_args[@]}" \
      --max-connections "$CONCURRENCY" \
      --max-host-connections "$CONCURRENCY"
  fi

  if [[ "$RUN_ASIO_H1" == "1" ]]; then
    echo "-- asio_beast_h1"
    asio_args=("${common_args[@]}")
    if [[ "$SHARDS" != "0" ]]; then
      asio_args+=(--shards "$SHARDS")
    fi
    "$build_dir/asio_httpclient_bench" "${asio_args[@]}"
  fi

  if [[ "$RUN_H2" == "1" ]]; then
    echo "-- asio_nghttp2_h2"
    "$build_dir/h2_httpclient_bench" "${common_args[@]}" --sessions "$H2_SESSIONS"
  fi
  echo
done

if [[ "$RUN_GO" == "1" ]]; then
  echo "== go default =="
  (cd "$ROOT_DIR/tools/go_compare" && GOCACHE="$PWD/.cache/go-build" \
    go run bench_client.go "${common_args[@]}")
  echo

  echo "== go http1 =="
  (cd "$ROOT_DIR/tools/go_compare" && GOCACHE="$PWD/.cache/go-build" \
    go run bench_client.go "${common_args[@]}" --http1)
fi
