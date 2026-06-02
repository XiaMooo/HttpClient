#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

PRESET="${PRESET:-o2}"
RUN_ASAN="${RUN_ASAN:-1}"
RUN_MIXED_BENCH="${RUN_MIXED_BENCH:-1}"
ASAN_PRESET="${ASAN_PRESET:-asan}"

cmake --preset "$PRESET" -DHTTPCLIENT_ENABLE_CURL_BASELINE=OFF
cmake --build --preset "$PRESET" -j
ctest --test-dir "$ROOT_DIR/build-$PRESET" --output-on-failure

if [[ "$RUN_ASAN" == "1" ]]; then
  cmake --preset "$ASAN_PRESET" -DHTTPCLIENT_ENABLE_CURL_BASELINE=OFF
  cmake --build --preset "$ASAN_PRESET" -j
  (
    cd "$ROOT_DIR/build-$ASAN_PRESET"
    ASAN_OPTIONS="${ASAN_OPTIONS:-detect_leaks=1}" ./asyncx_test
  )
fi

if [[ "$RUN_MIXED_BENCH" == "1" ]]; then
  REQUESTS="${REQUESTS:-10000}" \
  CONCURRENCY="${CONCURRENCY:-512}" \
  DELAY_MS="${DELAY_MS:-5}" \
  RESPONSE_BYTES="${RESPONSE_BYTES:-1024}" \
  BODY_CASES="${BODY_CASES:-0 1024}" \
  WARMUP_PER_URL="${WARMUP_PER_URL:-512}" \
  CONCURRENT_WARMUP="${CONCURRENT_WARMUP:-1}" \
  STRICT_DETECT="${STRICT_DETECT:-1}" \
  H2_SESSIONS="${H2_SESSIONS:-4}" \
  H1_MAX_CONNECTIONS_PER_ORIGIN="${H1_MAX_CONNECTIONS_PER_ORIGIN:-256}" \
    "$ROOT_DIR/scripts/run_mixed_protocol_bench.sh"
fi
