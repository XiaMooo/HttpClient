#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

PRESET="${PRESET:-asan}"
JOBS="${JOBS:-}"
CTEST_ARGS=("--test-dir" "$ROOT_DIR/build-$PRESET" "--output-on-failure")

if [[ -n "${TEST_REGEX:-}" ]]; then
  CTEST_ARGS+=("-R" "$TEST_REGEX")
fi

cmake --preset "$PRESET" -DHTTPCLIENT_ENABLE_CURL_BASELINE=OFF

if [[ -n "$JOBS" ]]; then
  cmake --build --preset "$PRESET" -j "$JOBS"
else
  cmake --build --preset "$PRESET" -j
fi

ASAN_OPTIONS="${ASAN_OPTIONS:-detect_leaks=1:halt_on_error=1:strict_string_checks=1}" \
UBSAN_OPTIONS="${UBSAN_OPTIONS:-halt_on_error=1:print_stacktrace=1}" \
  ctest "${CTEST_ARGS[@]}"
