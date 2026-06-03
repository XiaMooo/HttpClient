#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

PRESET="${PRESET:-asan}"
JOBS="${JOBS:-}"
ENABLE_LSAN="${ENABLE_LSAN:-0}"
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

if [[ "$ENABLE_LSAN" == "1" ]]; then
  DEFAULT_ASAN_OPTIONS="detect_leaks=1:halt_on_error=1:strict_string_checks=1"
else
  DEFAULT_ASAN_OPTIONS="detect_leaks=0:halt_on_error=1:strict_string_checks=1"
fi

ASAN_OPTIONS="${ASAN_OPTIONS:-$DEFAULT_ASAN_OPTIONS}"
UBSAN_OPTIONS="${UBSAN_OPTIONS:-halt_on_error=1:print_stacktrace=1}"
export ASAN_OPTIONS UBSAN_OPTIONS

if [[ -n "${TEST_REGEX:-}" ]]; then
  ctest "${CTEST_ARGS[@]}" -R "$TEST_REGEX"
else
  mapfile -t tests < <(ctest --test-dir "$ROOT_DIR/build-$PRESET" -N |
    awk '/Test #[0-9]+:/ { print $3 }')
  for test_name in "${tests[@]}"; do
    ctest "${CTEST_ARGS[@]}" -R "^${test_name}$"
  done
fi
