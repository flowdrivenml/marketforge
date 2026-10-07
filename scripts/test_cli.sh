#!/usr/bin/env bash

set +e

PASSED=0
FAILED=0
FAILURES=()

TMP_DIR="$(mktemp -d)"

cleanup() {
  rm -rf "$TMP_DIR"
}

trap cleanup EXIT

run() {
  local output_file
  local status

  output_file="$TMP_DIR/test-$((PASSED + FAILED + 1)).log"

  "$@" >"$output_file" 2>&1
  status=$?

  if [ "$status" -eq 0 ]; then
    PASSED=$((PASSED + 1))
  else
    FAILED=$((FAILED + 1))

    FAILURES+=(
      "$status|$*|$output_file"
    )
  fi
}

# ==============================================================================
# TESTS
# ==============================================================================

run marketforge --help
run marketforge instruments --help
run marketforge archives --help
run marketforge datasets --help
run marketforge process --help
run marketforge merge --help
run marketforge config --help

run marketforge database --help

run marketforge metadata list --limit 10

run marketforge metadata list \
  --exchange bybit \
  --symbol BTCUSDT

run marketforge metadata formats \
  --exchange bybit \
  --dataset trade \
  --type perpetual \
  --category linear

run marketforge metadata rules \
  --exchange bybit \
  --dataset trade \
  --type perpetual \
  --category linear

run marketforge instruments \
  --symbol BTC \
  --market linear \
  --sort turnover

run marketforge instruments \
  --exchange bybit \
  --market linear \
  --sort turnover

run marketforge instruments \
  --symbol BTC \
  --market linear \
  --sort turnover \
  --details

run marketforge instruments \
  --exchange bybit \
  --refresh \
  --sort turnover

run marketforge availability \
  --exchange bybit \
  --type perpetual \
  --category linear \
  --symbol BTCUSDT \
  --data trade_ticks \
  --start 2026-09-01 \
  --end 2026-09-03

run marketforge archives

run marketforge archives \
  --exchange bybit \
  --type perpetual \
  --category linear \
  --data trade_ticks \
  --symbol BTCUSDT

run marketforge archives \
  --exchange bybit \
  --type perpetual \
  --category linear \
  --data trade_ticks \
  --symbol BTCUSDT \
  --start 2026-09-01 \
  --end 2026-10-01

run marketforge config profiles
run marketforge config profile default

run marketforge datasets
run marketforge datasets --status complete
run marketforge datasets --symbol BTCUSDT

run marketforge process \
  --exchange bybit \
  --type perpetual \
  --category linear \
  --symbol BTCUSDT \
  --data trade_ticks \
  --dataset trade \
  --profile default

run cat data/.jobs/process.json

run marketforge datasets

run marketforge merge 1 2 3 \
  --profile default

run cat data/.jobs/merge.json

run marketforge datasets

# ==============================================================================
# RESULTS
# ==============================================================================

if [ "$FAILED" -eq 0 ]; then
  echo "All $PASSED CLI tests passed."
  exit 0
fi

echo
echo "================================================================================"
echo "FAILED CLI TESTS: $FAILED"
echo "================================================================================"

for failure in "${FAILURES[@]}"; do
  IFS='|' read -r status command output_file <<<"$failure"

  echo
  echo "--------------------------------------------------------------------------------"
  echo "FAILED [$status]"
  echo "\$ $command"
  echo "--------------------------------------------------------------------------------"
  cat "$output_file"
done

echo
echo "================================================================================"
echo "Passed: $PASSED"
echo "Failed: $FAILED"
echo "Total : $((PASSED + FAILED))"
echo "================================================================================"

exit 1
