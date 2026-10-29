#!/usr/bin/env bash

set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT/engine"

MODE="${1:-quick}"

FEATURES="live,postgres"
LOG_DIR="$ROOT/engine/target/live-verification/source-$(date -u +%Y%m%dT%H%M%SZ)-$$"
mkdir -p "$LOG_DIR"
GATE=0
CARGO_FLAGS=(--locked)
if [[ "${CARGO_OFFLINE:-1}" != "0" ]]; then
  CARGO_FLAGS+=(--offline)
fi

run() {
  echo
  echo "================================================================"
  echo "Running: $*"
  echo "================================================================"

  GATE=$((GATE + 1))
  "$@" 2>&1 | tee "$LOG_DIR/$GATE.log"
}

source_unit_tests() {
  run cargo test "${CARGO_FLAGS[@]}" \
    --no-default-features \
    --features "$FEATURES" \
    --lib live::stream::source
}

source_integration_tests() {
  run cargo test "${CARGO_FLAGS[@]}" \
    --no-default-features \
    --features "$FEATURES" \
    --test live_source \
    -- --nocapture
  if [[ "${SOURCE_POSTGRES_TESTS:-0}" == "1" ]]; then
    run cargo test "${CARGO_FLAGS[@]}" \
      --no-default-features --features "$FEATURES" --test live_source \
      database:: -- --ignored --nocapture
  fi
}

related_integration_tests() {
  local target

  for target in \
    live_websocket \
    live_rest \
    live_normalization \
    live_integrity \
    live_recovery; do
    run cargo test "${CARGO_FLAGS[@]}" \
      --no-default-features \
      --features "$FEATURES" \
      --test "$target"
  done
}

case "$MODE" in
quick)
  run cargo check "${CARGO_FLAGS[@]}" \
    --no-default-features \
    --features "$FEATURES"

  source_unit_tests
  ;;

integration)
  source_integration_tests
  ;;

extended)
  source_unit_tests
  source_integration_tests
  related_integration_tests
  ;;

full)
  source_unit_tests
  source_integration_tests
  related_integration_tests
  cd "$ROOT"

  run scripts/verify-live.sh full
  ;;

*)
  echo "Usage: $0 {quick|integration|extended|full}" >&2
  exit 2
  ;;
esac

echo
echo "================================================================"
echo "MarketForge Source Stream verification: PASS ($MODE)"
echo "================================================================"
