#!/usr/bin/env bash

# MarketForge Live — Automated Verification
#
# Usage:
#   scripts/verify-live.sh quick
#   scripts/verify-live.sh full
#   scripts/verify-live.sh tui
#   scripts/verify-live.sh historical
#
# Optional:
#   CARGO_OFFLINE=0 scripts/verify-live.sh full
#
# Logs:
#   engine/target/live-verification/

set -Eeuo pipefail

SCRIPT_DIR="$(
  cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd
)"

ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
ENGINE_DIR="$ROOT_DIR/engine"

MODE="${1:-quick}"

case "$MODE" in
quick | full | tui | historical)
  ;;
*)
  echo "Unknown verification mode: $MODE"
  echo "Usage: $0 {quick|full|tui|historical}"
  exit 2
  ;;
esac

if [[ ! -f "$ENGINE_DIR/Cargo.toml" ]]; then
  echo "ERROR: Cargo.toml not found in $ENGINE_DIR"
  exit 2
fi

if ! command -v cargo >/dev/null 2>&1; then
  echo "ERROR: Cargo is not installed or not in PATH"
  exit 2
fi

cd "$ENGINE_DIR"

TIMESTAMP="$(date -u +%Y%m%dT%H%M%SZ)"

LOG_DIR="$ENGINE_DIR/target/live-verification/${TIMESTAMP}-$$"

mkdir -p "$LOG_DIR"

CARGO_FLAGS=(--locked)

if [[ "${CARGO_OFFLINE:-1}" != "0" ]]; then
  CARGO_FLAGS+=(--offline)
fi

LIVE_FLAGS=(
  --no-default-features
  --features live
)

PASSED=()
FAILED=()

run_gate() {
  local name="$1"
  shift

  local log="$LOG_DIR/${name}.log"
  local exit_code=0

  echo
  echo "=================================================="
  echo "VERIFY: $name"
  echo "=================================================="

  printf 'COMMAND:'
  printf ' %q' "$@"
  printf '\n'

  if "$@" >"$log" 2>&1; then
    echo "RESULT: PASS"
    PASSED+=("$name")

    grep -E \
      'running [0-9]+ tests|test result:|Finished .* target' \
      "$log" || true
  else
    exit_code=$?

    echo "RESULT: FAIL (exit $exit_code)"
    FAILED+=("$name")

    echo
    echo "Last 50 log lines:"
    tail -n 50 "$log"
  fi

  echo "LOG: $log"
}

verify_format() {
  run_gate \
    formatting \
    cargo fmt --all -- --check
}

verify_live_check() {
  run_gate \
    live-check \
    cargo check \
    "${CARGO_FLAGS[@]}" \
    "${LIVE_FLAGS[@]}" \
    --lib
}

verify_live_unit() {
  run_gate \
    live-unit \
    cargo test \
    "${CARGO_FLAGS[@]}" \
    "${LIVE_FLAGS[@]}" \
    --lib
}

verify_live_integration() {
  local target
  for target in \
    live_websocket live_rest live_normalization live_integrity \
    live_config live_metrics live_exchange_contracts; do
    run_gate \
      "$target" \
      cargo test \
      "${CARGO_FLAGS[@]}" \
      "${LIVE_FLAGS[@]}" \
      --test "$target"
  done

  run_gate \
    live-network-compile \
    cargo test \
    "${CARGO_FLAGS[@]}" \
    "${LIVE_FLAGS[@]}" \
    --test live_network --no-run
}

verify_live_docs() {
  run_gate \
    live-doctests \
    cargo test \
    "${CARGO_FLAGS[@]}" \
    "${LIVE_FLAGS[@]}" \
    --doc
}

verify_live_tui() {
  run_gate \
    live-tui \
    cargo check \
    "${CARGO_FLAGS[@]}" \
    --no-default-features \
    --features live,monitor-tui \
    --bin marketforge-live-tui
}

verify_historical() {
  run_gate \
    historical-library \
    cargo test \
    "${CARGO_FLAGS[@]}" \
    --no-default-features \
    --features historical \
    --lib
}

echo
echo "MarketForge Live Verification"
echo "Mode: $MODE"
echo "Engine: $ENGINE_DIR"
echo "Logs: $LOG_DIR"

case "$MODE" in
quick)
  verify_format
  verify_live_check
  verify_live_unit
  ;;

full)
  verify_format
  verify_live_check
  verify_live_unit
  verify_live_integration
  verify_live_docs
  ;;

tui)
  verify_live_tui
  ;;

historical)
  verify_historical
  ;;
esac

echo
echo "=================================================="
echo "VERIFICATION SUMMARY"
echo "=================================================="

echo "Passed: ${#PASSED[@]}"
echo "Failed: ${#FAILED[@]}"

if ((${#PASSED[@]} > 0)); then
  echo
  echo "Successful gates:"
  printf '  ✓ %s\n' "${PASSED[@]}"
fi

if ((${#FAILED[@]} > 0)); then
  echo
  echo "Failed gates:"
  printf '  ✗ %s\n' "${FAILED[@]}"

  echo
  echo "VERIFICATION FAILED"
  echo "Logs: $LOG_DIR"

  exit 1
fi

echo
echo "VERIFICATION PASSED"
echo "Logs: $LOG_DIR"

exit 0
