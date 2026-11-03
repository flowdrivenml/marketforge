#!/usr/bin/env bash
# Compile separately; run only normal cases, each in its own bounded process.
# Slow test targets and benchmark entrypoints are NEVER executed by this script.
set -Eeuo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT/engine"
MODE="${1:-normal}"
case "$MODE" in normal|compile) ;; *) echo 'Usage: scripts/verify-historical.sh {normal|compile}' >&2; exit 2;; esac
LOG_DIR="$ROOT/engine/target/historical-verification/$(date -u +%Y%m%dT%H%M%SZ)-$$"
mkdir -p "$LOG_DIR"
echo "Historical verification: $MODE; logs: $LOG_DIR"
FLAGS=(--locked --offline --no-default-features)
run() { local label="$1"; shift; echo "CHECK: $label"; "$@" >"$LOG_DIR/$label.log" 2>&1; }
run historical-check cargo check "${FLAGS[@]}" --features historical
run live-compatibility-check cargo check "${FLAGS[@]}" --features live --lib
run minimal-check cargo check "${FLAGS[@]}" --lib
NORMAL=(--test historical_normalize --test historical_merge --test historical_convert)
SHARED=(--test depth_spec --test depth_processor --test depth_transforms --test depth_scalar --test depth_csv_adapter --test depth_xlsx_decoder)
cargo test "${FLAGS[@]}" --features historical "${NORMAL[@]}" "${SHARED[@]}" --no-run --message-format=json >"$LOG_DIR/normal-artifacts.jsonl" 2>"$LOG_DIR/normal-build.log"
cargo test "${FLAGS[@]}" --features historical-slow-tests --test historical_slow_normalize --test historical_slow_merge --test historical_slow_convert --no-run --message-format=json >"$LOG_DIR/slow-artifacts.jsonl" 2>"$LOG_DIR/slow-build.log"
if [[ "$MODE" == normal ]]; then
    # Existing supervisor: exact-case execution, 30s deadline, process-group cleanup.
    python3 scripts/bounded_tests.py "$LOG_DIR/normal-artifacts.jsonl" "$LOG_DIR/normal-run" --run --targets '(historical_(normalize|merge|convert)|depth_(spec|processor|transforms|scalar|csv_adapter|xlsx_decoder))' | tee "$LOG_DIR/results.log"
fi
echo "Historical verification passed ($MODE). Slow cases compiled only. Logs: $LOG_DIR"
