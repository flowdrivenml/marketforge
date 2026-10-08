#!/usr/bin/env bash

set -u
set -o pipefail

# ==============================================================================
# MarketForge Merge Job Test
#
# Generates representative merge-job JSON manifests.
#
# Input datasets are temporarily marked complete for Python orchestration
# testing. Rust execution is intentionally separate.
# ==============================================================================

successful=0
failed=0

declare -a FAILURES

run_merge() {
  local name="$1"
  shift

  local datasets=("$@")

  local number=$((successful + failed + 1))

  echo
  echo "[$number] Creating merge job:"
  echo "  $name"
  echo "  datasets: ${datasets[*]}"

  local command=(
    marketforge
    merge
    "${datasets[@]}"
    --profile
    default
  )

  local output
  output="$("${command[@]}" 2>&1)"
  local status=$?

  if [[ $status -eq 0 ]]; then
    echo "  OK"
    ((successful += 1))
    return
  fi

  echo "  FAILED"
  ((failed += 1))

  FAILURES+=(
    "--------------------------------------------------------------------------------
FAILED [$number]
--------------------------------------------------------------------------------

$name

Datasets:
${datasets[*]}

Command:

${command[*]}

Error:
--------------------------------------------------------------------------------
$output"
  )
}

# ------------------------------------------------------------------------------
# 1. Trade + L2
#
# Basic heterogeneous canonical merge for one instrument.
# ------------------------------------------------------------------------------

run_merge \
  "Bybit BTC perpetual: trade + L2" \
  88 89

# ------------------------------------------------------------------------------
# 2. Cross-exchange full microstructure
#
# Bybit and OKX both contribute trades and depth.
# ------------------------------------------------------------------------------

run_merge \
  "Bybit + OKX BTC perpetual: full microstructure" \
  88 89 91 92

# ------------------------------------------------------------------------------
# 3. Spot + perpetual + future
#
# Verifies that spot and contract streams may coexist chronologically.
# ------------------------------------------------------------------------------

run_merge \
  "BTC spot + perpetual + future" \
  104 88 120 121

# ------------------------------------------------------------------------------
# 4. Linear + inverse contracts
#
# Exercises different contract semantics, including inverse contract value.
# ------------------------------------------------------------------------------

run_merge \
  "BTC linear + inverse contracts" \
  88 99 118 119

# ------------------------------------------------------------------------------
# 5. Large mixed chronological merge
#
# Exercises in one job:
#
#   - different underlying assets
#   - different quote assets
#   - spot
#   - perpetual
#   - futures
#   - linear
#   - inverse
#   - trades
#   - L2
#   - multiple exchanges
#
# This is intentionally the broad stress case rather than many repetitive
# pairwise merge tests.
# ------------------------------------------------------------------------------

run_merge \
  "Mixed chronological stress merge" \
  88 89 \
  91 92 \
  99 \
  104 105 \
  113 \
  118 119 \
  120 121

# ------------------------------------------------------------------------------
# Result
# ------------------------------------------------------------------------------

total=$((successful + failed))

echo
echo "================================================================================"
echo "MERGE JOB RESULT"
echo "================================================================================"
echo "Successful : $successful"
echo "Failed     : $failed"
echo "Total      : $total"

if ((failed > 0)); then
  echo
  echo "================================================================================"
  echo "FAILURES"
  echo "================================================================================"

  for failure in "${FAILURES[@]}"; do
    echo
    echo "$failure"
  done
else
  echo
  echo "All merge jobs created successfully."
fi

echo
echo "Jobs:"
echo "  data/.jobs/merge/"
echo "================================================================================"

if ((failed > 0)); then
  exit 1
fi

exit 0
