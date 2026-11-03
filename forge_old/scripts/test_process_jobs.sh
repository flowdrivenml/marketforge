#!/usr/bin/env bash

set -uo pipefail

START="2026-09-01"
END="2026-09-04"
PROFILE="default"

PASSED=0
FAILED=0
FAILURES=()

TMP_DIR="$(mktemp -d)"

cleanup() {
  rm -rf "$TMP_DIR"
}

trap cleanup EXIT

create_job() {
  local exchange="$1"
  local type="$2"
  local category="$3"
  local symbol="$4"
  local raw_data="$5"
  local dataset="$6"

  local output_file
  local status
  local job_number

  job_number=$((PASSED + FAILED + 1))
  output_file="$TMP_DIR/process-$job_number.log"

  echo
  echo "[$job_number] Creating process job:"
  echo "  $exchange | $type | $category | $symbol | $raw_data -> $dataset"

  marketforge process \
    --exchange "$exchange" \
    --type "$type" \
    --category "$category" \
    --symbol "$symbol" \
    --data "$raw_data" \
    --dataset "$dataset" \
    --start "$START" \
    --end "$END" \
    --profile "$PROFILE" \
    >"$output_file" 2>&1

  status=$?

  if [ "$status" -eq 0 ]; then
    PASSED=$((PASSED + 1))
    echo "  OK"
  else
    FAILED=$((FAILED + 1))

    FAILURES+=(
      "$status|$exchange|$type|$category|$symbol|$raw_data|$dataset|$output_file"
    )

    echo "  FAILED"
  fi
}

create_market_jobs() {
  local exchange="$1"
  local type="$2"
  local category="$3"
  local symbol="$4"

  create_job \
    "$exchange" \
    "$type" \
    "$category" \
    "$symbol" \
    trade_ticks \
    trade

  create_job \
    "$exchange" \
    "$type" \
    "$category" \
    "$symbol" \
    order_book_l2 \
    l2
}

# ==============================================================================
# LINEAR PERPETUAL
# ==============================================================================

create_market_jobs \
  bybit \
  perpetual \
  linear \
  BTCUSDT

create_job \
  binance \
  perpetual \
  linear \
  BTCUSDT \
  trade_ticks \
  trade

create_market_jobs \
  okx \
  perpetual \
  linear \
  BTC-USDT-SWAP

create_market_jobs \
  bitget \
  perpetual \
  linear \
  BTCUSDT

create_market_jobs \
  gateio \
  perpetual \
  linear \
  BTC_USDT

# ==============================================================================
# INVERSE PERPETUAL
# ==============================================================================

create_market_jobs \
  bybit \
  perpetual \
  inverse \
  BTCUSD

create_job \
  binance \
  perpetual \
  inverse \
  BTCUSD_PERP \
  trade_ticks \
  trade

create_market_jobs \
  okx \
  perpetual \
  inverse \
  BTC-USD-SWAP

create_market_jobs \
  gateio \
  perpetual \
  inverse \
  BTC_USD

# ==============================================================================
# BTC/USDT SPOT
# ==============================================================================

create_market_jobs \
  bybit \
  spot \
  spot \
  BTCUSDT

create_job \
  binance \
  spot \
  spot \
  BTCUSDT \
  trade_ticks \
  trade

create_market_jobs \
  okx \
  spot \
  spot \
  BTC-USDT

create_market_jobs \
  bitget \
  spot \
  spot \
  BTCUSDT

create_market_jobs \
  gateio \
  spot \
  spot \
  BTC_USDT

# ==============================================================================
# ETH/USDC SPOT
# ==============================================================================

create_market_jobs \
  bybit \
  spot \
  spot \
  ETHUSDC

create_job \
  binance \
  spot \
  spot \
  ETHUSDC \
  trade_ticks \
  trade

create_market_jobs \
  bitget \
  spot \
  spot \
  ETHUSDC

# ==============================================================================
# DATED FUTURES
#
# Binance historical depth is not supported, so only trade jobs are generated.
# ==============================================================================

create_job \
  binance \
  future \
  linear \
  BTCUSDT_261225 \
  trade_ticks \
  trade

create_job \
  binance \
  future \
  inverse \
  BTCUSD_261225 \
  trade_ticks \
  trade

create_market_jobs \
  okx \
  future \
  linear \
  BTC-USD_UM-261225

create_market_jobs \
  okx \
  future \
  inverse \
  BTC-USD-261225

# ==============================================================================
# RESULT
# ==============================================================================

echo
echo "================================================================================"
echo "PROCESS JOB RESULT"
echo "================================================================================"
echo "Successful : $PASSED"
echo "Failed     : $FAILED"
echo "Total      : $((PASSED + FAILED))"

if [ "$FAILED" -eq 0 ]; then
  echo
  echo "All process jobs created successfully."
  echo
  echo "Jobs:"
  echo "  data/.jobs/process/"
  echo "================================================================================"

  exit 0
fi

# ==============================================================================
# FAILURES
# ==============================================================================

echo
echo "================================================================================"
echo "FAILED PROCESS JOBS"
echo "================================================================================"

for failure in "${FAILURES[@]}"; do
  IFS='|' read -r \
    status \
    exchange \
    type \
    category \
    symbol \
    raw_data \
    dataset \
    output_file \
    <<<"$failure"

  echo
  echo "--------------------------------------------------------------------------------"
  echo "FAILED [$status]"
  echo "--------------------------------------------------------------------------------"
  echo
  echo "Exchange : $exchange"
  echo "Type     : $type"
  echo "Category : $category"
  echo "Symbol   : $symbol"
  echo "Raw data : $raw_data"
  echo "Dataset  : $dataset"
  echo
  echo "Command:"
  echo
  echo "marketforge process \\"
  echo "    --exchange $exchange \\"
  echo "    --type $type \\"
  echo "    --category $category \\"
  echo "    --symbol $symbol \\"
  echo "    --data $raw_data \\"
  echo "    --dataset $dataset \\"
  echo "    --start $START \\"
  echo "    --end $END \\"
  echo "    --profile $PROFILE"
  echo
  echo "Error:"
  echo "--------------------------------------------------------------------------------"

  cat "$output_file"
done

# ==============================================================================
# FINAL SUMMARY
# ==============================================================================

echo
echo "================================================================================"
echo "FINAL RESULT"
echo "================================================================================"
echo "Successful : $PASSED"
echo "Failed     : $FAILED"
echo "Total      : $((PASSED + FAILED))"
echo
echo "Jobs:"
echo "  data/.jobs/process/"
echo "================================================================================"

exit 1
