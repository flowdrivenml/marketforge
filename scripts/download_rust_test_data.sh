#!/usr/bin/env bash

set -uo pipefail

START="2026-09-01"
END="2026-09-04"

PASSED=0
FAILED=0
FAILURES=()

TMP_DIR="$(mktemp -d)"

cleanup() {
  rm -rf "$TMP_DIR"
}

trap cleanup EXIT

download_dataset() {
  local exchange="$1"
  local type="$2"
  local category="$3"
  local symbol="$4"
  local data="$5"
  local family="${6:-}"

  local output_file
  local status
  local test_number

  test_number=$((PASSED + FAILED + 1))
  output_file="$TMP_DIR/download-$test_number.log"

  echo
  echo "[$test_number] Downloading:"
  echo "  $exchange | $type | $category | $symbol | $data"

  if [ -n "$family" ]; then
    echo "  family: $family"
  fi

  local command=(
    marketforge download
    --exchange "$exchange"
    --type "$type"
    --category "$category"
    --symbol "$symbol"
    --data "$data"
    --start "$START"
    --end "$END"
  )

  if [ -n "$family" ]; then
    command+=(
      --family "$family"
    )
  fi

  "${command[@]}" >"$output_file" 2>&1

  status=$?

  if [ "$status" -eq 0 ]; then
    PASSED=$((PASSED + 1))
    echo "  OK"
  else
    FAILED=$((FAILED + 1))

    FAILURES+=(
      "$status|$exchange|$type|$category|$symbol|$data|$family|$output_file"
    )

    echo "  FAILED"
  fi
}

download_market() {
  local exchange="$1"
  local type="$2"
  local category="$3"
  local symbol="$4"
  local family="${5:-}"

  download_dataset \
    "$exchange" \
    "$type" \
    "$category" \
    "$symbol" \
    trade_ticks \
    "$family"

  download_dataset \
    "$exchange" \
    "$type" \
    "$category" \
    "$symbol" \
    order_book_l2 \
    "$family"
}

# ==============================================================================
# LINEAR PERPETUAL
#
# Primary BTC derivative corpus for cross-exchange processing and merging.
#
# Binance has historical trades only in the current MarketForge acquisition
# scope, so it is intentionally not passed through download_market().
# ==============================================================================

download_market \
  bybit \
  perpetual \
  linear \
  BTCUSDT

download_dataset \
  binance \
  perpetual \
  linear \
  BTCUSDT \
  trade_ticks

download_market \
  okx \
  perpetual \
  linear \
  BTC-USDT-SWAP \
  BTC-USDT

download_market \
  bitget \
  perpetual \
  linear \
  BTCUSDT

download_market \
  gateio \
  perpetual \
  linear \
  BTC_USDT

# ==============================================================================
# INVERSE PERPETUAL
#
# Exercises inverse normalization and contract economics.
#
# Binance BTCUSD_PERP:
#     quantity_type        = contracts
#     contract_value       = 100
#     contract_value_asset = USD
#
# OKX BTC-USD-SWAP:
#     quantity_type        = contracts
#     contract_value       = 100
#     contract_value_asset = USD
#
# Gate.io BTC_USD:
#     quantity_type        = contracts
#     contract_value       = 1
#     contract_value_asset = USD
# ==============================================================================

download_market \
  bybit \
  perpetual \
  inverse \
  BTCUSD

download_dataset \
  binance \
  perpetual \
  inverse \
  BTCUSD_PERP \
  trade_ticks

download_market \
  okx \
  perpetual \
  inverse \
  BTC-USD-SWAP \
  BTC-USD

download_market \
  gateio \
  perpetual \
  inverse \
  BTC_USD

# ==============================================================================
# BTC/USDT SPOT
#
# Cross-exchange spot normalization and merging.
# ==============================================================================

download_market \
  bybit \
  spot \
  spot \
  BTCUSDT

download_dataset \
  binance \
  spot \
  spot \
  BTCUSDT \
  trade_ticks

download_market \
  okx \
  spot \
  spot \
  BTC-USDT

download_market \
  bitget \
  spot \
  spot \
  BTCUSDT

download_market \
  gateio \
  spot \
  spot \
  BTC_USDT

# ==============================================================================
# ETH/USDC SPOT
#
# Non-USDT quote cross-exchange fixture.
#
# This verifies that processing and merge logic are not implicitly tied to
# BTC/USDT or USDT-denominated markets.
# ==============================================================================

download_market \
  bybit \
  spot \
  spot \
  ETHUSDC

download_dataset \
  binance \
  spot \
  spot \
  ETHUSDC \
  trade_ticks

download_market \
  bitget \
  spot \
  spot \
  ETHUSDC

# ==============================================================================
# DATED FUTURES
#
# Exercises:
#
#     instrument_kind = future
#
# independently from:
#
#     contract_kind = linear / inverse
#
# Binance provides historical trades only in the current acquisition scope.
# ==============================================================================

# ------------------------------------------------------------------------------
# Binance linear future
#
# quantity_type = base
# ------------------------------------------------------------------------------

download_dataset \
  binance \
  future \
  linear \
  BTCUSDT_261225 \
  trade_ticks

# ------------------------------------------------------------------------------
# Binance inverse future
#
# quantity_type        = contracts
# contract_value       = 100
# contract_value_asset = USD
# ------------------------------------------------------------------------------

download_dataset \
  binance \
  future \
  inverse \
  BTCUSD_261225 \
  trade_ticks

# ------------------------------------------------------------------------------
# OKX linear future
#
# quantity_type        = contracts
# contract_value       = 0.01 BTC
# family               = BTC-USD
# ------------------------------------------------------------------------------

download_market \
  okx \
  future \
  linear \
  BTC-USD_UM-261225 \
  BTC-USD_UM

# ------------------------------------------------------------------------------
# OKX inverse future
#
# quantity_type        = contracts
# contract_value       = 100 USD
# family               = BTC-USD
# ------------------------------------------------------------------------------

download_market \
  okx \
  future \
  inverse \
  BTC-USD-261225 \
  BTC-USD

# ==============================================================================
# RESULTS
# ==============================================================================

echo
echo "================================================================================"
echo "DOWNLOAD RESULT"
echo "================================================================================"
echo "Successful : $PASSED"
echo "Failed     : $FAILED"
echo "Total      : $((PASSED + FAILED))"

if [ "$FAILED" -eq 0 ]; then
  echo
  echo "All datasets downloaded successfully."
  echo
  echo "Raw data:"
  echo "  data/raw/"
  echo "================================================================================"

  exit 0
fi

# ==============================================================================
# FAILURES
# ==============================================================================

echo
echo "================================================================================"
echo "FAILED DOWNLOADS"
echo "================================================================================"

for failure in "${FAILURES[@]}"; do
  IFS='|' read -r \
    status \
    exchange \
    type \
    category \
    symbol \
    data \
    family \
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
  echo "Data     : $data"

  if [ -n "$family" ]; then
    echo "Family   : $family"
  fi

  echo
  echo "Command:"
  echo

  echo "marketforge download \\"
  echo "    --exchange $exchange \\"
  echo "    --type $type \\"
  echo "    --category $category \\"
  echo "    --symbol $symbol \\"
  echo "    --data $data \\"

  if [ -n "$family" ]; then
    echo "    --family $family \\"
  fi

  echo "    --start $START \\"
  echo "    --end $END"

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
echo "Raw data:"
echo "  data/raw/"
echo "================================================================================"

exit 1
