#!/usr/bin/env bash

set -uo pipefail

# ==============================================================================
# MarketForge — Historical Raw Fixture Capture
# ==============================================================================
#
# Downloads a small representative historical dataset for offline parser,
# normalization, and Rust processing tests.
#
# Files are downloaded through the MarketForge CLI exactly as normal users
# acquire historical data.
#
# Output:
#
#   tests/fixtures/raw/
#
# Default fixture interval:
#
#   2026-09-01 <= date < 2026-09-08
#
# Usage:
#
#   ./scripts/capture_raw_fixtures.sh
#
#   ./scripts/capture_raw_fixtures.sh bybit
#   ./scripts/capture_raw_fixtures.sh binance
#   ./scripts/capture_raw_fixtures.sh okx
#   ./scripts/capture_raw_fixtures.sh bitget
#   ./scripts/capture_raw_fixtures.sh gateio
#
#   ./scripts/capture_raw_fixtures.sh bybit spot
#   ./scripts/capture_raw_fixtures.sh bybit option
#   ./scripts/capture_raw_fixtures.sh binance linear
#
# Environment:
#
#   FIXTURE_START=2026-09-01
#   FIXTURE_END=2026-09-08
#   FIXTURE_ROOT=tests/fixtures
#
# ==============================================================================

START="${FIXTURE_START:-2026-09-01}"
END="${FIXTURE_END:-2026-09-08}"
DATA_ROOT="${FIXTURE_ROOT:-tests/fixtures}"

EXCHANGE="${1:-all}"
MARKET="${2:-all}"

# ==============================================================================
# Helpers
# ==============================================================================

section() {
  printf '\n'
  printf '%s\n' "================================================================================"
  printf '%s\n' "$1"
  printf '%s\n' "================================================================================"
}

info() {
  printf '[INFO] %s\n' "$*"
}

warn() {
  printf '[WARN] %s\n' "$*" >&2
}

usage() {
  cat <<EOF
MarketForge historical raw fixture capture

Usage:
    $0 [exchange] [market]

Exchanges:
    all
    bybit
    binance
    okx
    bitget
    gateio

Examples:
    $0
    $0 bybit
    $0 bybit option
    $0 binance spot
    $0 okx swap
    $0 gateio linear

Environment:
    FIXTURE_START   Start date, inclusive
                    default: 2026-09-01

    FIXTURE_END     End date, exclusive
                    default: 2026-09-08

    FIXTURE_ROOT    MarketForge data root
                    default: tests/fixtures

Output:
    ${DATA_ROOT}/raw/<exchange>/...
EOF
}

case "$EXCHANGE" in
-h | --help | help)
  usage
  exit 0
  ;;
esac

if ! command -v marketforge >/dev/null 2>&1; then
  echo "ERROR: marketforge not found in PATH" >&2
  exit 1
fi

want_exchange() {
  local name="$1"

  [[ "$EXCHANGE" == "all" || "$EXCHANGE" == "$name" ]]
}

want_market() {
  local name="$1"

  [[ "$MARKET" == "all" || "$MARKET" == "$name" ]]
}

# ==============================================================================
# Download wrapper
# ==============================================================================

download_symbol() {
  local exchange="$1"
  local type="$2"
  local category="$3"
  local symbol="$4"
  local data="$5"

  info \
    "$exchange | $type | $category | $data | $symbol"

  if ! marketforge download \
    --exchange "$exchange" \
    --type "$type" \
    --category "$category" \
    --symbol "$symbol" \
    --data "$data" \
    --start "$START" \
    --end "$END" \
    --data-root "$DATA_ROOT"; then
    warn \
      "FAILED: $exchange / $type / $category / $data / $symbol"
  fi
}

download_base_coin() {
  local exchange="$1"
  local type="$2"
  local category="$3"
  local base_coin="$4"
  local data="$5"

  info \
    "$exchange | $type | $category | $data | base=$base_coin"

  if ! marketforge download \
    --exchange "$exchange" \
    --type "$type" \
    --category "$category" \
    --base-coin "$base_coin" \
    --data "$data" \
    --start "$START" \
    --end "$END" \
    --data-root "$DATA_ROOT"; then
    warn \
      "FAILED: $exchange / $type / $category / $data / $base_coin"
  fi
}

capture_symbol_pair() {
  local exchange="$1"
  local type="$2"
  local category="$3"
  local symbol="$4"

  download_symbol \
    "$exchange" \
    "$type" \
    "$category" \
    "$symbol" \
    "trade_ticks"

  download_symbol \
    "$exchange" \
    "$type" \
    "$category" \
    "$symbol" \
    "order_book_l2"
}

capture_base_coin_pair() {
  local exchange="$1"
  local type="$2"
  local category="$3"
  local base_coin="$4"

  download_base_coin \
    "$exchange" \
    "$type" \
    "$category" \
    "$base_coin" \
    "trade_ticks"

  download_base_coin \
    "$exchange" \
    "$type" \
    "$category" \
    "$base_coin" \
    "order_book_l2"
}

# ==============================================================================
# Bybit
# ==============================================================================

capture_bybit() {
  section "BYBIT"

  if want_market spot; then
    capture_symbol_pair \
      "bybit" \
      "spot" \
      "spot" \
      "BTCUSDT"
  fi

  if want_market linear; then
    capture_symbol_pair \
      "bybit" \
      "future" \
      "linear" \
      "BTCUSDT"
  fi

  if want_market inverse; then
    capture_symbol_pair \
      "bybit" \
      "future" \
      "inverse" \
      "BTCUSD"
  fi

  if want_market option; then
    capture_base_coin_pair \
      "bybit" \
      "option" \
      "option" \
      "BTC"
  fi
}

# ==============================================================================
# Binance
# ==============================================================================

capture_binance() {
  section "BINANCE"

  if want_market spot; then
    capture_symbol_pair \
      "binance" \
      "spot" \
      "spot" \
      "BTCUSDT"
  fi

  if want_market linear; then
    capture_symbol_pair \
      "binance" \
      "future" \
      "linear" \
      "BTCUSDT"
  fi

  if want_market inverse; then
    capture_symbol_pair \
      "binance" \
      "future" \
      "inverse" \
      "BTCUSD_PERP"
  fi
}

# ==============================================================================
# OKX
# ==============================================================================

capture_okx() {
  section "OKX"

  if want_market spot; then
    capture_symbol_pair \
      "okx" \
      "spot" \
      "spot" \
      "BTC-USDT"
  fi

  if want_market swap; then
    capture_symbol_pair \
      "okx" \
      "swap" \
      "linear" \
      "BTC-USDT-SWAP"
  fi

  if want_market option; then
    capture_base_coin_pair \
      "okx" \
      "option" \
      "option" \
      "BTC"
  fi
}

# ==============================================================================
# Bitget
# ==============================================================================

capture_bitget() {
  section "BITGET"

  if want_market spot; then
    capture_symbol_pair \
      "bitget" \
      "spot" \
      "spot" \
      "BTCUSDT"
  fi

  if want_market linear; then
    capture_symbol_pair \
      "bitget" \
      "future" \
      "linear" \
      "BTCUSDT"
  fi
}

# ==============================================================================
# Gate.io
# ==============================================================================

capture_gateio() {
  section "GATE.IO"

  if want_market spot; then
    capture_symbol_pair \
      "gateio" \
      "spot" \
      "spot" \
      "BTC_USDT"
  fi

  if want_market linear; then
    capture_symbol_pair \
      "gateio" \
      "future" \
      "linear" \
      "BTC_USDT"
  fi

  if want_market inverse; then
    capture_symbol_pair \
      "gateio" \
      "future" \
      "inverse" \
      "BTC_USD"
  fi
}

# ==============================================================================
# Validate arguments
# ==============================================================================

case "$EXCHANGE" in
all | bybit | binance | okx | bitget | gateio)
  ;;
*)
  echo "ERROR: unknown exchange: $EXCHANGE" >&2
  echo
  usage
  exit 2
  ;;
esac

# ==============================================================================
# Capture
# ==============================================================================

section "MARKETFORGE RAW HISTORICAL FIXTURES"

printf 'Date range : %s -> %s (end exclusive)\n' "$START" "$END"
printf 'Data root  : %s\n' "$DATA_ROOT"
printf 'Exchange   : %s\n' "$EXCHANGE"
printf 'Market     : %s\n' "$MARKET"

if want_exchange bybit; then
  capture_bybit
fi

if want_exchange binance; then
  capture_binance
fi

if want_exchange okx; then
  capture_okx
fi

if want_exchange bitget; then
  capture_bitget
fi

if want_exchange gateio; then
  capture_gateio
fi

# ==============================================================================
# Inventory
# ==============================================================================

section "RAW FIXTURE INVENTORY"

RAW_ROOT="$DATA_ROOT/raw"

if [[ -d "$RAW_ROOT" ]]; then
  find "$RAW_ROOT" \
    -type f \
    -printf '%p  %s bytes\n' |
    sort
else
  warn "No raw fixture directory found: $RAW_ROOT"
fi

# ==============================================================================
# Summary
# ==============================================================================

section "DONE"

printf '%s\n' \
  "Historical raw fixtures:" \
  "  $RAW_ROOT"

printf '\n%s\n' \
  "The files remain in their original exchange formats."

printf '%s\n' \
  "No decompression, parsing, normalization, or Rust processing was performed."
