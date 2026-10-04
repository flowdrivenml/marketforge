#!/usr/bin/env bash

set -uo pipefail

# ==============================================================================
# MarketForge — Live Fixture Capture
# ==============================================================================
#
# Captures raw live market-data fixtures for:
#
#   Binance
#   Bybit
#   Bitget
#   OKX
#   Gate.io
#
# Output:
#
#   tests/fixtures/live/<exchange>/<market>/
#
# Principles:
#
#   - Raw source data only.
#   - No normalization.
#   - No jq transformation of captured payloads.
#   - Expiring instruments are discovered dynamically.
#   - Quiet streams are bounded by timeout.
#   - Individual capture failures do not abort the entire run.
#
# Usage:
#
#   ./scripts/capture_live_fixtures.sh
#   ./scripts/capture_live_fixtures.sh binance
#   ./scripts/capture_live_fixtures.sh bybit
#   ./scripts/capture_live_fixtures.sh okx linear_future
#   ./scripts/capture_live_fixtures.sh gateio linear_delivery
#   ./scripts/capture_live_fixtures.sh --help
#
# ==============================================================================

ROOT="${MARKETFORGE_ROOT:-tests/fixtures/live}"

WS_MESSAGES="${WS_MESSAGES:-6}"
WS_TIMEOUT="${WS_TIMEOUT:-30}"
HTTP_TIMEOUT="${HTTP_TIMEOUT:-30}"

EXCHANGE="${1:-all}"
MARKET="${2:-all}"

# ==============================================================================
# Output helpers
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

error() {
  printf '[ERROR] %s\n' "$*" >&2
}

# ==============================================================================
# Help
# ==============================================================================

usage() {
  cat <<'EOF'
MarketForge live fixture capture

Usage:
    capture_live_fixtures.sh [exchange] [market]

Exchanges:
    all
    binance
    bybit
    bitget
    okx
    gateio

Examples:
    ./scripts/capture_live_fixtures.sh
    ./scripts/capture_live_fixtures.sh binance
    ./scripts/capture_live_fixtures.sh bybit option
    ./scripts/capture_live_fixtures.sh okx linear_future
    ./scripts/capture_live_fixtures.sh gateio inverse_perpetual

Environment:
    MARKETFORGE_ROOT   Fixture root directory
                       default: tests/fixtures/live

    WS_MESSAGES        Maximum WebSocket messages per capture
                       default: 6

    WS_TIMEOUT         WebSocket capture timeout in seconds
                       default: 30

    HTTP_TIMEOUT       REST timeout in seconds
                       default: 30

Behavior:
    WebSocket and REST payloads are written exactly as received.

    jq is used only for:
      - constructing subscription requests
      - instrument discovery
      - validation

    jq output is never used to rewrite captured market-data fixtures.
EOF
}

case "$EXCHANGE" in
-h | --help | help)
  usage
  exit 0
  ;;
esac

# ==============================================================================
# Dependencies
# ==============================================================================

require_command() {
  local command_name="$1"

  if ! command -v "$command_name" >/dev/null 2>&1; then
    error "Missing dependency: $command_name"
    exit 1
  fi
}

require_command curl
require_command jq
require_command websocat
require_command timeout
require_command date
require_command find

# ==============================================================================
# Selection
# ==============================================================================

want_exchange() {
  local name="$1"

  [[ "$EXCHANGE" == "all" || "$EXCHANGE" == "$name" ]]
}

want_market() {
  local name="$1"

  [[ "$MARKET" == "all" || "$MARKET" == "$name" ]]
}

# ==============================================================================
# Filesystem
# ==============================================================================

prepare_dir() {
  mkdir -p "$1"
}

write_meta() {
  local directory="$1"
  local exchange="$2"
  local market="$3"
  local instrument="$4"

  jq -n \
    --arg exchange "$exchange" \
    --arg market "$market" \
    --arg instrument "$instrument" \
    --arg captured_at "$(date -u +"%Y-%m-%dT%H:%M:%SZ")" \
    '{
            exchange: $exchange,
            market: $market,
            instrument: $instrument,
            captured_at: $captured_at,
            source: "live"
        }' \
    >"$directory/capture.meta.json"
}

# ==============================================================================
# REST capture
# ==============================================================================

capture_rest() {
  local url="$1"
  local output="$2"

  info "REST -> $output"

  local tmp="${output}.tmp"

  rm -f "$tmp"

  if curl \
    --fail \
    --silent \
    --show-error \
    --max-time "$HTTP_TIMEOUT" \
    "$url" \
    >"$tmp"; then
    mv "$tmp" "$output"
    return 0
  fi

  rm -f "$tmp"

  warn "REST capture failed: $url"
  return 1
}

# ==============================================================================
# WebSocket capture
# ==============================================================================

capture_ws_direct() {
  local url="$1"
  local output="$2"

  info "WS -> $output"

  local tmp="${output}.tmp"

  rm -f "$tmp"

  timeout "$WS_TIMEOUT" \
    websocat \
    --max-messages "$WS_MESSAGES" \
    "$url" \
    >"$tmp"

  local status=$?

  # timeout(1) returns 124 when the capture duration expires.
  if [[ "$status" -eq 0 || "$status" -eq 124 ]]; then
    mv "$tmp" "$output"
    return 0
  fi

  rm -f "$tmp"

  warn "WebSocket capture failed: $url"
  return "$status"
}

capture_ws_subscription() {
  local url="$1"
  local subscription="$2"
  local output="$3"

  info "WS -> $output"

  local tmp="${output}.tmp"

  rm -f "$tmp"

  printf '%s\n' "$subscription" |
    timeout "$WS_TIMEOUT" \
      websocat \
      --max-messages "$WS_MESSAGES" \
      "$url" \
      >"$tmp"

  local status=$?

  if [[ "$status" -eq 0 || "$status" -eq 124 ]]; then
    mv "$tmp" "$output"
    return 0
  fi

  rm -f "$tmp"

  warn "WebSocket capture failed: $url"
  return "$status"
}

# ==============================================================================
# JSON validation
# ==============================================================================

validate_json() {
  local file="$1"

  if [[ ! -s "$file" ]]; then
    warn "EMPTY: $file"
    return 1
  fi

  if jq -e . "$file" >/dev/null 2>&1; then
    info "VALID JSON: $file"
    return 0
  fi

  warn "INVALID JSON: $file"
  return 1
}

validate_jsonl() {
  local file="$1"

  if [[ ! -s "$file" ]]; then
    warn "EMPTY: $file"
    return 1
  fi

  if jq -c . "$file" >/dev/null 2>&1; then
    info "VALID JSONL: $file"
    return 0
  fi

  warn "INVALID JSONL: $file"
  return 1
}

# ==============================================================================
# Binance
# ==============================================================================

capture_binance() {
  section "BINANCE"

  # --------------------------------------------------------------------------
  # Spot
  # --------------------------------------------------------------------------

  if want_market spot; then
    local dir="$ROOT/binance/spot"

    prepare_dir "$dir"

    write_meta \
      "$dir" \
      "binance" \
      "spot" \
      "BTCUSDT"

    capture_ws_direct \
      "wss://stream.binance.com:9443/ws/btcusdt@trade" \
      "$dir/trades.raw.jsonl"

    capture_ws_direct \
      "wss://stream.binance.com:9443/ws/btcusdt@depth@100ms" \
      "$dir/books.raw.jsonl"

    capture_rest \
      "https://api.binance.com/api/v3/depth?symbol=BTCUSDT&limit=1000" \
      "$dir/book.rest.snapshot.json"
  fi

  # --------------------------------------------------------------------------
  # Linear
  # --------------------------------------------------------------------------

  if want_market linear; then
    local dir="$ROOT/binance/linear"

    prepare_dir "$dir"

    write_meta \
      "$dir" \
      "binance" \
      "linear" \
      "BTCUSDT"

    capture_ws_direct \
      "wss://fstream.binance.com/ws/btcusdt@trade" \
      "$dir/trades.raw.jsonl"

    capture_ws_direct \
      "wss://fstream.binance.com/ws/btcusdt@depth@100ms" \
      "$dir/books.raw.jsonl"

    capture_rest \
      "https://fapi.binance.com/fapi/v1/depth?symbol=BTCUSDT&limit=1000" \
      "$dir/book.rest.snapshot.json"
  fi

  # --------------------------------------------------------------------------
  # Inverse
  # --------------------------------------------------------------------------

  if want_market inverse; then
    local dir="$ROOT/binance/inverse"

    prepare_dir "$dir"

    write_meta \
      "$dir" \
      "binance" \
      "inverse" \
      "BTCUSD_PERP"

    capture_ws_direct \
      "wss://dstream.binance.com/ws/btcusd_perp@trade" \
      "$dir/trades.raw.jsonl"

    capture_ws_direct \
      "wss://dstream.binance.com/ws/btcusd_perp@depth@100ms" \
      "$dir/books.raw.jsonl"

    capture_rest \
      "https://dapi.binance.com/dapi/v1/depth?symbol=BTCUSD_PERP&limit=1000" \
      "$dir/book.rest.snapshot.json"
  fi
}

# ==============================================================================
# Bybit discovery
# ==============================================================================

discover_bybit_option() {
  curl \
    --fail \
    --silent \
    --show-error \
    --max-time "$HTTP_TIMEOUT" \
    'https://api.bybit.com/v5/market/instruments-info?category=option&baseCoin=BTC&limit=1000' |
    jq -r '
            .result.list
            | map(
                select(.status == "Trading")
            )
            | sort_by(
                (.deliveryTime | tonumber),
                .symbol
            )
            | .[0].symbol // empty
        '
}

# ==============================================================================
# Bybit
# ==============================================================================

capture_bybit() {
  section "BYBIT"

  # --------------------------------------------------------------------------
  # Spot
  # --------------------------------------------------------------------------

  if want_market spot; then
    local dir="$ROOT/bybit/spot"

    prepare_dir "$dir"

    write_meta "$dir" "bybit" "spot" "BTCUSDT"

    capture_ws_subscription \
      "wss://stream.bybit.com/v5/public/spot" \
      '{"op":"subscribe","args":["publicTrade.BTCUSDT"]}' \
      "$dir/trades.raw.jsonl"

    capture_ws_subscription \
      "wss://stream.bybit.com/v5/public/spot" \
      '{"op":"subscribe","args":["orderbook.200.BTCUSDT"]}' \
      "$dir/books.raw.jsonl"

    capture_rest \
      "https://api.bybit.com/v5/market/orderbook?category=spot&symbol=BTCUSDT&limit=200" \
      "$dir/book.rest.snapshot.json"

    capture_ws_subscription \
      "wss://stream.bybit.com/v5/public/spot" \
      '{"op":"subscribe","args":["orderbook.full.BTCUSDT"]}' \
      "$dir/full.books.raw.jsonl"

    capture_rest \
      "https://api.bybit.com/v5/market/full_orderbook?category=spot&symbol=BTCUSDT" \
      "$dir/full.book.rest.snapshot.json"
  fi

  # --------------------------------------------------------------------------
  # Linear
  # --------------------------------------------------------------------------

  if want_market linear; then
    local dir="$ROOT/bybit/linear"

    prepare_dir "$dir"

    write_meta "$dir" "bybit" "linear" "BTCUSDT"

    capture_ws_subscription \
      "wss://stream.bybit.com/v5/public/linear" \
      '{"op":"subscribe","args":["publicTrade.BTCUSDT"]}' \
      "$dir/trades.raw.jsonl"

    capture_ws_subscription \
      "wss://stream.bybit.com/v5/public/linear" \
      '{"op":"subscribe","args":["orderbook.200.BTCUSDT"]}' \
      "$dir/books.raw.jsonl"

    capture_rest \
      "https://api.bybit.com/v5/market/orderbook?category=linear&symbol=BTCUSDT&limit=200" \
      "$dir/book.rest.snapshot.json"

    capture_ws_subscription \
      "wss://stream.bybit.com/v5/public/linear" \
      '{"op":"subscribe","args":["orderbook.full.BTCUSDT"]}' \
      "$dir/full.books.raw.jsonl"

    capture_rest \
      "https://api.bybit.com/v5/market/full_orderbook?category=linear&symbol=BTCUSDT" \
      "$dir/full.book.rest.snapshot.json"
  fi

  # --------------------------------------------------------------------------
  # Inverse
  # --------------------------------------------------------------------------

  if want_market inverse; then
    local dir="$ROOT/bybit/inverse"

    prepare_dir "$dir"

    write_meta "$dir" "bybit" "inverse" "BTCUSD"

    capture_ws_subscription \
      "wss://stream.bybit.com/v5/public/inverse" \
      '{"op":"subscribe","args":["publicTrade.BTCUSD"]}' \
      "$dir/trades.raw.jsonl"

    capture_ws_subscription \
      "wss://stream.bybit.com/v5/public/inverse" \
      '{"op":"subscribe","args":["orderbook.200.BTCUSD"]}' \
      "$dir/books.raw.jsonl"

    capture_rest \
      "https://api.bybit.com/v5/market/orderbook?category=inverse&symbol=BTCUSD&limit=200" \
      "$dir/book.rest.snapshot.json"

    capture_ws_subscription \
      "wss://stream.bybit.com/v5/public/inverse" \
      '{"op":"subscribe","args":["orderbook.full.BTCUSD"]}' \
      "$dir/full.books.raw.jsonl"

    capture_rest \
      "https://api.bybit.com/v5/market/full_orderbook?category=inverse&symbol=BTCUSD" \
      "$dir/full.book.rest.snapshot.json"
  fi

  # --------------------------------------------------------------------------
  # Option
  # --------------------------------------------------------------------------

  if want_market option; then
    local option

    option="$(discover_bybit_option || true)"

    if [[ -z "$option" ]]; then
      warn "Bybit: could not discover active BTC option"
    else
      info "Bybit option: $option"

      local dir="$ROOT/bybit/option"

      prepare_dir "$dir"

      write_meta "$dir" "bybit" "option" "$option"

      # Broad BTC stream is intentionally used for trades because an
      # individual option may remain inactive during a short capture.
      capture_ws_subscription \
        "wss://stream.bybit.com/v5/public/option" \
        '{"op":"subscribe","args":["publicTrade.BTC"]}' \
        "$dir/trades.raw.jsonl"

      capture_ws_subscription \
        "wss://stream.bybit.com/v5/public/option" \
        "{\"op\":\"subscribe\",\"args\":[\"orderbook.25.${option}\"]}" \
        "$dir/books.raw.jsonl"

      capture_rest \
        "https://api.bybit.com/v5/market/orderbook?category=option&symbol=${option}&limit=25" \
        "$dir/book.rest.snapshot.json"
    fi
  fi
}

# ==============================================================================
# Bitget
# ==============================================================================
#
# Bitget symbols used here are stable:
#
#   Spot         BTCUSDT
#   USDT Linear  BTCUSDT
#   USDC Linear  BTCPERP
#
# ==============================================================================

capture_bitget() {
  section "BITGET"

  local ws="wss://ws.bitget.com/v2/ws/public"

  # --------------------------------------------------------------------------
  # Spot
  # --------------------------------------------------------------------------

  if want_market spot; then
    local dir="$ROOT/bitget/spot"

    prepare_dir "$dir"

    write_meta "$dir" "bitget" "spot" "BTCUSDT"

    capture_ws_subscription \
      "$ws" \
      '{"op":"subscribe","args":[{"instType":"SPOT","channel":"trade","instId":"BTCUSDT"}]}' \
      "$dir/trades.raw.jsonl"

    capture_ws_subscription \
      "$ws" \
      '{"op":"subscribe","args":[{"instType":"SPOT","channel":"books","instId":"BTCUSDT"}]}' \
      "$dir/books.raw.jsonl"

    capture_rest \
      "https://api.bitget.com/api/v2/spot/market/orderbook?symbol=BTCUSDT&type=step0&limit=150" \
      "$dir/book.rest.snapshot.json"
  fi

  # --------------------------------------------------------------------------
  # USDT Linear
  # --------------------------------------------------------------------------

  if want_market linear_usdt; then
    local dir="$ROOT/bitget/linear_usdt"

    prepare_dir "$dir"

    write_meta "$dir" "bitget" "linear_usdt" "BTCUSDT"

    capture_ws_subscription \
      "$ws" \
      '{"op":"subscribe","args":[{"instType":"USDT-FUTURES","channel":"trade","instId":"BTCUSDT"}]}' \
      "$dir/trades.raw.jsonl"

    capture_ws_subscription \
      "$ws" \
      '{"op":"subscribe","args":[{"instType":"USDT-FUTURES","channel":"books","instId":"BTCUSDT"}]}' \
      "$dir/books.raw.jsonl"

    capture_rest \
      "https://api.bitget.com/api/v2/mix/market/merge-depth?symbol=BTCUSDT&productType=USDT-FUTURES&precision=scale0&limit=max" \
      "$dir/book.rest.snapshot.json"
  fi

  # --------------------------------------------------------------------------
  # USDC Linear
  # --------------------------------------------------------------------------

  if want_market linear_usdc; then
    local dir="$ROOT/bitget/linear_usdc"

    prepare_dir "$dir"

    write_meta "$dir" "bitget" "linear_usdc" "BTCPERP"

    capture_ws_subscription \
      "$ws" \
      '{"op":"subscribe","args":[{"instType":"USDC-FUTURES","channel":"trade","instId":"BTCPERP"}]}' \
      "$dir/trades.raw.jsonl"

    capture_ws_subscription \
      "$ws" \
      '{"op":"subscribe","args":[{"instType":"USDC-FUTURES","channel":"books","instId":"BTCPERP"}]}' \
      "$dir/books.raw.jsonl"

    capture_rest \
      "https://api.bitget.com/api/v2/mix/market/merge-depth?symbol=BTCPERP&productType=USDC-FUTURES&precision=scale0&limit=max" \
      "$dir/book.rest.snapshot.json"
  fi
}

# ==============================================================================
# OKX discovery
# ==============================================================================

discover_okx_linear_future() {
  curl \
    --fail \
    --silent \
    --show-error \
    --max-time "$HTTP_TIMEOUT" \
    'https://www.okx.com/api/v5/public/instruments?instType=FUTURES' |
    jq -r '
            .data
            | map(
                select(
                    .state == "live"
                    and .ctType == "linear"
                    and .instFamily == "BTC-USD_UM"
                )
            )
            | sort_by(.expTime | tonumber)
            | .[0].instId // empty
        '
}

discover_okx_inverse_future() {
  curl \
    --fail \
    --silent \
    --show-error \
    --max-time "$HTTP_TIMEOUT" \
    'https://www.okx.com/api/v5/public/instruments?instType=FUTURES' |
    jq -r '
            .data
            | map(
                select(
                    .state == "live"
                    and .ctType == "inverse"
                    and .instFamily == "BTC-USD"
                )
            )
            | sort_by(.expTime | tonumber)
            | .[0].instId // empty
        '
}

discover_okx_option() {
  local index_price
  local options

  index_price="$(
    curl \
      --fail \
      --silent \
      --show-error \
      --max-time "$HTTP_TIMEOUT" \
      'https://www.okx.com/api/v5/market/index-tickers?instId=BTC-USD' |
      jq -r '.data[0].idxPx // empty'
  )"

  if [[ -z "$index_price" ]]; then
    return 1
  fi

  options="$(
    curl \
      --fail \
      --silent \
      --show-error \
      --max-time "$HTTP_TIMEOUT" \
      'https://www.okx.com/api/v5/public/instruments?instType=OPTION&uly=BTC-USD'
  )"

  # Select:
  #
  #   1. live options
  #   2. nearest expiry
  #   3. strike nearest current BTC index
  #
  jq -r \
    --argjson index "$index_price" '
            .data
            | map(
                select(.state == "live")
            )
            | (
                map(.expTime | tonumber)
                | min
            ) as $nearest_expiry
            | map(
                select(
                    (.expTime | tonumber) == $nearest_expiry
                )
            )
            | sort_by(
                ((.stk | tonumber) - $index) | fabs
            )
            | .[0].instId // empty
        ' <<<"$options"
}

# ==============================================================================
# OKX
# ==============================================================================

capture_okx() {
  section "OKX"

  local ws="wss://ws.okx.com:8443/ws/v5/public"

  local linear_future=""
  local inverse_future=""
  local option=""

  # Discover only when required.

  if want_market linear_future; then
    linear_future="$(discover_okx_linear_future || true)"
  fi

  if want_market inverse_future; then
    inverse_future="$(discover_okx_inverse_future || true)"
  fi

  if want_market option; then
    option="$(discover_okx_option || true)"
  fi

  # --------------------------------------------------------------------------
  # Spot
  # --------------------------------------------------------------------------

  if want_market spot; then
    local dir="$ROOT/okx/spot"

    prepare_dir "$dir"

    write_meta "$dir" "okx" "spot" "BTC-USDT"

    capture_ws_subscription \
      "$ws" \
      '{"op":"subscribe","args":[{"channel":"trades","instId":"BTC-USDT"}]}' \
      "$dir/trades.raw.jsonl"

    capture_ws_subscription \
      "$ws" \
      '{"op":"subscribe","args":[{"channel":"books","instId":"BTC-USDT"}]}' \
      "$dir/books.raw.jsonl"

    capture_rest \
      "https://www.okx.com/api/v5/market/books?instId=BTC-USDT&sz=400" \
      "$dir/book.rest.snapshot.json"

    capture_rest \
      "https://www.okx.com/api/v5/market/trades?instId=BTC-USDT&limit=100" \
      "$dir/trades.rest.json"
  fi

  # --------------------------------------------------------------------------
  # Linear Swap
  # --------------------------------------------------------------------------

  if want_market linear_swap; then
    local dir="$ROOT/okx/linear_swap"

    prepare_dir "$dir"

    write_meta "$dir" "okx" "linear_swap" "BTC-USDT-SWAP"

    capture_ws_subscription \
      "$ws" \
      '{"op":"subscribe","args":[{"channel":"trades","instId":"BTC-USDT-SWAP"}]}' \
      "$dir/trades.raw.jsonl"

    capture_ws_subscription \
      "$ws" \
      '{"op":"subscribe","args":[{"channel":"books","instId":"BTC-USDT-SWAP"}]}' \
      "$dir/books.raw.jsonl"

    capture_rest \
      "https://www.okx.com/api/v5/market/books?instId=BTC-USDT-SWAP&sz=400" \
      "$dir/book.rest.snapshot.json"

    capture_rest \
      "https://www.okx.com/api/v5/market/trades?instId=BTC-USDT-SWAP&limit=100" \
      "$dir/trades.rest.json"
  fi

  # --------------------------------------------------------------------------
  # Linear Future
  # --------------------------------------------------------------------------

  if want_market linear_future; then
    if [[ -z "$linear_future" ]]; then
      warn "OKX: could not discover BTC linear future"
    else
      info "OKX linear future: $linear_future"

      local dir="$ROOT/okx/linear_future"

      prepare_dir "$dir"

      write_meta \
        "$dir" \
        "okx" \
        "linear_future" \
        "$linear_future"

      capture_ws_subscription \
        "$ws" \
        "{\"op\":\"subscribe\",\"args\":[{\"channel\":\"trades\",\"instId\":\"${linear_future}\"}]}" \
        "$dir/trades.raw.jsonl"

      capture_ws_subscription \
        "$ws" \
        "{\"op\":\"subscribe\",\"args\":[{\"channel\":\"books\",\"instId\":\"${linear_future}\"}]}" \
        "$dir/books.raw.jsonl"

      capture_rest \
        "https://www.okx.com/api/v5/market/books?instId=${linear_future}&sz=400" \
        "$dir/book.rest.snapshot.json"

      capture_rest \
        "https://www.okx.com/api/v5/market/trades?instId=${linear_future}&limit=100" \
        "$dir/trades.rest.json"
    fi
  fi

  # --------------------------------------------------------------------------
  # Inverse Future
  # --------------------------------------------------------------------------

  if want_market inverse_future; then
    if [[ -z "$inverse_future" ]]; then
      warn "OKX: could not discover BTC inverse future"
    else
      info "OKX inverse future: $inverse_future"

      local dir="$ROOT/okx/inverse_future"

      prepare_dir "$dir"

      write_meta \
        "$dir" \
        "okx" \
        "inverse_future" \
        "$inverse_future"

      capture_ws_subscription \
        "$ws" \
        "{\"op\":\"subscribe\",\"args\":[{\"channel\":\"trades\",\"instId\":\"${inverse_future}\"}]}" \
        "$dir/trades.raw.jsonl"

      capture_ws_subscription \
        "$ws" \
        "{\"op\":\"subscribe\",\"args\":[{\"channel\":\"books\",\"instId\":\"${inverse_future}\"}]}" \
        "$dir/books.raw.jsonl"

      capture_rest \
        "https://www.okx.com/api/v5/market/books?instId=${inverse_future}&sz=400" \
        "$dir/book.rest.snapshot.json"

      capture_rest \
        "https://www.okx.com/api/v5/market/trades?instId=${inverse_future}&limit=100" \
        "$dir/trades.rest.json"
    fi
  fi

  # --------------------------------------------------------------------------
  # Option
  # --------------------------------------------------------------------------

  if want_market option; then
    if [[ -z "$option" ]]; then
      warn "OKX: could not discover BTC option"
    else
      info "OKX option: $option"

      local dir="$ROOT/okx/option"

      prepare_dir "$dir"

      write_meta "$dir" "okx" "option" "$option"

      capture_ws_subscription \
        "$ws" \
        "{\"op\":\"subscribe\",\"args\":[{\"channel\":\"trades\",\"instId\":\"${option}\"}]}" \
        "$dir/trades.raw.jsonl"

      capture_ws_subscription \
        "$ws" \
        "{\"op\":\"subscribe\",\"args\":[{\"channel\":\"books\",\"instId\":\"${option}\"}]}" \
        "$dir/books.raw.jsonl"

      capture_rest \
        "https://www.okx.com/api/v5/market/books?instId=${option}&sz=400" \
        "$dir/book.rest.snapshot.json"

      capture_rest \
        "https://www.okx.com/api/v5/market/trades?instId=${option}&limit=100" \
        "$dir/trades.rest.json"
    fi
  fi
}

# ==============================================================================
# Gate.io discovery
# ==============================================================================

discover_gate_delivery() {
  curl \
    --fail \
    --silent \
    --show-error \
    --max-time "$HTTP_TIMEOUT" \
    'https://api.gateio.ws/api/v4/delivery/usdt/contracts' |
    jq -r '
            map(
                select(
                    .underlying == "BTC_USDT"
                    and .in_delisting == false
                )
            )
            | sort_by(.expire_time)
            | .[0].name // empty
        '
}

# ==============================================================================
# Gate.io subscription helper
# ==============================================================================

gate_subscription() {
  local channel="$1"
  shift

  local payload_json

  payload_json="$(
    printf '%s\n' "$@" |
      jq -R . |
      jq -s .
  )"

  jq -nc \
    --argjson time "$(date +%s)" \
    --arg channel "$channel" \
    --argjson payload "$payload_json" \
    '{
            time: $time,
            channel: $channel,
            event: "subscribe",
            payload: $payload
        }'
}

# ==============================================================================
# Gate.io
# ==============================================================================

capture_gateio() {
  section "GATE.IO"

  local delivery=""

  if want_market linear_delivery; then
    delivery="$(discover_gate_delivery || true)"
  fi

  # --------------------------------------------------------------------------
  # Spot
  # --------------------------------------------------------------------------

  if want_market spot; then
    local dir="$ROOT/gateio/spot"

    prepare_dir "$dir"

    write_meta "$dir" "gateio" "spot" "BTC_USDT"

    local trade_sub
    local book_sub

    trade_sub="$(
      gate_subscription \
        "spot.trades" \
        "BTC_USDT"
    )"

    book_sub="$(
      gate_subscription \
        "spot.order_book_update" \
        "BTC_USDT" \
        "100ms"
    )"

    capture_ws_subscription \
      "wss://api.gateio.ws/ws/v4/" \
      "$trade_sub" \
      "$dir/trades.raw.jsonl"

    capture_ws_subscription \
      "wss://api.gateio.ws/ws/v4/" \
      "$book_sub" \
      "$dir/books.raw.jsonl"

    capture_rest \
      "https://api.gateio.ws/api/v4/spot/order_book?currency_pair=BTC_USDT&limit=100&with_id=true" \
      "$dir/book.rest.snapshot.json"
  fi

  # --------------------------------------------------------------------------
  # Linear Perpetual
  # --------------------------------------------------------------------------

  if want_market linear_perpetual; then
    local dir="$ROOT/gateio/linear_perpetual"

    prepare_dir "$dir"

    write_meta \
      "$dir" \
      "gateio" \
      "linear_perpetual" \
      "BTC_USDT"

    local trade_sub
    local book_sub

    trade_sub="$(
      gate_subscription \
        "futures.trades" \
        "BTC_USDT"
    )"

    book_sub="$(
      gate_subscription \
        "futures.order_book_update" \
        "BTC_USDT" \
        "100ms"
    )"

    capture_ws_subscription \
      "wss://fx-ws.gateio.ws/v4/ws/usdt" \
      "$trade_sub" \
      "$dir/trades.raw.jsonl"

    capture_ws_subscription \
      "wss://fx-ws.gateio.ws/v4/ws/usdt" \
      "$book_sub" \
      "$dir/books.raw.jsonl"

    capture_rest \
      "https://api.gateio.ws/api/v4/futures/usdt/order_book?contract=BTC_USDT&limit=100&with_id=true" \
      "$dir/book.rest.snapshot.json"
  fi

  # --------------------------------------------------------------------------
  # Inverse Perpetual
  # --------------------------------------------------------------------------

  if want_market inverse_perpetual; then
    local dir="$ROOT/gateio/inverse_perpetual"

    prepare_dir "$dir"

    write_meta \
      "$dir" \
      "gateio" \
      "inverse_perpetual" \
      "BTC_USD"

    local trade_sub
    local book_sub

    trade_sub="$(
      gate_subscription \
        "futures.trades" \
        "BTC_USD"
    )"

    book_sub="$(
      gate_subscription \
        "futures.order_book_update" \
        "BTC_USD" \
        "100ms"
    )"

    capture_ws_subscription \
      "wss://fx-ws.gateio.ws/v4/ws/btc" \
      "$trade_sub" \
      "$dir/trades.raw.jsonl"

    capture_ws_subscription \
      "wss://fx-ws.gateio.ws/v4/ws/btc" \
      "$book_sub" \
      "$dir/books.raw.jsonl"

    capture_rest \
      "https://api.gateio.ws/api/v4/futures/btc/order_book?contract=BTC_USD&limit=100&with_id=true" \
      "$dir/book.rest.snapshot.json"
  fi

  # --------------------------------------------------------------------------
  # Linear Delivery
  # --------------------------------------------------------------------------

  if want_market linear_delivery; then
    if [[ -z "$delivery" ]]; then
      warn "Gate.io: could not discover active BTC_USDT delivery contract"
    else
      info "Gate.io delivery future: $delivery"

      local dir="$ROOT/gateio/linear_delivery"

      prepare_dir "$dir"

      write_meta \
        "$dir" \
        "gateio" \
        "linear_delivery" \
        "$delivery"

      local trade_sub
      local book_sub

      trade_sub="$(
        gate_subscription \
          "futures.trades" \
          "$delivery"
      )"

      book_sub="$(
        gate_subscription \
          "futures.order_book_update" \
          "$delivery" \
          "100ms"
      )"

      capture_ws_subscription \
        "wss://fx-ws.gateio.ws/v4/ws/delivery/usdt" \
        "$trade_sub" \
        "$dir/trades.raw.jsonl"

      capture_ws_subscription \
        "wss://fx-ws.gateio.ws/v4/ws/delivery/usdt" \
        "$book_sub" \
        "$dir/books.raw.jsonl"

      capture_rest \
        "https://api.gateio.ws/api/v4/delivery/usdt/order_book?contract=${delivery}&limit=100&with_id=true" \
        "$dir/book.rest.snapshot.json"
    fi
  fi
}

# ==============================================================================
# Validation
# ==============================================================================

validate_exchange() {
  local exchange="$1"
  local directory="$ROOT/$exchange"

  [[ -d "$directory" ]] || return 0

  section "VALIDATING $exchange"

  while IFS= read -r -d '' file; do
    case "$file" in
    *.jsonl)
      validate_jsonl "$file" || true
      ;;

    *.json)
      validate_json "$file" || true
      ;;
    esac
  done < <(
    find "$directory" \
      -type f \
      \( -name '*.json' -o -name '*.jsonl' \) \
      -print0
  )
}

# ==============================================================================
# Inventory
# ==============================================================================

print_inventory() {
  section "FIXTURE INVENTORY"

  if [[ ! -d "$ROOT" ]]; then
    warn "No fixture directory: $ROOT"
    return
  fi

  find "$ROOT" \
    -type f \
    -printf '%p  %s bytes\n' |
    sort
}

# ==============================================================================
# Argument validation
# ==============================================================================

case "$EXCHANGE" in
all | binance | bybit | bitget | okx | gateio)
  ;;
*)
  error "Unknown exchange: $EXCHANGE"
  echo
  usage
  exit 2
  ;;
esac

# ==============================================================================
# Capture
# ==============================================================================

if want_exchange binance; then
  capture_binance
fi

if want_exchange bybit; then
  capture_bybit
fi

if want_exchange bitget; then
  capture_bitget
fi

if want_exchange okx; then
  capture_okx
fi

if want_exchange gateio; then
  capture_gateio
fi

# ==============================================================================
# Validate
# ==============================================================================

if want_exchange binance; then
  validate_exchange binance
fi

if want_exchange bybit; then
  validate_exchange bybit
fi

if want_exchange bitget; then
  validate_exchange bitget
fi

if want_exchange okx; then
  validate_exchange okx
fi

if want_exchange gateio; then
  validate_exchange gateio
fi

# ==============================================================================
# Final inventory
# ==============================================================================

print_inventory

# ==============================================================================
# Summary
# ==============================================================================

section "DONE"

printf '%s\n' \
  "Raw live fixtures captured under:" \
  "  $ROOT"

printf '\n%s\n' \
  "Examples:"

printf '%s\n' \
  "  $0 binance" \
  "  $0 bybit option" \
  "  $0 bitget linear_usdt" \
  "  $0 okx linear_future" \
  "  $0 gateio linear_delivery"
