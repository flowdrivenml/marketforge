from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import httpx

from marketforge.market_metrics.models import InstrumentMetrics
from marketforge.market_metrics.providers import decimal_or_none

BASE_URL = "https://api.bitget.com"


def fetch(
    instruments: list[dict[str, Any]],
) -> list[InstrumentMetrics]:
    """Fetch and normalize current Bitget market metrics."""

    measured_at = datetime.now(timezone.utc)

    index = _build_index(instruments)

    spot = _get_tickers("SPOT")
    usdt_futures = _get_tickers("USDT-FUTURES")
    usdc_futures = _get_tickers("USDC-FUTURES")
    coin_futures = _get_tickers("COIN-FUTURES")

    metrics: list[InstrumentMetrics] = []

    # -----------------------------------------------------------------------
    # Spot
    #
    # volume24h   -> base quantity
    # turnover24h -> quote turnover
    # -----------------------------------------------------------------------

    for row in spot:
        instrument = index.get(
            (
                row["symbol"],
                "spot",
            )
        )

        if instrument is None:
            continue

        metrics.append(
            InstrumentMetrics(
                instrument_id=instrument["instrument_id"],
                last_price=decimal_or_none(row.get("lastPrice")),
                high_24h=decimal_or_none(row.get("highPrice24h")),
                low_24h=decimal_or_none(row.get("lowPrice24h")),
                price_change_pct_24h=decimal_or_none(row.get("price24hPcnt")),
                volume_24h_native=decimal_or_none(row.get("volume24h")),
                volume_24h_base=decimal_or_none(row.get("volume24h")),
                volume_24h_quote=decimal_or_none(row.get("turnover24h")),
                turnover_24h=decimal_or_none(row.get("turnover24h")),
                turnover_denomination=instrument["quote_asset"],
                measured_at=measured_at,
            )
        )

    # -----------------------------------------------------------------------
    # USDT + USDC linear contracts
    #
    # volume24h   -> base quantity
    # turnover24h -> USD-like quote turnover
    # -----------------------------------------------------------------------

    for rows in (
        usdt_futures,
        usdc_futures,
    ):
        for row in rows:
            instrument = index.get(
                (
                    row["symbol"],
                    "linear",
                )
            )

            if instrument is None:
                continue

            metrics.append(
                InstrumentMetrics(
                    instrument_id=instrument["instrument_id"],
                    last_price=decimal_or_none(row.get("lastPrice")),
                    high_24h=decimal_or_none(row.get("highPrice24h")),
                    low_24h=decimal_or_none(row.get("lowPrice24h")),
                    price_change_pct_24h=(decimal_or_none(row.get("price24hPcnt"))),
                    volume_24h_native=(decimal_or_none(row.get("volume24h"))),
                    volume_24h_base=(decimal_or_none(row.get("volume24h"))),
                    volume_24h_quote=(decimal_or_none(row.get("turnover24h"))),
                    turnover_24h=(decimal_or_none(row.get("turnover24h"))),
                    turnover_denomination="USD",
                    open_interest=decimal_or_none(row.get("openInterest")),
                    funding_rate=decimal_or_none(row.get("fundingRate")),
                    measured_at=measured_at,
                )
            )

    # -----------------------------------------------------------------------
    # COIN / inverse contracts
    #
    # volume24h   -> base quantity
    # turnover24h -> USD notional
    # -----------------------------------------------------------------------

    for row in coin_futures:
        instrument = index.get(
            (
                row["symbol"],
                "inverse",
            )
        )

        if instrument is None:
            continue

        metrics.append(
            InstrumentMetrics(
                instrument_id=instrument["instrument_id"],
                last_price=decimal_or_none(row.get("lastPrice")),
                high_24h=decimal_or_none(row.get("highPrice24h")),
                low_24h=decimal_or_none(row.get("lowPrice24h")),
                price_change_pct_24h=decimal_or_none(row.get("price24hPcnt")),
                volume_24h_native=decimal_or_none(row.get("volume24h")),
                volume_24h_base=decimal_or_none(row.get("volume24h")),
                volume_24h_quote=decimal_or_none(row.get("turnover24h")),
                turnover_24h=decimal_or_none(row.get("turnover24h")),
                turnover_denomination="USD",
                open_interest=decimal_or_none(row.get("openInterest")),
                funding_rate=decimal_or_none(row.get("fundingRate")),
                measured_at=measured_at,
            )
        )

    return metrics


def _get_tickers(
    category: str,
) -> list[dict]:
    response = httpx.get(
        f"{BASE_URL}/api/v3/market/tickers",
        params={
            "category": category,
        },
        timeout=30.0,
    )

    response.raise_for_status()

    payload = response.json()

    if payload["code"] != "00000":
        raise RuntimeError(f"Bitget: {payload['msg']}")

    return payload["data"]


def _build_index(
    instruments: list[dict[str, Any]],
) -> dict[tuple[str, str], dict[str, Any]]:
    return {
        (
            instrument["symbol"],
            instrument["market_category"],
        ): instrument
        for instrument in instruments
    }
