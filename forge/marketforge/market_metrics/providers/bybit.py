from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import httpx

from marketforge.market_metrics.models import InstrumentMetrics
from marketforge.market_metrics.providers import decimal_or_none

BASE_URL = "https://api.bybit.com"


def fetch(
    instruments: list[dict[str, Any]],
) -> list[InstrumentMetrics]:
    """Fetch current Bybit market metrics."""

    measured_at = datetime.now(timezone.utc)

    index = _build_index(instruments)

    spot = _get_tickers("spot")
    linear = _get_tickers("linear")
    inverse = _get_tickers("inverse")

    metrics: list[InstrumentMetrics] = []

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

    for row in linear:
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
                price_change_pct_24h=decimal_or_none(row.get("price24hPcnt")),
                volume_24h_native=decimal_or_none(row.get("volume24h")),
                volume_24h_base=decimal_or_none(row.get("volume24h")),
                volume_24h_quote=decimal_or_none(row.get("turnover24h")),
                turnover_24h=decimal_or_none(row.get("turnover24h")),
                turnover_denomination="USD",
                open_interest=decimal_or_none(row.get("openInterest")),
                open_interest_value=decimal_or_none(row.get("openInterestValue")),
                funding_rate=decimal_or_none(row.get("fundingRate")),
                measured_at=measured_at,
            )
        )

    for row in inverse:
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
                volume_24h_base=decimal_or_none(row.get("turnover24h")),
                volume_24h_quote=decimal_or_none(row.get("volume24h")),
                turnover_24h=decimal_or_none(row.get("volume24h")),
                turnover_denomination="USD",
                open_interest=decimal_or_none(row.get("openInterest")),
                open_interest_value=decimal_or_none(row.get("openInterestValue")),
                funding_rate=decimal_or_none(row.get("fundingRate")),
                measured_at=measured_at,
            )
        )

    return metrics


def _get_tickers(
    category: str,
) -> list[dict]:
    response = httpx.get(
        f"{BASE_URL}/v5/market/tickers",
        params={
            "category": category,
        },
        timeout=30.0,
    )

    response.raise_for_status()

    payload = response.json()

    if payload["retCode"] != 0:
        raise RuntimeError(f"Bybit: {payload['retMsg']}")

    return payload["result"]["list"]


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
