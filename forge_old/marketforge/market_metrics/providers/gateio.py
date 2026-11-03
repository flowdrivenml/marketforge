from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import httpx

from marketforge.market_metrics.models import InstrumentMetrics
from marketforge.market_metrics.providers import decimal_or_none, percentage_or_none

BASE_URL = "https://api.gateio.ws/api/v4"


def fetch(
    instruments: list[dict[str, Any]],
) -> list[InstrumentMetrics]:
    """Fetch and normalize current Gate.io market metrics."""

    measured_at = datetime.now(timezone.utc)

    index = _build_index(instruments)

    spot = _get_spot_tickers()

    usdt_futures = _get_futures_tickers("usdt")

    btc_futures = _get_futures_tickers("btc")

    usd1_futures = _get_futures_tickers("usd1")

    metrics: list[InstrumentMetrics] = []

    # -----------------------------------------------------------------------
    # Spot
    #
    # base_volume  -> base quantity
    # quote_volume -> quote turnover
    # -----------------------------------------------------------------------

    for row in spot:
        instrument = index.get(
            (
                row["currency_pair"],
                "spot",
            )
        )

        if instrument is None:
            continue

        metrics.append(
            InstrumentMetrics(
                instrument_id=instrument["instrument_id"],
                last_price=decimal_or_none(row.get("last")),
                high_24h=decimal_or_none(row.get("high_24h")),
                low_24h=decimal_or_none(row.get("low_24h")),
                price_change_pct_24h=(percentage_or_none(row.get("change_percentage"))),
                volume_24h_native=decimal_or_none(row.get("base_volume")),
                volume_24h_base=decimal_or_none(row.get("base_volume")),
                volume_24h_quote=decimal_or_none(row.get("quote_volume")),
                turnover_24h=decimal_or_none(row.get("quote_volume")),
                turnover_denomination=instrument["quote_asset"],
                measured_at=measured_at,
            )
        )

    # -----------------------------------------------------------------------
    # USDT contracts -> linear
    # -----------------------------------------------------------------------

    _append_contract_metrics(
        metrics=metrics,
        rows=usdt_futures,
        index=index,
        market_category="linear",
        measured_at=measured_at,
    )

    # -----------------------------------------------------------------------
    # BTC-settled contracts -> inverse
    # -----------------------------------------------------------------------

    _append_contract_metrics(
        metrics=metrics,
        rows=btc_futures,
        index=index,
        market_category="inverse",
        measured_at=measured_at,
    )

    # -----------------------------------------------------------------------
    # USD1 contracts -> linear
    # -----------------------------------------------------------------------

    _append_contract_metrics(
        metrics=metrics,
        rows=usd1_futures,
        index=index,
        market_category="linear",
        measured_at=measured_at,
    )

    return metrics


def _append_contract_metrics(
    *,
    metrics: list[InstrumentMetrics],
    rows: list[dict],
    index: dict[
        tuple[str, str],
        dict[str, Any],
    ],
    market_category: str,
    measured_at: datetime,
) -> None:
    for row in rows:
        instrument = index.get(
            (
                row["contract"],
                market_category,
            )
        )

        if instrument is None:
            continue

        metrics.append(
            InstrumentMetrics(
                instrument_id=instrument["instrument_id"],
                last_price=decimal_or_none(row.get("last")),
                high_24h=decimal_or_none(row.get("high_24h")),
                low_24h=decimal_or_none(row.get("low_24h")),
                price_change_pct_24h=(percentage_or_none(row.get("change_percentage"))),
                volume_24h_native=decimal_or_none(row.get("volume_24h")),
                volume_24h_base=decimal_or_none(row.get("volume_24h_base")),
                volume_24h_quote=decimal_or_none(row.get("volume_24h_quote")),
                volume_24h_contracts=decimal_or_none(row.get("volume_24h")),
                turnover_24h=decimal_or_none(row.get("volume_24h_quote")),
                turnover_denomination="USD",
                funding_rate=decimal_or_none(row.get("funding_rate")),
                measured_at=measured_at,
            )
        )


def _get_spot_tickers() -> list[dict]:
    response = httpx.get(
        f"{BASE_URL}/spot/tickers",
        timeout=30.0,
    )

    response.raise_for_status()

    return response.json()


def _get_futures_tickers(
    settle: str,
) -> list[dict]:
    response = httpx.get(
        f"{BASE_URL}/futures/{settle}/tickers",
        timeout=30.0,
    )

    response.raise_for_status()

    return response.json()


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
