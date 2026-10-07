from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import httpx

from marketforge.market_metrics.models import InstrumentMetrics
from marketforge.market_metrics.providers import decimal_or_none, percentage_or_none

SPOT_URL = "https://api.binance.com/api/v3/ticker/24hr"
LINEAR_URL = "https://fapi.binance.com/fapi/v1/ticker/24hr"
INVERSE_URL = "https://dapi.binance.com/dapi/v1/ticker/24hr"


def fetch(
    instruments: list[dict[str, Any]],
) -> list[InstrumentMetrics]:
    """Fetch current Binance market metrics."""

    measured_at = datetime.now(timezone.utc)

    index = _build_index(instruments)

    spot = _get(SPOT_URL)
    linear = _get(LINEAR_URL)
    inverse = _get(INVERSE_URL)

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
                high_24h=decimal_or_none(row.get("highPrice")),
                low_24h=decimal_or_none(row.get("lowPrice")),
                price_change_pct_24h=percentage_or_none(row.get("priceChangePercent")),
                volume_24h_native=decimal_or_none(row.get("volume")),
                volume_24h_base=decimal_or_none(row.get("volume")),
                volume_24h_quote=decimal_or_none(row.get("quoteVolume")),
                turnover_24h=decimal_or_none(row.get("quoteVolume")),
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
                high_24h=decimal_or_none(row.get("highPrice")),
                low_24h=decimal_or_none(row.get("lowPrice")),
                price_change_pct_24h=percentage_or_none(row.get("priceChangePercent")),
                volume_24h_native=decimal_or_none(row.get("volume")),
                volume_24h_base=decimal_or_none(row.get("volume")),
                volume_24h_quote=decimal_or_none(row.get("quoteVolume")),
                turnover_24h=decimal_or_none(row.get("quoteVolume")),
                turnover_denomination="USD",
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

        contracts = decimal_or_none(row.get("volume"))

        base_volume = decimal_or_none(row.get("baseVolume"))

        contract_value = decimal_or_none(instrument.get("contract_value"))

        contract_value_asset = instrument.get("contract_value_asset")

        turnover = None

        if (
            contracts is not None
            and contract_value is not None
            and contract_value_asset == "USD"
        ):
            turnover = contracts * contract_value

        metrics.append(
            InstrumentMetrics(
                instrument_id=instrument["instrument_id"],
                last_price=decimal_or_none(row.get("lastPrice")),
                high_24h=decimal_or_none(row.get("highPrice")),
                low_24h=decimal_or_none(row.get("lowPrice")),
                price_change_pct_24h=percentage_or_none(row.get("priceChangePercent")),
                volume_24h_native=contracts,
                volume_24h_base=base_volume,
                volume_24h_quote=turnover,
                volume_24h_contracts=contracts,
                turnover_24h=turnover,
                turnover_denomination=("USD" if turnover is not None else None),
                measured_at=measured_at,
            )
        )

    return metrics


def _get(
    url: str,
) -> list[dict]:
    response = httpx.get(
        url,
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
