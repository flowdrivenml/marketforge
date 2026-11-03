from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import httpx

from marketforge.market_metrics.models import InstrumentMetrics
from marketforge.market_metrics.providers import decimal_or_none

BASE_URL = "https://www.okx.com"


def fetch(
    instruments: list[dict[str, Any]],
) -> list[InstrumentMetrics]:
    """Fetch current OKX market metrics."""

    measured_at = datetime.now(timezone.utc)

    index = _build_index(instruments)

    spot = _get_tickers("SPOT")
    swaps = _get_tickers("SWAP")
    futures = _get_tickers("FUTURES")

    metrics: list[InstrumentMetrics] = []

    for row in spot:
        instrument = index.get(row["instId"])

        if instrument is None:
            continue

        if instrument["instrument_type"] != "spot":
            continue

        metrics.append(
            InstrumentMetrics(
                instrument_id=instrument["instrument_id"],
                last_price=decimal_or_none(row.get("last")),
                high_24h=decimal_or_none(row.get("high24h")),
                low_24h=decimal_or_none(row.get("low24h")),
                volume_24h_native=decimal_or_none(row.get("vol24h")),
                volume_24h_base=decimal_or_none(row.get("vol24h")),
                volume_24h_quote=decimal_or_none(row.get("volCcy24h")),
                turnover_24h=decimal_or_none(row.get("volCcy24h")),
                turnover_denomination=instrument["quote_asset"],
                measured_at=measured_at,
            )
        )

    for row in swaps + futures:
        instrument = index.get(row["instId"])

        if instrument is None:
            continue

        contracts = decimal_or_none(row.get("vol24h"))

        api_volume_currency = decimal_or_none(row.get("volCcy24h"))

        last_price = decimal_or_none(row.get("last"))

        contract_value = decimal_or_none(instrument.get("contract_value"))

        contract_value_asset = instrument.get("contract_value_asset")

        turnover = None
        base_volume = None

        if contracts is not None and contract_value is not None:
            if contract_value_asset == "USD":
                # Inverse contract:
                # contracts × USD value per contract.
                turnover = contracts * contract_value

                base_volume = api_volume_currency

            elif contract_value_asset == instrument["base_asset"]:
                # Linear contract:
                # contracts × base units per contract.
                base_volume = contracts * contract_value

                if last_price is not None:
                    turnover = base_volume * last_price

        metrics.append(
            InstrumentMetrics(
                instrument_id=instrument["instrument_id"],
                last_price=last_price,
                high_24h=decimal_or_none(row.get("high24h")),
                low_24h=decimal_or_none(row.get("low24h")),
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


def _get_tickers(
    inst_type: str,
) -> list[dict]:
    response = httpx.get(
        f"{BASE_URL}/api/v5/market/tickers",
        params={
            "instType": inst_type,
        },
        timeout=30.0,
    )

    response.raise_for_status()

    payload = response.json()

    if payload["code"] != "0":
        raise RuntimeError(f"OKX: {payload['msg']}")

    return payload["data"]


def _build_index(
    instruments: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    return {instrument["symbol"]: instrument for instrument in instruments}
