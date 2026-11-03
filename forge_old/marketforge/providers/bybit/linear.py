from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from marketforge.catalog.models import Instrument, InstrumentSpec

from .client import get


@dataclass(frozen=True, slots=True)
class LinearInstrumentMetadata:
    instrument: Instrument
    spec: InstrumentSpec
    raw: dict[str, Any]


def fetch_linear_instruments() -> list[dict[str, Any]]:
    instruments: list[dict[str, Any]] = []
    cursor: str | None = None

    while True:
        params: dict[str, Any] = {
            "category": "linear",
            "limit": 1000,
        }

        if cursor:
            params["cursor"] = cursor

        data = get(
            "/v5/market/instruments-info",
            params=params,
        )

        result = data["result"]

        instruments.extend(result["list"])

        cursor = result.get("nextPageCursor")

        if not cursor:
            break

    return instruments


def parse_linear_instrument(
    raw: dict[str, Any],
    *,
    exchange_id: int,
    fetched_at: datetime,
) -> LinearInstrumentMetadata:
    price_filter = raw["priceFilter"]
    lot_size = raw["lotSizeFilter"]

    contract_type = raw["contractType"]

    if contract_type == "LinearPerpetual":
        instrument_type = "perpetual"
        expiry_ns = None
    else:
        instrument_type = "future"

        delivery_time = int(raw["deliveryTime"])

        expiry_ns = delivery_time * 1_000_000 if delivery_time else None

    launch_time = int(raw["launchTime"])

    instrument = Instrument(
        exchange_id=exchange_id,
        symbol=raw["symbol"],
        instrument_type=instrument_type,
        market_category="linear",
        base_asset=raw["baseCoin"],
        quote_asset=raw["quoteCoin"],
        settlement_asset=raw["settleCoin"],
        launch_time_ns=launch_time * 1_000_000,
        expiry_ns=expiry_ns,
        strike=None,
        option_type=None,
        status=raw["status"],
    )

    spec = InstrumentSpec(
        instrument_id=None,
        quantity_type="base",
        contract_value=None,
        contract_value_asset=None,
        tick_size=Decimal(price_filter["tickSize"]),
        qty_step=Decimal(lot_size["qtyStep"]),
        min_qty=Decimal(lot_size["minOrderQty"]),
        max_qty=Decimal(lot_size["maxOrderQty"]),
        min_notional=Decimal(lot_size["minNotionalValue"]),
        fetched_at=fetched_at,
    )

    return LinearInstrumentMetadata(
        instrument=instrument,
        spec=spec,
        raw=raw,
    )


def get_linear_metadata(
    *,
    exchange_id: int,
) -> list[LinearInstrumentMetadata]:
    fetched_at = datetime.now(timezone.utc)

    return [
        parse_linear_instrument(
            raw,
            exchange_id=exchange_id,
            fetched_at=fetched_at,
        )
        for raw in fetch_linear_instruments()
    ]
