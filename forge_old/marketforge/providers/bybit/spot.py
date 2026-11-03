from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from marketforge.catalog.models import Instrument, InstrumentSpec

from .client import get


@dataclass(frozen=True, slots=True)
class SpotInstrumentMetadata:
    instrument: Instrument
    spec: InstrumentSpec
    raw: dict[str, Any]


def fetch_spot_instruments() -> list[dict[str, Any]]:
    data = get(
        "/v5/market/instruments-info",
        params={
            "category": "spot",
        },
    )

    return data["result"]["list"]


def parse_spot_instrument(
    raw: dict[str, Any],
    *,
    exchange_id: int,
    fetched_at: datetime,
) -> SpotInstrumentMetadata:
    price_filter = raw["priceFilter"]
    lot_size = raw["lotSizeFilter"]

    instrument = Instrument(
        exchange_id=exchange_id,
        symbol=raw["symbol"],
        instrument_type="spot",
        market_category="spot",
        base_asset=raw["baseCoin"],
        quote_asset=raw["quoteCoin"],
        settlement_asset=None,
        launch_time_ns=None,
        expiry_ns=None,
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
        qty_step=Decimal(lot_size["basePrecision"]),
        min_qty=Decimal(lot_size["minOrderQty"]),
        max_qty=Decimal(lot_size["maxOrderQty"]),
        min_notional=Decimal(lot_size["minOrderAmt"]),
        fetched_at=fetched_at,
    )

    return SpotInstrumentMetadata(
        instrument=instrument,
        spec=spec,
        raw=raw,
    )


def get_spot_metadata(
    *,
    exchange_id: int,
) -> list[SpotInstrumentMetadata]:
    fetched_at = datetime.now(timezone.utc)

    raw_instruments = fetch_spot_instruments()

    return [
        parse_spot_instrument(
            raw,
            exchange_id=exchange_id,
            fetched_at=fetched_at,
        )
        for raw in raw_instruments
    ]
