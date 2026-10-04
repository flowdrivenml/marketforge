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


def _precision_to_step(value: str) -> Decimal:
    precision = int(value)

    if precision < 0:
        raise ValueError(f"Invalid Bitget precision: {precision}")

    return Decimal(1).scaleb(-precision)


def _optional_ms_to_ns(value: str) -> int | None:
    if not value:
        return None

    return int(value) * 1_000_000


def fetch_spot_instruments() -> list[dict[str, Any]]:
    data = get(
        "/api/v2/spot/public/symbols",
    )

    return data["data"]


def parse_spot_instrument(
    raw: dict[str, Any],
    *,
    exchange_id: int,
    fetched_at: datetime,
) -> SpotInstrumentMetadata:
    instrument = Instrument(
        exchange_id=exchange_id,
        symbol=raw["symbol"],
        instrument_type="spot",
        market_category="spot",
        base_asset=raw["baseCoin"],
        quote_asset=raw["quoteCoin"],
        settlement_asset=None,
        launch_time_ns=_optional_ms_to_ns(raw["openTime"]),
        expiry_ns=_optional_ms_to_ns(raw["offTime"]),
        strike=None,
        option_type=None,
        status=raw["status"],
    )

    spec = InstrumentSpec(
        instrument_id=None,
        quantity_type="base",
        contract_value=None,
        contract_value_asset=None,
        tick_size=_precision_to_step(raw["pricePrecision"]),
        qty_step=_precision_to_step(raw["quantityPrecision"]),
        min_qty=Decimal(raw["minTradeAmount"]),
        max_qty=Decimal(raw["maxTradeAmount"]),
        min_notional=Decimal(raw["minTradeUSDT"]),
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

    return [
        parse_spot_instrument(
            raw,
            exchange_id=exchange_id,
            fetched_at=fetched_at,
        )
        for raw in fetch_spot_instruments()
    ]
