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


def _precision_to_step(value: int) -> Decimal:
    if value < 0:
        raise ValueError(f"Invalid Gate.io precision: {value}")

    return Decimal(1).scaleb(-value)


def fetch_spot_instruments() -> list[dict[str, Any]]:
    return get(
        "/api/v4/spot/currency_pairs",
    )


def _optional_decimal(
    value: str | None,
) -> Decimal | None:
    if value is None or value == "":
        return None

    return Decimal(value)


def parse_spot_instrument(
    raw: dict[str, Any],
    *,
    exchange_id: int,
    fetched_at: datetime,
) -> SpotInstrumentMetadata:
    instrument = Instrument(
        exchange_id=exchange_id,
        symbol=raw["id"],
        instrument_type="spot",
        market_category="spot",
        base_asset=raw["base"],
        quote_asset=raw["quote"],
        settlement_asset=None,
        launch_time_ns=None,
        expiry_ns=None,
        strike=None,
        option_type=None,
        status=raw["trade_status"],
    )

    spec = InstrumentSpec(
        instrument_id=None,
        quantity_type="base",
        contract_value=None,
        contract_value_asset=None,
        tick_size=_precision_to_step(raw["precision"]),
        qty_step=_precision_to_step(raw["amount_precision"]),
        min_qty=_optional_decimal(raw.get("min_base_amount")),
        max_qty=_optional_decimal(raw.get("max_base_amount")),
        min_notional=_optional_decimal(raw.get("min_quote_amount")),
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
