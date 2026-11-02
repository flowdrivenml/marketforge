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


def _optional_ms_to_ns(value: str) -> int | None:
    if not value:
        return None

    return int(value) * 1_000_000


def fetch_spot_instruments() -> list[dict[str, Any]]:
    data = get(
        "/api/v5/public/instruments",
        params={
            "instType": "SPOT",
        },
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
        symbol=raw["instId"],
        instrument_type="spot",
        market_category="spot",
        base_asset=raw["baseCcy"],
        quote_asset=raw["quoteCcy"],
        settlement_asset=raw["settleCcy"] or None,
        launch_time_ns=_optional_ms_to_ns(raw["listTime"]),
        expiry_ns=_optional_ms_to_ns(raw["expTime"]),
        strike=None,
        option_type=None,
        status=raw["state"],
    )

    spec = InstrumentSpec(
        instrument_id=None,
        quantity_type="base",
        contract_value=None,
        contract_value_asset=None,
        tick_size=Decimal(raw["tickSz"]),
        qty_step=Decimal(raw["lotSz"]),
        min_qty=Decimal(raw["minSz"]),
        max_qty=None,
        min_notional=None,
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
