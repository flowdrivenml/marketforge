from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from marketforge.catalog.models import Instrument, InstrumentSpec

from .client import get

SETTLEMENT_ASSET = "USDT"
LINEAR_SETTLEMENTS = (
    "usdt",
    "usd1",
)


@dataclass(frozen=True, slots=True)
class LinearInstrumentMetadata:
    instrument: Instrument
    spec: InstrumentSpec
    raw: dict[str, Any]


def _optional_seconds_to_ns(
    value: int | None,
) -> int | None:
    if value is None or value == 0:
        return None

    return int(value) * 1_000_000_000


def _parse_assets(
    name: str,
) -> tuple[str, str]:
    try:
        base_asset, quote_asset = name.rsplit("_", 1)
    except ValueError as exc:
        raise ValueError(f"Invalid Gate.io futures contract name: {name}") from exc

    return base_asset, quote_asset


def fetch_linear_instruments() -> list[dict[str, Any]]:
    instruments: list[dict[str, Any]] = []

    for settlement in LINEAR_SETTLEMENTS:
        data = get(
            f"/api/v4/futures/{settlement}/contracts",
        )

        for raw in data:
            raw = dict(raw)
            raw["_marketforge_settlement"] = settlement.upper()
            instruments.append(raw)

    return instruments


def parse_linear_instrument(
    raw: dict[str, Any],
    *,
    exchange_id: int,
    fetched_at: datetime,
) -> LinearInstrumentMetadata:
    if raw["type"] != "direct":
        raise ValueError("Unsupported Gate.io linear contract type: " f"{raw['type']}")

    base_asset, quote_asset = _parse_assets(raw["name"])

    instrument = Instrument(
        exchange_id=exchange_id,
        symbol=raw["name"],
        instrument_type="perpetual",
        market_category="linear",
        base_asset=base_asset,
        quote_asset=quote_asset,
        settlement_asset=quote_asset,
        launch_time_ns=_optional_seconds_to_ns(raw.get("launch_time")),
        expiry_ns=None,
        strike=None,
        option_type=None,
        status=raw["status"],
    )

    spec = InstrumentSpec(
        instrument_id=None,
        quantity_type="contracts",
        contract_value=Decimal(raw["quanto_multiplier"]),
        contract_value_asset=base_asset,
        tick_size=Decimal(raw["order_price_round"]),
        qty_step=Decimal("1"),
        min_qty=Decimal(str(raw["order_size_min"])),
        max_qty=Decimal(str(raw["order_size_max"])),
        min_notional=None,
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
