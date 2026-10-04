from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from marketforge.catalog.models import Instrument, InstrumentSpec

from .client import get


@dataclass(frozen=True, slots=True)
class InverseInstrumentMetadata:
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
        raise ValueError(f"Invalid Gate.io inverse contract name: {name}") from exc

    return base_asset, quote_asset


def fetch_inverse_instruments() -> list[dict[str, Any]]:
    return get(
        "/api/v4/futures/btc/contracts",
    )


def parse_inverse_instrument(
    raw: dict[str, Any],
    *,
    exchange_id: int,
    fetched_at: datetime,
) -> InverseInstrumentMetadata:
    if raw["type"] != "inverse":
        raise ValueError("Unsupported Gate.io inverse contract type: " f"{raw['type']}")

    base_asset, quote_asset = _parse_assets(raw["name"])

    instrument = Instrument(
        exchange_id=exchange_id,
        symbol=raw["name"],
        instrument_type="perpetual",
        market_category="inverse",
        base_asset=base_asset,
        quote_asset=quote_asset,
        settlement_asset=base_asset,
        launch_time_ns=_optional_seconds_to_ns(raw.get("launch_time")),
        expiry_ns=None,
        strike=None,
        option_type=None,
        status=raw["status"],
    )

    spec = InstrumentSpec(
        instrument_id=None,
        quantity_type="contracts",
        contract_value=Decimal("1"),
        contract_value_asset=quote_asset,
        tick_size=Decimal(raw["order_price_round"]),
        qty_step=Decimal("1"),
        min_qty=Decimal(str(raw["order_size_min"])),
        max_qty=Decimal(str(raw["order_size_max"])),
        min_notional=None,
        fetched_at=fetched_at,
    )

    return InverseInstrumentMetadata(
        instrument=instrument,
        spec=spec,
        raw=raw,
    )


def get_inverse_metadata(
    *,
    exchange_id: int,
) -> list[InverseInstrumentMetadata]:
    fetched_at = datetime.now(timezone.utc)

    return [
        parse_inverse_instrument(
            raw,
            exchange_id=exchange_id,
            fetched_at=fetched_at,
        )
        for raw in fetch_inverse_instruments()
    ]
