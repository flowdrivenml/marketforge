from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from marketforge.catalog.models import Instrument, InstrumentSpec

from .client import get


@dataclass(frozen=True, slots=True)
class FuturesInstrumentMetadata:
    instrument: Instrument
    spec: InstrumentSpec
    raw: dict[str, Any]


def _optional_ms_to_ns(value: str) -> int | None:
    if not value:
        return None

    return int(value) * 1_000_000


def _parse_assets(raw: dict[str, Any]) -> tuple[str, str]:
    underlying = raw["uly"]

    try:
        base_asset, quote_asset = underlying.rsplit("-", 1)
    except ValueError as exc:
        raise ValueError(f"Invalid OKX underlying: {underlying}") from exc

    return base_asset, quote_asset


def _parse_market_category(ct_type: str) -> str:
    if ct_type == "linear":
        return "linear"

    if ct_type == "inverse":
        return "inverse"

    raise ValueError(f"Unsupported OKX contract type: {ct_type}")


def fetch_futures_instruments() -> list[dict[str, Any]]:
    data = get(
        "/api/v5/public/instruments",
        params={
            "instType": "FUTURES",
        },
    )

    return data["data"]


def parse_futures_instrument(
    raw: dict[str, Any],
    *,
    exchange_id: int,
    fetched_at: datetime,
) -> FuturesInstrumentMetadata:
    base_asset, quote_asset = _parse_assets(raw)

    contract_value = Decimal(raw["ctVal"])
    contract_multiplier = Decimal(raw["ctMult"])

    effective_contract_value = contract_value * contract_multiplier

    instrument = Instrument(
        exchange_id=exchange_id,
        symbol=raw["instId"],
        instrument_type="future",
        market_category=_parse_market_category(raw["ctType"]),
        base_asset=base_asset,
        quote_asset=quote_asset,
        settlement_asset=raw["settleCcy"] or None,
        launch_time_ns=_optional_ms_to_ns(raw["listTime"]),
        expiry_ns=_optional_ms_to_ns(raw["expTime"]),
        strike=None,
        option_type=None,
        status=raw["state"],
    )

    spec = InstrumentSpec(
        instrument_id=None,
        quantity_type="contracts",
        contract_value=effective_contract_value,
        contract_value_asset=raw["ctValCcy"] or None,
        tick_size=Decimal(raw["tickSz"]),
        qty_step=Decimal(raw["lotSz"]),
        min_qty=Decimal(raw["minSz"]),
        max_qty=None,
        min_notional=None,
        fetched_at=fetched_at,
    )

    return FuturesInstrumentMetadata(
        instrument=instrument,
        spec=spec,
        raw=raw,
    )


def get_futures_metadata(
    *,
    exchange_id: int,
) -> list[FuturesInstrumentMetadata]:
    fetched_at = datetime.now(timezone.utc)

    return [
        parse_futures_instrument(
            raw,
            exchange_id=exchange_id,
            fetched_at=fetched_at,
        )
        for raw in fetch_futures_instruments()
    ]
