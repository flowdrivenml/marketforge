from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from marketforge.catalog.models import Instrument, InstrumentSpec

from .client import OKXAPIError, get


@dataclass(frozen=True, slots=True)
class OptionInstrumentMetadata:
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
        raise ValueError(f"Invalid OKX option underlying: {underlying}") from exc

    return base_asset, quote_asset


def _parse_option_type(value: str) -> str:
    if value == "C":
        return "call"

    if value == "P":
        return "put"

    raise ValueError(f"Unsupported OKX option type: {value}")


def fetch_option_families() -> list[str]:
    data = get(
        "/api/v5/public/underlying",
        params={
            "instType": "OPTION",
        },
    )

    families = {family for group in data["data"] for family in group}

    return sorted(families)


def fetch_option_instruments() -> list[dict[str, Any]]:
    instruments: list[dict[str, Any]] = []

    for family in fetch_option_families():
        try:
            data = get(
                "/api/v5/public/instruments",
                params={
                    "instType": "OPTION",
                    "instFamily": family,
                },
            )

        except OKXAPIError as exc:
            if exc.code == "51000" and exc.message == "Parameter instFamily error":
                continue

            raise

        instruments.extend(data["data"])

    return instruments


def parse_option_instrument(
    raw: dict[str, Any],
    *,
    exchange_id: int,
    fetched_at: datetime,
) -> OptionInstrumentMetadata:
    base_asset, quote_asset = _parse_assets(raw)

    contract_value = Decimal(raw["ctVal"]) * Decimal(raw["ctMult"])

    instrument = Instrument(
        exchange_id=exchange_id,
        symbol=raw["instId"],
        instrument_type="option",
        market_category="option",
        base_asset=base_asset,
        quote_asset=quote_asset,
        settlement_asset=raw["settleCcy"] or None,
        launch_time_ns=_optional_ms_to_ns(raw["listTime"]),
        expiry_ns=_optional_ms_to_ns(raw["expTime"]),
        strike=Decimal(raw["stk"]),
        option_type=_parse_option_type(raw["optType"]),
        status=raw["state"],
    )

    spec = InstrumentSpec(
        instrument_id=None,
        quantity_type="contracts",
        contract_value=contract_value,
        contract_value_asset=raw["ctValCcy"] or None,
        tick_size=Decimal(raw["tickSz"]),
        qty_step=Decimal(raw["lotSz"]),
        min_qty=Decimal(raw["minSz"]),
        max_qty=None,
        min_notional=None,
        fetched_at=fetched_at,
    )

    return OptionInstrumentMetadata(
        instrument=instrument,
        spec=spec,
        raw=raw,
    )


def get_option_metadata(
    *,
    exchange_id: int,
) -> list[OptionInstrumentMetadata]:
    fetched_at = datetime.now(timezone.utc)

    return [
        parse_option_instrument(
            raw,
            exchange_id=exchange_id,
            fetched_at=fetched_at,
        )
        for raw in fetch_option_instruments()
    ]
