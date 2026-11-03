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


def _get_filter(
    raw: dict[str, Any],
    filter_type: str,
) -> dict[str, Any] | None:
    for item in raw["filters"]:
        if item["filterType"] == filter_type:
            return item

    return None


def _decimal_or_none(
    value: str | None,
) -> Decimal | None:
    if value is None or value == "":
        return None

    return Decimal(value)


def fetch_spot_instruments() -> list[dict[str, Any]]:
    data = get("/api/v3/exchangeInfo")

    return data["symbols"]


def parse_spot_instrument(
    raw: dict[str, Any],
    *,
    exchange_id: int,
    fetched_at: datetime,
) -> SpotInstrumentMetadata:
    price_filter = _get_filter(
        raw,
        "PRICE_FILTER",
    )

    lot_size = _get_filter(
        raw,
        "LOT_SIZE",
    )

    notional = _get_filter(raw, "NOTIONAL") or _get_filter(raw, "MIN_NOTIONAL")

    if price_filter is None:
        raise ValueError(f"Missing PRICE_FILTER for Binance {raw['symbol']}")

    if lot_size is None:
        raise ValueError(f"Missing LOT_SIZE for Binance {raw['symbol']}")

    instrument = Instrument(
        exchange_id=exchange_id,
        symbol=raw["symbol"],
        instrument_type="spot",
        market_category="spot",
        base_asset=raw["baseAsset"],
        quote_asset=raw["quoteAsset"],
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
        qty_step=Decimal(lot_size["stepSize"]),
        min_qty=Decimal(lot_size["minQty"]),
        max_qty=Decimal(lot_size["maxQty"]),
        min_notional=(
            _decimal_or_none(notional.get("minNotional")) if notional else None
        ),
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
