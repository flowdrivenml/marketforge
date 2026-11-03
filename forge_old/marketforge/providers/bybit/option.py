from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from marketforge.catalog.models import Instrument, InstrumentSpec

from .client import get


@dataclass(frozen=True, slots=True)
class OptionInstrumentMetadata:
    instrument: Instrument
    spec: InstrumentSpec
    raw: dict[str, Any]


def fetch_option_instruments() -> list[dict[str, Any]]:
    instruments: list[dict[str, Any]] = []
    cursor: str | None = None

    while True:
        params: dict[str, Any] = {
            "category": "option",
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


def parse_option_strike(symbol: str) -> Decimal:
    """
    Parse the strike from a Bybit option symbol.

    Example:
        BTC-25JUN27-106000-P-USDT
                    ↓
                  106000
    """
    parts = symbol.split("-")

    if len(parts) < 5:
        raise ValueError(f"Invalid Bybit option symbol: {symbol}")

    return Decimal(parts[-3])


def parse_option_type(value: str) -> str:
    if value == "Call":
        return "call"

    if value == "Put":
        return "put"

    raise ValueError(f"Unsupported Bybit option type: {value}")


def parse_option_instrument(
    raw: dict[str, Any],
    *,
    exchange_id: int,
    fetched_at: datetime,
) -> OptionInstrumentMetadata:
    price_filter = raw["priceFilter"]
    lot_size = raw["lotSizeFilter"]

    launch_time = int(raw["launchTime"])
    delivery_time = int(raw["deliveryTime"])

    instrument = Instrument(
        exchange_id=exchange_id,
        symbol=raw["symbol"],
        instrument_type="option",
        market_category="option",
        base_asset=raw["baseCoin"],
        quote_asset=raw["quoteCoin"],
        settlement_asset=raw["settleCoin"],
        launch_time_ns=launch_time * 1_000_000,
        expiry_ns=delivery_time * 1_000_000,
        strike=parse_option_strike(raw["symbol"]),
        option_type=parse_option_type(raw["optionsType"]),
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
