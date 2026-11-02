from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from marketforge.catalog.models import Instrument, InstrumentSpec

from .client import LINEAR_BASE_URL, get

PERPETUAL_CONTRACT_TYPES = {
    "PERPETUAL",
    "TRADIFI_PERPETUAL",
}

FUTURE_CONTRACT_TYPES = {
    "CURRENT_QUARTER",
    "NEXT_QUARTER",
}

SUPPORTED_CONTRACT_TYPES = PERPETUAL_CONTRACT_TYPES | FUTURE_CONTRACT_TYPES


@dataclass(frozen=True, slots=True)
class LinearInstrumentMetadata:
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


def _parse_instrument_type(
    raw: dict[str, Any],
) -> tuple[str, int | None]:
    contract_type = raw["contractType"]

    if contract_type in PERPETUAL_CONTRACT_TYPES:
        return "perpetual", None

    if contract_type in FUTURE_CONTRACT_TYPES:
        delivery_date = int(raw["deliveryDate"])

        return (
            "future",
            delivery_date * 1_000_000,
        )

    raise ValueError("Unsupported Binance linear contract type: " f"{contract_type}")


def fetch_linear_instruments() -> list[dict[str, Any]]:
    data = get(
        "/fapi/v1/exchangeInfo",
        base_url=LINEAR_BASE_URL,
    )

    instruments = data["symbols"]

    unsupported = {
        raw["contractType"]
        for raw in instruments
        if raw["contractType"] not in SUPPORTED_CONTRACT_TYPES
    }

    if unsupported:
        raise ValueError(
            "Unsupported Binance linear contract types: " f"{sorted(unsupported)}"
        )

    return instruments


def parse_linear_instrument(
    raw: dict[str, Any],
    *,
    exchange_id: int,
    fetched_at: datetime,
) -> LinearInstrumentMetadata:
    price_filter = _get_filter(
        raw,
        "PRICE_FILTER",
    )

    lot_size = _get_filter(
        raw,
        "LOT_SIZE",
    )

    min_notional = _get_filter(
        raw,
        "MIN_NOTIONAL",
    )

    if price_filter is None:
        raise ValueError("Missing PRICE_FILTER for Binance " f"{raw['symbol']}")

    if lot_size is None:
        raise ValueError("Missing LOT_SIZE for Binance " f"{raw['symbol']}")

    instrument_type, expiry_ns = _parse_instrument_type(raw)

    instrument = Instrument(
        exchange_id=exchange_id,
        symbol=raw["symbol"],
        instrument_type=instrument_type,
        market_category="linear",
        base_asset=raw["baseAsset"],
        quote_asset=raw["quoteAsset"],
        settlement_asset=raw["marginAsset"],
        launch_time_ns=(int(raw["onboardDate"]) * 1_000_000),
        expiry_ns=expiry_ns,
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
            Decimal(min_notional["notional"]) if min_notional is not None else None
        ),
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
