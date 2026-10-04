from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from marketforge.catalog.models import Instrument, InstrumentSpec

from .client import get

LINEAR_PRODUCT_TYPES = (
    "USDT-FUTURES",
    "USDC-FUTURES",
)


@dataclass(frozen=True, slots=True)
class LinearInstrumentMetadata:
    instrument: Instrument
    spec: InstrumentSpec
    raw: dict[str, Any]


def _optional_ms_to_ns(value: str) -> int | None:
    if not value:
        return None

    return int(value) * 1_000_000


def _price_tick(raw: dict[str, Any]) -> Decimal:
    price_place = int(raw["pricePlace"])
    price_end_step = Decimal(raw["priceEndStep"])

    return price_end_step * Decimal(1).scaleb(-price_place)


def _settlement_asset(
    raw: dict[str, Any],
) -> str:
    margin_coins = raw["supportMarginCoins"]

    if len(margin_coins) != 1:
        raise ValueError(
            "Expected exactly one Bitget settlement asset "
            f"for {raw['symbol']}: {margin_coins}"
        )

    return margin_coins[0]


def fetch_linear_instruments() -> list[dict[str, Any]]:
    instruments: list[dict[str, Any]] = []

    for product_type in LINEAR_PRODUCT_TYPES:
        data = get(
            "/api/v2/mix/market/contracts",
            params={
                "productType": product_type,
            },
        )

        instruments.extend(data["data"])

    return instruments


def parse_linear_instrument(
    raw: dict[str, Any],
    *,
    exchange_id: int,
    fetched_at: datetime,
) -> LinearInstrumentMetadata:
    if raw["symbolType"] != "perpetual":
        raise ValueError(
            "Unsupported Bitget linear symbol type: " f"{raw['symbolType']}"
        )

    instrument = Instrument(
        exchange_id=exchange_id,
        symbol=raw["symbol"],
        instrument_type="perpetual",
        market_category="linear",
        base_asset=raw["baseCoin"],
        quote_asset=raw["quoteCoin"],
        settlement_asset=_settlement_asset(raw),
        launch_time_ns=_optional_ms_to_ns(raw["launchTime"]),
        expiry_ns=None,
        strike=None,
        option_type=None,
        status=raw["symbolStatus"],
    )

    spec = InstrumentSpec(
        instrument_id=None,
        quantity_type="base",
        contract_value=None,
        contract_value_asset=None,
        tick_size=_price_tick(raw),
        qty_step=Decimal(raw["sizeMultiplier"]),
        min_qty=Decimal(raw["minTradeNum"]),
        max_qty=Decimal(raw["maxOrderQty"]),
        min_notional=Decimal(raw["minTradeUSDT"]),
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
