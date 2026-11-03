from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal


@dataclass(frozen=True, slots=True)
class Exchange:
    code: str
    name: str
    id: int | None = None


@dataclass(frozen=True, slots=True)
class Instrument:
    exchange_id: int
    symbol: str
    instrument_type: str
    market_category: str

    base_asset: str | None = None
    quote_asset: str | None = None
    settlement_asset: str | None = None

    launch_time_ns: int | None = None
    expiry_ns: int | None = None

    strike: Decimal | None = None
    option_type: str | None = None

    status: str | None = None

    id: int | None = None


@dataclass(frozen=True, slots=True)
class InstrumentSpec:
    quantity_type: str

    instrument_id: int | None = None

    contract_value: Decimal | None = None
    contract_value_asset: str | None = None

    tick_size: Decimal | None = None
    qty_step: Decimal | None = None
    min_qty: Decimal | None = None
    max_qty: Decimal | None = None
    min_notional: Decimal | None = None

    fetched_at: datetime | None = None
