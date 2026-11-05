"""Concrete current-market identities and unit-preserving decimal observations."""

from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Literal, cast

from pydantic import BaseModel, ConfigDict, Field, model_validator

from forge.shared.errors import MarketForgeError

Exchange = Literal["binance", "bybit", "okx", "gateio", "bitget"]
Market = Literal["spot", "linear", "inverse"]


class MarketDataError(MarketForgeError):
    """An upstream market response or comparison cannot be interpreted safely."""


def obj(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        raise MarketDataError("Expected a JSON object.")
    return cast(dict[str, object], value)


def array(value: object) -> list[object]:
    if not isinstance(value, list):
        raise MarketDataError("Expected a JSON array.")
    return cast(list[object], value)


def string(value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        raise MarketDataError("Required source identity is absent.")
    return value


def number(value: object, *, signed: bool = False) -> Decimal | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool) or not isinstance(value, (str, int, float, Decimal)):
        raise MarketDataError("Invalid decimal statistic.")
    try:
        result = Decimal(str(value))
    except InvalidOperation:
        raise MarketDataError("Invalid decimal statistic.") from None
    if not result.is_finite() or (result < 0 and not signed):
        raise MarketDataError("Non-finite or negative quantity.")
    return result


class Instrument(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    exchange: Exchange
    market: Market
    instrument_type: Literal["spot", "perpetual", "future"] = "perpetual"
    qualified_reference: bool = False
    launch_time_ns: int | None = Field(default=None, ge=1)
    expiry_ns: int | None = Field(default=None, ge=1)
    source_status: str | None = None
    min_qty: Decimal | None = None
    max_qty: Decimal | None = None
    min_notional: Decimal | None = None
    symbol: str = Field(min_length=1)
    base: str = Field(min_length=1)
    quote: str = Field(min_length=1)
    settlement: str | None = None
    quantity_type: Literal["base", "quote", "contracts"] = "base"
    contract_value: Decimal | None = None
    contract_unit: str | None = None
    tick_size: Decimal | None = None
    qty_step: Decimal | None = None
    active: bool = True
    raw: dict[str, object] = Field(default_factory=lambda: dict[str, object]())

    @property
    def reference(self) -> str:
        kind = f"{self.instrument_type}." if self.qualified_reference else ""
        return f"{self.exchange}.{self.market}.{kind}{self.symbol}"

    @model_validator(mode="before")
    @classmethod
    def default_kind(cls, value: object) -> object:
        if isinstance(value, dict):
            data = cast(dict[str, object], value).copy()
            data.setdefault(
                "instrument_type", "spot" if data.get("market") == "spot" else "perpetual"
            )
            return data
        return value

    @model_validator(mode="after")
    def finite_specs(self) -> "Instrument":
        for v in (
            self.contract_value,
            self.tick_size,
            self.qty_step,
            self.min_qty,
            self.max_qty,
            self.min_notional,
        ):
            if v is not None and (not v.is_finite() or v <= 0):
                raise ValueError("Specifications must be finite and positive.")
        if (self.instrument_type == "spot") != (self.market == "spot"):
            raise ValueError("Instrument type and market category are incompatible.")
        if self.instrument_type != "future" and self.expiry_ns is not None:
            raise ValueError("Only dated futures have expiry metadata.")
        return self


class Observation(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    instrument: Instrument
    fetched_at: datetime
    source_observed_at: datetime | None = None
    window_start: datetime
    window_end: datetime
    window_basis: Literal["source", "fetch_time"]
    last_price: Decimal | None = None
    base_volume: Decimal | None = None
    quote_volume: Decimal | None = None
    contract_volume: Decimal | None = None
    turnover: Decimal | None = None
    turnover_unit: str | None = None
    turnover_estimated: bool = False
    open_interest: Decimal | None = None
    open_interest_unit: str | None = None
    funding_rate: Decimal | None = None
    raw: dict[str, object]
    interpretation: dict[str, object] = Field(default_factory=lambda: dict[str, object]())
    mapping_version: str = "current-metrics-v1"

    @model_validator(mode="after")
    def validate_units(self) -> "Observation":
        for name in (
            "last_price",
            "base_volume",
            "quote_volume",
            "contract_volume",
            "turnover",
            "open_interest",
            "funding_rate",
        ):
            value = getattr(self, name)
            if value is not None and (
                not value.is_finite() or (name != "funding_rate" and value < 0)
            ):
                raise ValueError("Invalid statistic.")
        if (self.turnover is None) != (self.turnover_unit is None):
            raise ValueError("Turnover requires a unit.")
        if (self.open_interest is None) != (self.open_interest_unit is None):
            raise ValueError("Open interest requires a unit.")
        if self.turnover_unit is not None and self.turnover_unit not in {
            self.instrument.base,
            self.instrument.quote,
            self.instrument.settlement,
        }:
            raise ValueError("Turnover unit is incompatible with instrument.")
        if self.open_interest_unit is not None and self.open_interest_unit not in {
            self.instrument.base,
            self.instrument.quote,
            "contracts",
        }:
            raise ValueError("Invalid open interest unit.")
        if (
            self.window_start.tzinfo is None
            or self.window_end.tzinfo is None
            or self.fetched_at.tzinfo is None
            or self.window_end <= self.window_start
        ):
            raise ValueError("A valid aware statistical window is required.")
        return self
