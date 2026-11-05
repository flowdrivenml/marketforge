"""Named turnover policies; conversion is explicit and snapshotted."""

import tomllib
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Literal, Self, cast

from pydantic import Field, field_validator, model_validator

from forge.markets.capabilities import EXCHANGES
from forge.markets.models import Exchange, Market, MarketDataError, number
from forge.shared.config.models import SettingsModel


def currency(value: str) -> str:
    import re

    if re.fullmatch(r"[A-Z0-9]{1,32}", value) is None:
        raise ValueError("Currency codes must be uppercase letters/digits (1..32 characters).")
    return value


class ManualRate(SettingsModel, frozen=True):
    rate: Decimal
    as_of: datetime
    source: str = Field(min_length=1)

    @field_validator("rate", mode="before")
    @classmethod
    def parse_rate(cls, value: object) -> Decimal:
        try:
            parsed = number(value)
        except MarketDataError:
            raise ValueError("Manual rates must be finite and strictly positive.") from None
        if parsed is None or parsed <= 0:
            raise ValueError("Manual rates must be finite and strictly positive.")
        return parsed

    @field_validator("as_of", mode="before")
    @classmethod
    def parse_time(cls, value: object) -> object:
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value)
            except ValueError:
                raise ValueError("Manual as_of must be an ISO datetime with a timezone.") from None
        return value

    @model_validator(mode="after")
    def validate_rate(self) -> Self:
        if not self.rate.is_finite() or self.rate <= 0:
            raise ValueError("Manual rates must be finite and strictly positive.")
        if self.as_of.tzinfo is None or not self.source.strip():
            raise ValueError("Manual rates require an aware as_of and nonempty source.")
        return self


class ConversionPolicy(SettingsModel, frozen=True):
    automatic: bool = False
    max_price_age_seconds: int | None = Field(default=None, ge=1, le=604800)
    max_leg_skew_seconds: int | None = Field(default=None, ge=0, le=86400)
    max_hops: Literal[1, 2] = 2
    bridge_assets: list[str] = Field(default_factory=lambda: ["BTC", "ETH"], max_length=16)
    prefer_same_exchange: Literal[True] = True
    allow_cross_exchange: bool = False
    allow_mixed_exchanges: Literal[False] = False
    source_exchanges: list[Exchange] = Field(
        default_factory=lambda: list(EXCHANGES), min_length=1, max_length=5
    )
    equivalent_currencies: list[str] = Field(default_factory=list, max_length=32)
    manual_rates: dict[str, ManualRate] = Field(default_factory=lambda: dict[str, ManualRate]())
    valuation: Literal["reference_price_turnover"] = "reference_price_turnover"

    @field_validator("prefer_same_exchange", "allow_mixed_exchanges", mode="before")
    @classmethod
    def strict_literal_bool(cls, value: object) -> object:
        if not isinstance(value, bool):
            raise ValueError("Conversion permissions must be TOML booleans.")
        return value

    @field_validator("max_hops", mode="before")
    @classmethod
    def validate_hops(cls, value: object) -> object:
        if type(value) is not int:
            raise ValueError("max_hops must be the integer 1 or 2.")
        return value

    @model_validator(mode="after")
    def validate_conversion(self) -> Self:
        for values in (self.bridge_assets, self.equivalent_currencies):
            for value in values:
                currency(value)
            if len(set(values)) != len(values):
                raise ValueError("Duplicate conversion currencies are not allowed.")
        for value in self.manual_rates:
            currency(value)
        if len(set(self.source_exchanges)) != len(self.source_exchanges):
            raise ValueError("Duplicate conversion source exchanges are not allowed.")
        return self


class RankingPolicy(SettingsModel, frozen=True):
    version: Literal[1, 2] = 1
    metric: Literal["turnover_24h", "volume"] = "turnover_24h"
    volume_unit: Literal["base_asset"] | None = None
    group_by: Literal["base_asset"] | None = None
    exchanges: list[Exchange] = Field(default_factory=lambda: list[Exchange]())
    markets: list[Market] = Field(default_factory=lambda: ["spot", "linear", "inverse"])
    instruments: list[str] = Field(default_factory=list)
    max_age_seconds: int = Field(default=3600, ge=1, le=604800)
    max_window_skew_seconds: int = Field(default=300, ge=0, le=86400)
    comparison_currency: str = Field(default="USDT", pattern=r"^[A-Z0-9]+$")
    estimates: Literal["exclude", "allow"] = "exclude"
    allow_partial_universe: bool = False
    manual_preferences: list[str] = Field(default_factory=list)
    conversion: ConversionPolicy = Field(default_factory=ConversionPolicy)
    conversion_rates: dict[str, Decimal] = Field(default_factory=lambda: dict[str, Decimal]())
    conversion_as_of: datetime | None = None
    conversion_source: str | None = None

    @model_validator(mode="before")
    @classmethod
    def version_defaults(cls, value: object) -> object:
        if isinstance(value, dict):
            data = cast(dict[str, object], value).copy()
            if data.get("metric") == "volume":
                forbidden = {
                    "conversion",
                    "conversion_rates",
                    "conversion_as_of",
                    "conversion_source",
                    "comparison_currency",
                }
                # Serialized volume policies omit turnover-only defaults.
                if forbidden & data.keys():
                    raise ValueError(
                        "Volume policies cannot contain turnover conversion or comparison-currency settings."
                    )
            if data.get("version") == 2 and data.get("metric") != "volume":
                raw = data.get("conversion", {})
                from forge.markets.models import obj

                data["conversion"] = (
                    raw if isinstance(raw, ConversionPolicy) else {"automatic": True, **obj(raw)}
                )
            return data
        return value

    @field_validator("version", mode="before")
    @classmethod
    def strict_version(cls, value: object) -> object:
        if type(value) is not int:
            raise ValueError("Policy version must be the integer 1 or 2.")
        return value

    @field_validator("conversion_rates", mode="before")
    @classmethod
    def parse_rates(cls, value: object) -> dict[str, Decimal]:
        from forge.markets.models import obj

        result: dict[str, Decimal] = {}
        for key, raw in obj(value).items():
            rate = number(raw)
            if rate is None or rate <= 0:
                raise ValueError("Conversion rates must be finite and positive.")
            result[key] = rate
        return result

    @model_validator(mode="after")
    def validate_policy(self) -> Self:
        if self.metric == "volume":
            if self.volume_unit != "base_asset" or self.group_by != "base_asset":
                raise ValueError(
                    "Volume requires volume_unit=base_asset and group_by=base_asset explicitly."
                )
            if self.manual_preferences:
                raise ValueError(
                    "Grouped volume orders by quantity and instrument reference, not manual preferences."
                )
        elif self.volume_unit is not None or self.group_by is not None:
            raise ValueError("Volume units/grouping are only valid for metric=volume.")
        currency(self.comparison_currency)
        for unit in self.conversion_rates:
            currency(unit)
        if set(self.conversion_rates) & set(self.conversion.manual_rates):
            raise ValueError(
                "A currency appears in both legacy conversion_rates and conversion.manual_rates; choose one representation."
            )
        if self.comparison_currency in self.conversion.equivalent_currencies:
            raise ValueError(
                "equivalent_currencies names currencies equivalent to the comparison currency; do not repeat the comparison currency itself."
            )
        if self.conversion_as_of is not None and self.conversion_as_of.tzinfo is None:
            raise ValueError("conversion_as_of must include a timezone.")
        rates = {
            **self.conversion_rates,
            **{k: v.rate for k, v in self.conversion.manual_rates.items()},
        }
        if self.comparison_currency in rates and rates[self.comparison_currency] != 1:
            raise ValueError(
                "A manual rate for the comparison currency must be 1; identity always takes precedence."
            )
        if self.conversion_rates and (
            self.conversion_as_of is None
            or self.conversion_as_of.tzinfo is None
            or not self.conversion_source
            or not self.conversion_source.strip()
        ):
            raise ValueError("Explicit rates need aware conversion_as_of and conversion_source.")
        if len(set(self.manual_preferences)) != len(self.manual_preferences):
            raise ValueError("Duplicate manual preferences.")
        if not self.markets:
            raise ValueError("Select at least one market.")
        return self


def load_policy(path: Path) -> RankingPolicy:
    with path.open("rb") as source:
        document = tomllib.load(source)
    return RankingPolicy.model_validate(document)


def policy_document(policy: RankingPolicy) -> dict[str, object]:
    document = policy.model_dump(mode="json")
    if policy.metric == "volume":
        for field in (
            "conversion",
            "conversion_rates",
            "conversion_as_of",
            "conversion_source",
            "comparison_currency",
        ):
            document.pop(field, None)
    return document


TEMPLATE = """version = 2
metric = "turnover_24h"
comparison_currency = "USDT"
max_age_seconds = 3600
max_window_skew_seconds = 300
estimates = "exclude" # volume-derived turnover; FX valuation is a separate convention
allow_partial_universe = false
exchanges = ["binance", "bybit", "okx", "gateio", "bitget"]
markets = ["spot", "linear", "inverse"]
manual_preferences = []

[conversion]
automatic = true
valuation = "reference_price_turnover" # aggregate turnover at reference prices, not execution-time FX
max_price_age_seconds = 3600 # omit to reuse max_age_seconds
max_leg_skew_seconds = 300 # omit to reuse max_window_skew_seconds
max_hops = 2 # direct or one bridge, never unlimited traversal
bridge_assets = [] # any connecting spot asset; nonempty lists are ordered allowlists
prefer_same_exchange = true
allow_cross_exchange = false
allow_mixed_exchanges = false # mixed-venue legs are deliberately unsupported
source_exchanges = ["binance", "bybit", "okx", "gateio", "bitget"]
equivalent_currencies = [] # optional ["USD"] explicitly assumes USD == comparison_currency
# Precedence: identity, manual override, explicit equivalence, stored spot rates.
# [conversion.manual_rates.EUR]
# rate = "1.08"
# as_of = 2026-11-05T00:00:00Z # replace with the actual observation time
# source = "explicit user override"
"""
