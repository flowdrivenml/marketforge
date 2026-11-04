"""Named turnover policies; conversion is explicit and snapshotted."""

import tomllib
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Literal, Self

from pydantic import Field, field_validator, model_validator

from forge.markets.models import Exchange, Market, number
from forge.shared.config.models import SettingsModel


class RankingPolicy(SettingsModel, frozen=True):
    version: Literal[1] = 1
    metric: Literal["turnover_24h"] = "turnover_24h"
    exchanges: list[Exchange] = Field(default_factory=lambda: list[Exchange]())
    markets: list[Market] = Field(default_factory=lambda: ["spot", "linear", "inverse"])
    instruments: list[str] = Field(default_factory=list)
    max_age_seconds: int = Field(default=3600, ge=1, le=604800)
    max_window_skew_seconds: int = Field(default=300, ge=0, le=86400)
    comparison_currency: str = Field(default="USDT", pattern=r"^[A-Z0-9]+$")
    estimates: Literal["exclude", "allow"] = "exclude"
    allow_partial_universe: bool = False
    manual_preferences: list[str] = Field(default_factory=list)
    conversion_rates: dict[str, Decimal] = Field(default_factory=lambda: dict[str, Decimal]())
    conversion_as_of: datetime | None = None
    conversion_source: str | None = None

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
        if self.conversion_rates and (
            self.conversion_as_of is None
            or self.conversion_as_of.tzinfo is None
            or not self.conversion_source
        ):
            raise ValueError("Explicit rates need aware conversion_as_of and conversion_source.")
        if len(set(self.manual_preferences)) != len(self.manual_preferences):
            raise ValueError("Duplicate manual preferences.")
        if not self.markets:
            raise ValueError("Select at least one market.")
        return self


def load_policy(path: Path) -> RankingPolicy:
    with path.open("rb") as source:
        return RankingPolicy.model_validate(tomllib.load(source))


TEMPLATE = """version = 1
metric = "turnover_24h"
comparison_currency = "USDT"
max_age_seconds = 3600
max_window_skew_seconds = 300
estimates = "exclude"
allow_partial_universe = false
exchanges = ["binance", "bybit", "okx", "gateio", "bitget"]
markets = ["spot", "linear", "inverse"]
manual_preferences = []
# Different denominations are excluded unless an explicit rate and provenance
# are supplied. Allow a reported partial universe only when that is intentional.
# conversion_as_of = 2026-11-04T00:00:00Z
# conversion_source = "explicit user approximation, not an observed FX rate"
# [conversion_rates]
# USD = "1.0"
"""
