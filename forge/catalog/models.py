"""Declarations for manually supplied catalogue identities and specifications."""

import tomllib
from pathlib import Path
from typing import Self

from pydantic import Field, field_validator, model_validator

from forge.historical.imports.models import HistoricalSpecs, ImportIdentity
from forge.shared.config.models import SettingsModel

TOKEN = r"^[a-z][a-z0-9_]*$"
ASSET = r"^\S+$"


class Exchange(SettingsModel, frozen=True):
    code: str = Field(pattern=TOKEN, max_length=32)
    name: str = Field(min_length=1, pattern=r"^\S(?:[^\r\n]*\S)?$")

    @field_validator("name")
    @classmethod
    def printable_name(cls, value: str) -> str:
        if not value.isprintable():
            raise ValueError("An exchange name must contain printable characters.")
        return value

    @field_validator("code")
    @classmethod
    def venue(cls, value: str) -> str:
        if value == "external":
            raise ValueError("An execution venue code is required.")
        return value


class Instrument(ImportIdentity, frozen=True):
    base_asset: str = Field(pattern=ASSET)
    quote_asset: str = Field(pattern=ASSET)
    settlement_asset: str | None = Field(default=None, pattern=ASSET)
    status: str = Field(default="active", pattern=ASSET)


def reference(value: str) -> ImportIdentity:
    parts = value.split(".", 3)
    if len(parts) != 4:
        raise ValueError("Expected exchange.instrument_type.market_category.symbol.")
    return ImportIdentity.model_validate(
        dict(zip(("exchange", "instrument_type", "market_category", "symbol"), parts, strict=True))
    )


class Specification(HistoricalSpecs, frozen=True):
    instrument: str
    base_asset: str | None = Field(default=None, pattern=ASSET)
    quote_asset: str | None = Field(default=None, pattern=ASSET)
    settlement_asset: str | None = Field(default=None, pattern=ASSET)

    @model_validator(mode="after")
    def declaration(self) -> Self:
        instrument = reference(self.instrument)
        if not self.provenance or not self.provenance.strip():
            raise ValueError("Supply specification provenance.")
        if self.tick_size is None:
            raise ValueError("Supply a positive tick_size.")
        if instrument.instrument_type in {"spot", "equity"} and (
            self.contract_value is not None or self.quantity_type == "contracts"
        ):
            raise ValueError("Spot and equity specifications cannot declare contract sizing.")
        return self


MODELS = {"exchange": Exchange, "instrument": Instrument, "specs": Specification}


def load(kind: str, path: Path) -> Exchange | Instrument | Specification:
    with path.open("rb") as source:
        return MODELS[kind].model_validate(tomllib.load(source))
