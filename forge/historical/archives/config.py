"""Bulk historical acquisition configuration."""

from __future__ import annotations

import tomllib
from pathlib import Path
from typing import Literal, Self

from pydantic import AwareDatetime, Field, model_validator

from forge.shared.config.loader import ConfigurationLoadError
from forge.shared.config.models import SettingsModel


class ArchiveScope(SettingsModel, frozen=True):
    """A source-native archive selection scope."""

    kind: Literal["instrument", "base_coin", "family"]
    identifier: str = Field(min_length=1, pattern=r"\S")


class AcquisitionInterval(SettingsModel, frozen=True):
    """Requested half-open interval: [start, end)."""

    start: AwareDatetime
    end: AwareDatetime

    @model_validator(mode="after")
    def validate_order(self) -> Self:
        if self.end <= self.start:
            raise ValueError("end must be later than start")

        return self


class ArchiveRequest(SettingsModel, frozen=True):
    """One archive collection requested from a source."""

    exchange: str = Field(pattern=r"^[a-z][a-z0-9_]*$")
    instrument_type: Literal["spot", "perpetual", "future", "option"]
    market_category: Literal["spot", "linear", "inverse"]
    data_type: Literal["trades", "order_book_l2"]
    scope: ArchiveScope


class AcquisitionConfig(SettingsModel, frozen=True):
    """A versioned bulk acquisition request."""

    version: int = Field(default=1, ge=1, le=1)
    interval: AcquisitionInterval
    requests: list[ArchiveRequest] = Field(min_length=1)


def load_acquisition(path: Path) -> AcquisitionConfig:
    """Read and structurally validate a bulk acquisition TOML file."""
    source = path.expanduser().resolve()

    try:
        with source.open("rb") as stream:
            payload: object = tomllib.load(stream)
    except tomllib.TOMLDecodeError as exc:
        raise ConfigurationLoadError(f"Invalid acquisition TOML: {source}") from exc
    except (OSError, UnicodeError) as exc:
        raise ConfigurationLoadError(f"Cannot read acquisition configuration: {source}") from exc

    return AcquisitionConfig.model_validate(payload)
