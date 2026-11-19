"""Managed merge recipes and normalization-source references."""

import hashlib
import tomllib
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, Literal, Self

from pydantic import Field, TypeAdapter, field_validator, model_validator

from forge.catalog.models import reference
from forge.historical.archives.config import AcquisitionInterval
from forge.historical.normalization.config import NormalizationRecipe
from forge.historical.normalization.recipes import recipe_path
from forge.shared.config.models import SettingsModel
from forge.shared.errors import MarketForgeError
from forge.shared.paths import ProjectPaths

DatabaseID = Annotated[int, Field(strict=True, gt=0, lt=2**63)]
StreamRank = Annotated[int, Field(strict=True, ge=0, le=2**32 - 1)]
Timestamp = Annotated[int, Field(strict=True, ge=-(2**63), lt=2**63)]


def unix_ns(value: datetime) -> int:
    delta = value - datetime(1970, 1, 1, tzinfo=UTC)
    return ((delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds) * 1000


class Selection(SettingsModel, frozen=True):
    source: str = Field(min_length=1, pattern=r"\S")
    operation: Literal["normalize"] = "normalize"
    mode: Literal["all"] = "all"
    data_types: list[Literal["trades", "order_book_l2"]] | None = Field(default=None, min_length=1)
    exchanges: list[Annotated[str, Field(pattern=r"^[a-z][a-z0-9_]*$")]] | None = Field(
        default=None, min_length=1
    )
    instruments: list[str] | None = Field(default=None, min_length=1)
    instrument_types: list[Literal["spot", "perpetual", "future", "option", "equity"]] | None = (
        Field(default=None, min_length=1)
    )
    market_categories: list[Literal["spot", "linear", "inverse", "option", "equity"]] | None = (
        Field(default=None, min_length=1)
    )
    publication_ids: list[DatabaseID] | None = Field(default=None, min_length=1)

    @field_validator("instruments")
    @classmethod
    def canonical_instruments(cls, values: list[str] | None) -> list[str] | None:
        for value in values or ():
            reference(value)
        return values

    @field_validator("publication_ids")
    @classmethod
    def unique_publications(cls, values: list[int] | None) -> list[int] | None:
        if values is not None and len(values) != len(set(values)):
            raise ValueError("Select each registry publication once.")
        return values


class DerivedRecipe(SettingsModel, frozen=True):
    version: Literal[1]
    interval: AcquisitionInterval
    selection: Selection
    stream_ranks: dict[str, StreamRank] = Field(default_factory=dict)

    @property
    def start_ns(self) -> int:
        return unix_ns(self.interval.start)

    @property
    def end_ns(self) -> int:
        return unix_ns(self.interval.end)

    @model_validator(mode="after")
    def engine_range(self) -> Self:
        validator = TypeAdapter(Timestamp)
        validator.validate_python(self.start_ns)
        validator.validate_python(self.end_ns)
        return self


def normalization_path(paths: ProjectPaths, source: str) -> Path:
    if "/" in source:
        return Path(source).expanduser().resolve()
    return recipe_path(paths, source)


def normalization_source(paths: ProjectPaths, source: str) -> tuple[Path, NormalizationRecipe, str]:
    path = normalization_path(paths, source)
    if not path.is_file():
        raise MarketForgeError(f"Unknown normalization configuration {source!r}: {path}")
    data = path.read_bytes()
    document = tomllib.loads(data.decode("utf-8"))
    if "inputs" in document or "job_id" in document:
        raise MarketForgeError(
            "Source-derived merge requires a normalization recipe with requests and an interval. "
            "For a legacy archive-ID request, select the registered file dataset IDs explicitly."
        )
    return path, NormalizationRecipe.model_validate(document), hashlib.sha256(data).hexdigest()
