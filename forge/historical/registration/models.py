"""User-owned registration declarations; no market semantics inferred from bytes."""

import tomllib
from pathlib import Path
from typing import Literal, Self

from pydantic import Field, model_validator

from forge.catalog.models import reference
from forge.historical.archives.config import AcquisitionInterval
from forge.historical.imports.models import ImportConfig
from forge.historical.profiles.service import identity
from forge.shared.config.models import SettingsModel
from forge.shared.errors import MarketForgeError
from forge.shared.paths import ProjectPaths


class Registration(SettingsModel, frozen=True):
    version: Literal[1]
    name: str = Field(min_length=1)


class Source(SettingsModel, frozen=True):
    key: str = Field(min_length=1)
    provider: str = Field(min_length=1)
    product: str = Field(min_length=1)
    root: str = Field(min_length=1)
    provenance: str = Field(min_length=1)
    kind: Literal["recording", "purchased", "other"] = "recording"


class NativeIdentity(SettingsModel, frozen=True):
    namespace: str = Field(min_length=1)
    publisher_field: str = Field(min_length=1)
    publisher_id: int = Field(strict=True, ge=0)
    instrument_field: str = Field(min_length=1)
    instrument_id: int = Field(strict=True, ge=0)
    provenance: str = Field(min_length=1)

    @model_validator(mode="after")
    def distinct_fields(self) -> Self:
        if self.publisher_field == self.instrument_field:
            raise ValueError("Declare separate publisher and instrument fields.")
        return self


class Input(SettingsModel, frozen=True):
    instrument: str
    profile: str
    native_identity: NativeIdentity | None = None
    specification: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")
    content: Literal["trades", "depth"]
    files: list[str] = Field(default_factory=list)
    patterns: list[str] = Field(default_factory=list)
    max_files: int = Field(default=1000, ge=1, le=10000)


class Recipe(SettingsModel, frozen=True):
    registration: Registration
    source: Source
    inputs: list[Input] = Field(min_length=1)
    coverage: AcquisitionInterval

    @model_validator(mode="after")
    def references(self) -> Self:
        for index, group in enumerate(self.inputs):
            instrument = reference(group.instrument)
            expected = f"{instrument.exchange}.{instrument.instrument_type}.{instrument.market_category}.{group.content}"
            if identity(group.profile) != expected:
                raise ValueError(f"inputs.{index}.profile: expected {expected}")
            if not group.files and not group.patterns:
                raise ValueError(f"inputs.{index}: select explicit files or bounded patterns")
        return self


def recipe_path(paths: ProjectPaths, name: str) -> Path:
    supplied = Path(name).expanduser()
    if supplied.is_absolute() or len(supplied.parts) > 1:
        return supplied.resolve()
    if supplied.name in {".", ".."}:
        raise MarketForgeError("Invalid registration name.")
    return (
        paths.config_dir
        / "historical/registration"
        / (name if name.endswith(".toml") else name + ".toml")
    )


def load(path: Path) -> Recipe:
    return Recipe.model_validate(tomllib.loads(path.read_text()))


def groups(recipe: Recipe, paths: ProjectPaths) -> list[ImportConfig]:
    root = Path(recipe.source.root).expanduser()
    if not root.is_absolute():
        root = paths.root / root
    return [
        ImportConfig(
            source_key=recipe.source.key,
            source_kind=recipe.source.kind,
            root=str(root.resolve()),
            files=group.files,
            patterns=group.patterns,
            max_files=group.max_files,
            identity=reference(group.instrument),
            data_type="trades" if group.content == "trades" else "order_book_l2",
            format_profile=group.profile,
            interval=recipe.coverage,
        )
        for group in recipe.inputs
    ]
