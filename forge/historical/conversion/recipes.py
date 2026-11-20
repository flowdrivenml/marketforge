"""Editable conversion configuration and fingerprinted upstream intent."""

import hashlib
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Self

from pydantic import Field, model_validator

from forge.historical.archives.config import AcquisitionConfig
from forge.historical.conversion.contract import Currency, Policy, Resources
from forge.historical.merge.config import (
    DatabaseID,
    DerivedRecipe,
    Timestamp,
    normalization_path,
    unix_ns,
)
from forge.historical.merge.service import Recipe as DirectMerge
from forge.historical.normalization.config import NormalizationRecipe
from forge.shared.config.library import resolve_config_file
from forge.shared.config.models import SettingsModel
from forge.shared.config.toml import dumps
from forge.shared.errors import MarketForgeError
from forge.shared.paths import ProjectPaths

SourceKind = Literal["merge", "normalize", "acquisition"]
Upstream = DirectMerge | DerivedRecipe | NormalizationRecipe | AcquisitionConfig


class Source(SettingsModel, frozen=True):
    kind: SourceKind
    reference: str = Field(pattern=r"\S")
    path: str = Field(pattern=r"\S")
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class Interval(SettingsModel, frozen=True):
    start_ns: Timestamp
    end_ns: Timestamp

    @model_validator(mode="after")
    def ordered(self) -> Self:
        if self.start_ns >= self.end_ns:
            raise ValueError("Conversion interval must be nonempty and half-open.")
        return self


class Selection(SettingsModel, frozen=True):
    file_dataset_ids: list[DatabaseID] | None = Field(default=None, min_length=1)
    publication_ids: list[DatabaseID] | None = Field(default=None, min_length=1)
    expected_engine_publication_ids: list[str] | None = Field(default=None, min_length=1)
    whole_publication: bool = False

    @model_validator(mode="after")
    def selectors(self) -> Self:
        if self.file_dataset_ids and self.publication_ids:
            raise ValueError("Use either file_dataset_ids or publication_ids.")
        for ids in (
            self.file_dataset_ids,
            self.publication_ids,
            self.expected_engine_publication_ids,
        ):
            if ids and len(ids) != len(set(ids)):
                raise ValueError("Publication selectors must be unique.")
        for text in self.expected_engine_publication_ids or ():
            if (
                not text.isascii()
                or not text.isdecimal()
                or not 0 < int(text) < 2**64
                or str(int(text)) != text
            ):
                raise ValueError(
                    "Expected engine publication IDs must be canonical uint64 strings."
                )
        return self


class Recipe(SettingsModel, frozen=True):
    version: Literal[1]
    source: Source
    interval: Interval
    selection: Selection = Field(default_factory=Selection)
    conversion: Policy
    resources: Resources = Field(default_factory=Resources)
    reference_stream_ids: list[str] = Field(default_factory=list)
    reference_publication_id: DatabaseID | None = None

    @model_validator(mode="after")
    def reference_selection(self) -> Self:
        refs = self.reference_stream_ids
        if any(not item.strip() for item in refs) or len(refs) != len(set(refs)):
            raise ValueError("Reference stream IDs must be nonblank and unique.")
        if self.reference_publication_id and not refs:
            raise ValueError(
                "An external reference publication requires explicit trade stream IDs."
            )
        return self


@dataclass(frozen=True)
class LoadedSource:
    declaration: Source
    config: Upstream
    interval: Interval


def library(paths: ProjectPaths) -> Path:
    return paths.config_dir / "historical/conversion"


def source_path(paths: ProjectPaths, kind: SourceKind, reference: str) -> Path:
    if "/" in reference or Path(reference).is_absolute():
        return Path(reference).expanduser().resolve()
    if kind == "normalize":
        return normalization_path(paths, reference).resolve()
    directory = (
        paths.config_dir / "historical/merge" if kind == "merge" else paths.acquisition_config_dir
    )
    return resolve_config_file(directory, reference)


def load_source(paths: ProjectPaths, kind: SourceKind, reference: str) -> LoadedSource:
    path = source_path(paths, kind, reference)
    data = path.read_bytes()
    document = tomllib.loads(data.decode())
    config: Upstream
    if kind == "merge":
        config = (
            DirectMerge.model_validate(document)
            if "dataset_ids" in document
            else DerivedRecipe.model_validate(document)
        )
    elif kind == "normalize":
        if {"inputs", "job_id"} & document.keys():
            raise MarketForgeError(
                "Legacy archive-ID normalization requests cannot establish conversion recipe lineage."
            )
        config = NormalizationRecipe.model_validate(document)
    else:
        config = AcquisitionConfig.model_validate(document)
    interval = (
        Interval(start_ns=config.start_ns, end_ns=config.end_ns)
        if isinstance(config, (DirectMerge, DerivedRecipe))
        else Interval(start_ns=unix_ns(config.interval.start), end_ns=unix_ns(config.interval.end))
    )
    return LoadedSource(
        Source(
            kind=kind, reference=reference, path=str(path), sha256=hashlib.sha256(data).hexdigest()
        ),
        config,
        interval,
    )


def load_recipe(paths: ProjectPaths, path: Path) -> tuple[Recipe, LoadedSource, bytes]:
    data = path.read_bytes()
    recipe = Recipe.model_validate(tomllib.loads(data.decode()))
    source = load_source(paths, recipe.source.kind, recipe.source.reference)
    if source.declaration != recipe.source:
        raise MarketForgeError(
            "Upstream recipe path or fingerprint changed. Review and recreate the conversion recipe."
        )
    return recipe, source, data


def create(
    paths: ProjectPaths, name: str, kind: SourceKind, reference: str, target: Currency
) -> dict[str, object]:
    destination = resolve_config_file(library(paths), name)
    if destination.exists():
        raise MarketForgeError(f"Conversion recipe already exists: {destination}")
    if "/" in reference:
        reference = str(Path(reference).expanduser().resolve())
    upstream = load_source(paths, kind, reference)
    recipe = Recipe(
        version=1,
        source=upstream.declaration,
        interval=upstream.interval,
        conversion=Policy(target_quote=target, max_staleness_ns=60_000_000_000),
    )
    fields = recipe.model_dump(exclude_none=True)
    sections = ("source", "interval", "selection", "conversion", "resources")
    base = {key: value for key, value in fields.items() if key not in sections}
    document = (
        "# Select reference trade stream IDs explicitly. Review the interval and resource limits.\n"
        "# Set selection.file_dataset_ids or selection.publication_ids to disambiguate revisions.\n"
        + dumps(base)
        + "".join(f"\n[{name}]\n" + dumps(fields[name]) for name in sections)
    )
    destination.parent.mkdir(parents=True, exist_ok=True)
    try:
        with destination.open("x", encoding="utf-8") as output:
            output.write(document)
    except FileExistsError as error:
        raise MarketForgeError(f"Conversion recipe already exists: {destination}") from error
    return {"status": "template_created", "path": str(destination), "source": fields["source"]}
