"""Editable conversion configuration and fingerprinted upstream intent."""

import hashlib
import os
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Literal, Self

from pydantic import Field, model_validator

from forge.historical.archives.config import AcquisitionConfig
from forge.historical.conversion.contract import Currency, Digest, Policy, Resources
from forge.historical.conversion.selection import ReferenceOptions
from forge.historical.engine.runtime_config import directory_fd
from forge.historical.merge.config import (
    DatabaseID,
    DerivedRecipe,
    Timestamp,
    normalization_path,
    unix_ns,
)
from forge.historical.merge.service import Recipe as DirectMerge
from forge.historical.normalization.config import NormalizationRecipe
from forge.orchestration.registry import retain_recipe
from forge.shared.config.library import config_filename, resolve_config_file
from forge.shared.config.loader import configuration_errors
from forge.shared.config.models import SettingsModel
from forge.shared.config.toml import dumps
from forge.shared.errors import MarketForgeError
from forge.shared.identity import RunOperation
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


EnginePublicationID = Annotated[str, Field(pattern=r"^[1-9][0-9]{0,19}$")]


class PublicationSource(SettingsModel, frozen=True):
    kind: Literal["publication"] = "publication"
    registry_publication_id: DatabaseID
    registry_id: str = Field(
        pattern=r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
    )
    environment: Literal["testing", "production"]
    engine_publication_id: EnginePublicationID
    manifest_sha256: Digest
    processing_job_sha256: Digest
    publication_checksums_sha256: Digest
    interval: Interval

    @model_validator(mode="after")
    def engine_id_is_uint64(self) -> Self:
        if int(self.engine_publication_id) >= 2**64:
            raise ValueError("Engine publication ID must fit in uint64.")
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


class Recipe(ReferenceOptions, frozen=True):
    version: Literal[1, 2]
    source: Annotated[Source | PublicationSource, Field(discriminator="kind")]
    interval: Interval
    selection: Selection = Field(default_factory=Selection)
    conversion: Policy
    resources: Resources = Field(default_factory=Resources)

    @model_validator(mode="after")
    def source_and_references(self) -> Self:
        if isinstance(self.source, PublicationSource):
            if (
                self.selection.file_dataset_ids
                or self.selection.publication_ids
                or self.selection.expected_engine_publication_ids
            ):
                raise ValueError(
                    "A publication source already pins the selection; remove other source selectors."
                )
            if not self.selection.whole_publication:
                raise ValueError("A publication source requires selection.whole_publication=true.")
            if self.interval != self.source.interval:
                raise ValueError(
                    "A publication source interval must equal the committed half-open interval; cropping is unsupported."
                )
        self.validate_reference_options(self.version)
        return self


@dataclass(frozen=True)
class LoadedSource:
    declaration: Source
    config: Upstream
    interval: Interval


def library(paths: ProjectPaths) -> Path:
    return paths.workflow_dir("conversion")


def source_path(paths: ProjectPaths, kind: SourceKind, reference: str) -> Path:
    if "/" in reference or Path(reference).is_absolute():
        return Path(reference).expanduser().resolve()
    if kind == "normalize":
        return normalization_path(paths, reference).resolve()
    directory = paths.workflow_dir("merge") if kind == "merge" else paths.acquisition_config_dir
    return resolve_config_file(directory, reference)


def load_source(paths: ProjectPaths, kind: SourceKind, reference: str) -> LoadedSource:
    path = source_path(paths, kind, reference)
    data = path.read_bytes()
    with configuration_errors(path):
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


def read_recipe(path: Path) -> tuple[Recipe, bytes]:
    data = path.read_bytes()
    with configuration_errors(path):
        recipe = Recipe.model_validate(tomllib.loads(data.decode()))
    return recipe, data


def validate_binding(
    paths: ProjectPaths, path: Path, recipe: Recipe
) -> LoadedSource | PublicationSource:
    if isinstance(recipe.source, PublicationSource):
        return recipe.source
    source = load_source(paths, recipe.source.kind, recipe.source.reference)
    if source.declaration != recipe.source:
        raise MarketForgeError(
            f"{path}: source path or fingerprint changed ({source.declaration.path}). Review and recreate the conversion recipe."
        )
    return source


def load_recipe(
    paths: ProjectPaths, path: Path, *, retain: bool = True
) -> tuple[Recipe, LoadedSource | PublicationSource, bytes]:
    recipe, data = read_recipe(path)
    if retain:
        retain_recipe(
            RunOperation.CONVERT,
            path,
            data,
            effective=recipe.model_dump(mode="json"),
            family="conversion",
        )
    source = validate_binding(paths, path, recipe)
    return recipe, source, data


def create(
    paths: ProjectPaths, name: str, kind: SourceKind, reference: str, target: Currency
) -> dict[str, object]:
    if "/" in reference:
        reference = str(Path(reference).expanduser().resolve())
    upstream = load_source(paths, kind, reference)
    return create_bound(paths, name, upstream.declaration, upstream.interval, target)


def create_bound(
    paths: ProjectPaths,
    name: str,
    source: Source | PublicationSource,
    interval: Interval,
    target: Currency,
) -> dict[str, object]:
    destination = resolve_config_file(library(paths), name)
    entry = library(paths).resolve() / config_filename(name)
    if destination != entry or entry.exists() or entry.is_symlink():
        raise MarketForgeError(f"Conversion recipe already exists: {destination}")
    publication = isinstance(source, PublicationSource)
    recipe = Recipe(
        version=2,
        source=source,
        interval=interval,
        selection=Selection(whole_publication=publication),
        conversion=Policy(target_quote=target, max_staleness_ns=60_000_000_000),
    )
    fields = recipe.model_dump(mode="json", exclude_none=True)
    document = (
        "# Trade references are resolved automatically during preparation. Review the resource limits.\n"
        + (
            "# The bound source fixes the whole publication and its committed interval.\n"
            if publication
            else "# Review the interval and use selection IDs to disambiguate revisions.\n"
        )
        + dumps(fields)
    )
    try:
        with directory_fd(destination.parent, create=True) as directory:
            handle = os.open(
                destination.name,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                0o666,
                dir_fd=directory,
            )
            with os.fdopen(handle, "w", encoding="utf-8") as output:
                output.write(document)
    except FileExistsError as error:
        raise MarketForgeError(f"Conversion recipe already exists: {destination}") from error
    result: dict[str, object] = {
        "status": "template_created",
        "path": str(destination),
        "source": fields["source"],
    }
    if publication:
        result.update(
            source_binding="committed_metadata_checked",
            execution_scope="whole_merge_publication",
            reference_configuration="review_required",
            execution_readiness="not_assessed",
        )
    return result
