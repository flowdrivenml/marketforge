"""Strict, named preparation requests, separate from engine runtime settings."""

import tomllib
from pathlib import Path
from typing import Literal, Self

from pydantic import Field, model_validator

from forge.historical.archives.config import AcquisitionInterval, ArchiveSource
from forge.shared.config.models import SettingsModel


class NormalizationSource(ArchiveSource, frozen=True):
    instrument_type: Literal["spot", "perpetual", "future", "option", "equity"]
    market_category: Literal["spot", "linear", "inverse", "option", "equity"]
    source_key: str | None = Field(default=None, min_length=1, pattern=r"^\S+$")

    @model_validator(mode="after")
    def registered_source(self) -> Self:
        if (self.instrument_type == "equity") != (self.market_category == "equity"):
            raise ValueError("Equity instrument and market category must agree.")
        if self.instrument_type == "equity" and not self.source_key:
            raise ValueError("Equity normalization requires a registered source_key.")
        if self.source_key and self.scope.kind != "instrument":
            raise ValueError("Registered source selection requires a concrete instrument scope.")
        return self


class NormalizationRecipe(SettingsModel, frozen=True):
    version: Literal[1] = 1
    interval: AcquisitionInterval
    requests: list[NormalizationSource] = Field(min_length=1)
    ranking_policy: str = Field(default="turnover", pattern=r"^[a-zA-Z0-9_-]+$")


class InputSelection(SettingsModel, frozen=True):
    task_id: int = Field(gt=0, le=2**64 - 1)
    archive_id: int = Field(gt=0, le=2**63 - 1)
    instrument_id: int = Field(gt=0, le=2**63 - 1)
    format_profile: str | None = Field(default=None, min_length=1, pattern=r"\S")
    archive_member: str | None = Field(default=None, min_length=1)
    source_compression: Literal["plain", "gzip", "zip", "tar_gzip"] | None = None
    stream_rank: int | None = Field(default=None, ge=0, le=2**32 - 1)


class OutputSelection(SettingsModel, frozen=True):
    staging_path: str = Field(min_length=1, pattern=r"\S")
    dataset_path: str = Field(min_length=1, pattern=r"\S")


class PreparationRequest(SettingsModel, frozen=True):
    ranking_policy: str = Field(default="turnover", pattern=r"^[a-zA-Z0-9_-]+$")
    version: Literal[1] = 1
    job_id: int = Field(gt=0, le=2**64 - 1)
    dataset_id: int = Field(gt=0, le=2**64 - 1)
    content_type: Literal["trades", "depth"]
    inputs: list[InputSelection] = Field(min_length=1)
    output: OutputSelection | None = None

    @model_validator(mode="after")
    def unique_tasks(self) -> Self:
        if len({i.task_id for i in self.inputs}) != len(self.inputs):
            raise ValueError("task_id values must be unique.")
        selections = {(i.archive_id, i.archive_member) for i in self.inputs}
        if len(selections) != len(self.inputs):
            raise ValueError("An archive/member may be selected only once.")
        return self


def load_request(path: Path) -> PreparationRequest:
    with path.open("rb") as source:
        return PreparationRequest.model_validate(tomllib.load(source))


TEMPLATE = """# Supply positive, unreserved testing IDs before validation/preparation.
# These IDs are NOT allocated or reserved in the database.
# job_id = YOUR_POSITIVE_INTEGER
# dataset_id = YOUR_POSITIVE_INTEGER
version = 1
content_type = "trades" # trades or depth; exactly one per job

[[inputs]]
task_id = 1
# REQUIRED: registered complete LOCAL archive ID and catalog instrument ID.
# archive_id = YOUR_LOCAL_ARCHIVE_ID
# instrument_id = YOUR_INSTRUMENT_ID
# Legacy format_profile, archive_member, source_compression and stream_rank fields
# remain readable, but new preparation delegates archive interpretation to the engine
# and resolves dense ranks from the selected turnover snapshot.

# Optional output overrides (relative to project root or absolute).
# Defaults: data/test/normalized/{dataset_id} and sibling staging directory.
# Both must be absent and share a parent for atomic engine publication.
# [output]
# staging_path = "data/test/normalized/.DATASET.staging.JOB"
# dataset_path = "data/test/normalized/DATASET"
"""
