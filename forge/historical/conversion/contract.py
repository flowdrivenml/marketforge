"""Versioned standalone execution contract shared with marketforge-convert."""

from pathlib import Path
from typing import Annotated, Literal, Self

from pydantic import Field, model_validator

from forge.shared.config.models import SettingsModel

UInt = Annotated[int, Field(gt=0, lt=2**64)]
Currency = Annotated[str, Field(pattern=r"^[A-Z0-9]+$")]
Kind = Literal["spot", "perpetual", "future", "option", "equity"]


class Identity(SettingsModel, frozen=True):
    exchange: str = Field(pattern=r"\S")
    symbol: str = Field(pattern=r"\S")
    instrument_kind: Kind
    contract_kind: Literal["linear", "inverse"] | None = None


class Resources(SettingsModel, frozen=True):
    workers: UInt = 1
    memory_limit_mb: UInt = 1024
    max_open_files: UInt = 64


class Policy(SettingsModel, frozen=True):
    target_quote: Currency
    price_source: Literal["last_trade"] = "last_trade"
    max_staleness_ns: UInt
    max_hops: int = Field(default=3, ge=1, le=4)
    path_policy: Literal["all", "primary"] = "all"
    instrument_kinds: list[Kind] | None = Field(default=None, min_length=1)

    @model_validator(mode="after")
    def unique_kinds(self) -> Self:
        if self.instrument_kinds and len(set(self.instrument_kinds)) != len(self.instrument_kinds):
            raise ValueError("Instrument kinds must be unique.")
        return self


class Conversion(Policy, frozen=True):
    references: list[Identity]
    reference_merge: str | None

    @model_validator(mode="after")
    def unique_references(self) -> Self:
        if len(set(self.references)) != len(self.references):
            raise ValueError("Reference instruments must be unique.")
        return self


class Output(SettingsModel, frozen=True):
    dataset_path: str


Digest = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]


class Parquet(SettingsModel, frozen=True):
    row_group_target_mb: UInt
    file_target_mb: UInt
    compression_level: int = Field(ge=1, le=22)


class NormalizationEvidence(SettingsModel, frozen=True):
    dataset_id: UInt
    manifest_sha256: Digest | None
    processing_job_sha256: Digest


class StreamBinding(SettingsModel, frozen=True):
    dataset_id: UInt
    stream_id: str = Field(pattern=r"\S")
    stream_rank: int = Field(ge=0, lt=2**32)
    kind: Literal["trade", "depth"]
    instrument_id: int = Field(gt=0, lt=2**63)
    instrument_namespace: str = Field(pattern=r"\S")
    identity: Identity
    base_currency: Currency
    quote_currency: Currency


class PublicationBinding(SettingsModel, frozen=True):
    merge_job_id: UInt
    manifest_sha256: Digest
    publication_checksums_sha256: Digest
    normalized_inputs: list[NormalizationEvidence] = Field(min_length=1)
    streams: list[StreamBinding] = Field(min_length=1)


class Job(SettingsModel, frozen=True):
    schema_version: Literal[1]
    source: PublicationBinding
    external_reference: PublicationBinding | None
    catalog_sha256: Digest
    parquet: Parquet
    job_id: UInt
    operation: Literal["convert"]
    input_merge: str
    conversion: Conversion
    resources: Resources
    output: Output

    @model_validator(mode="after")
    def absolute_paths(self) -> Self:
        source = Path(self.input_merge)
        output = Path(self.output.dataset_path)
        ref = self.conversion.reference_merge
        if (
            not source.is_absolute()
            or not output.is_absolute()
            or source.resolve() == output.resolve()
            or ref is not None
            and (not Path(ref).is_absolute() or Path(ref).resolve() == output.resolve())
        ):
            raise ValueError("Job paths must be absolute and output must differ from its inputs.")
        if (ref is None) != (self.external_reference is None):
            raise ValueError(
                "External reference path and publication binding must both be present."
            )
        return self


def build_job(
    job_id: int,
    source: Path,
    output: Path,
    policy: Policy,
    resources: Resources,
    references: list[Identity],
    reference: Path | None,
    *,
    source_binding: PublicationBinding,
    external_binding: PublicationBinding | None,
    catalog_sha256: str,
    parquet: Parquet,
) -> Job:
    return Job(
        schema_version=1,
        source=source_binding,
        external_reference=external_binding,
        catalog_sha256=catalog_sha256,
        parquet=parquet,
        job_id=job_id,
        operation="convert",
        input_merge=str(source),
        conversion=Conversion(
            **policy.model_dump(),
            references=references,
            reference_merge=str(reference) if reference is not None else None,
        ),
        resources=resources,
        output=Output(dataset_path=str(output)),
    )
