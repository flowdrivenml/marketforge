"""Single-job JSON accepted by the current marketforge-convert loader."""

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


class Job(SettingsModel, frozen=True):
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
        return self


def build_job(
    job_id: int,
    source: Path,
    output: Path,
    policy: Policy,
    resources: Resources,
    references: list[Identity],
    reference: Path | None,
) -> Job:
    return Job(
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
