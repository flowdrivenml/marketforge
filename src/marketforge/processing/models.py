from __future__ import annotations

from decimal import Decimal
from enum import StrEnum
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, model_validator

PROCESSING_PROTOCOL_VERSION = 1


class ProcessingOperation(StrEnum):
    PROCESS = "process"
    MERGE = "merge"


class InstrumentKind(StrEnum):
    SPOT = "spot"
    PERPETUAL = "perpetual"
    FUTURE = "future"
    OPTION = "option"


class ContractKind(StrEnum):
    LINEAR = "linear"
    INVERSE = "inverse"


class TimestampEncoding(StrEnum):
    SECONDS = "seconds"
    MILLISECONDS = "milliseconds"
    MICROSECONDS = "microseconds"
    NANOSECONDS = "nanoseconds"


class QuantityEncoding(StrEnum):
    BASE = "base"
    QUOTE = "quote"
    CONTRACTS = "contracts"


class SourceOrdering(StrEnum):
    SOURCE_ORDERED = "source_ordered"
    TIMESTAMP_SORTABLE = "timestamp_sortable"


class SourceContainer(StrEnum):
    PLAIN = "plain"
    GZIP = "gzip"
    ZIP = "zip"
    TAR_GZIP = "tar_gzip"


class PrimaryOrdering(StrEnum):
    EVENT_TIMESTAMP_NS = "event_timestamp_ns"


class TieBreakOrdering(StrEnum):
    STREAM_RANK = "stream_rank"


class IntegrityAction(StrEnum):
    DEGRADE = "degrade"
    FAIL = "fail"


class IntegrityProfile(StrEnum):
    STANDARD = "standard"
    STRICT = "strict"


class ContentType(StrEnum):
    TRADES = "trades"
    DEPTH = "depth"
    COMBINED = "combined"


class InstrumentSpec(BaseModel):
    model_config = ConfigDict(frozen=True)

    instrument_kind: InstrumentKind

    contract_kind: ContractKind | None = None

    tick_size: Decimal = Field(gt=0)

    contract_value: Decimal | None = None
    contract_value_asset: str | None = None

    @model_validator(mode="after")
    def validate_contract(
        self,
    ) -> InstrumentSpec:
        if self.contract_kind is None and self.contract_value is not None:
            raise ValueError(
                "contract_value cannot be defined " "without contract_kind"
            )

        return self


class RawSchema(BaseModel):
    """
    Fully resolved description of the physical raw records.

    The structure originates from catalog.raw_formats.schema_json.
    It intentionally remains generic so new raw formats can be
    described without Python model changes.
    """

    model_config = ConfigDict(
        frozen=True,
        extra="allow",
    )

    fields: tuple[dict, ...] = ()

    header: bool | None = None

    ordering: str | None = None

    granularity: str | None = None


class NormalizationConfig(BaseModel):
    """
    Fully resolved instructions for converting raw records into
    a canonical MarketForge schema.
    """

    model_config = ConfigDict(frozen=True)

    timestamp_encoding: TimestampEncoding
    quantity_encoding: QuantityEncoding

    target_schema: str = Field(
        min_length=1,
    )

    rules: dict


class WorkTask(BaseModel):
    model_config = ConfigDict(frozen=True)

    task_id: int = Field(ge=0)

    stream_id: str = Field(
        min_length=1,
    )

    input_path: Path

    format_code: str = Field(
        min_length=1,
    )

    exchange: str = Field(
        min_length=1,
    )

    instrument_id: int

    symbol: str = Field(
        min_length=1,
    )

    raw_schema: RawSchema

    instrument: InstrumentSpec

    normalizations: tuple[NormalizationConfig, ...]

    source_ordering: SourceOrdering

    source_compression: SourceContainer

    archive_member: str | None = None

    @model_validator(mode="after")
    def validate_normalizations(
        self,
    ) -> WorkTask:
        if not self.normalizations:
            raise ValueError("Work task contains no normalization rules")

        return self


class StreamConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    stream_id: str = Field(
        min_length=1,
    )

    stream_rank: int = Field(
        ge=0,
    )


class OrderingConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    primary: PrimaryOrdering = PrimaryOrdering.EVENT_TIMESTAMP_NS

    tie_break: TieBreakOrdering = TieBreakOrdering.STREAM_RANK


class IntegrityRule(BaseModel):
    model_config = ConfigDict(frozen=True)

    action: IntegrityAction

    max_count: int | None = Field(
        default=None,
        ge=0,
    )


class IntegrityPolicy(BaseModel):
    model_config = ConfigDict(frozen=True)

    profile: IntegrityProfile

    parse_failure: IntegrityRule
    invalid_record: IntegrityRule
    sequence_gap: IntegrityRule
    timestamp_regression: IntegrityRule
    missing_snapshot: IntegrityRule
    invalid_book: IntegrityRule
    transformation_failure: IntegrityRule


class ParquetResourceConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    row_group_target_bytes: int = Field(
        gt=0,
    )

    file_target_bytes: int = Field(
        gt=0,
    )

    @model_validator(mode="after")
    def validate_targets(
        self,
    ) -> ParquetResourceConfig:
        if self.row_group_target_bytes > self.file_target_bytes:
            raise ValueError("Parquet row-group target cannot " "exceed file target")

        return self


class ResourceConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    workers: int = Field(
        gt=0,
    )

    memory_budget_bytes: int = Field(
        gt=0,
    )

    scratch_path: Path

    scratch_budget_bytes: int = Field(
        gt=0,
    )

    parquet: ParquetResourceConfig


class OutputConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    content_type: ContentType

    staging_path: Path
    dataset_path: Path

    @model_validator(mode="after")
    def validate_paths(
        self,
    ) -> OutputConfig:
        if self.staging_path == self.dataset_path:
            raise ValueError("staging_path and dataset_path " "must be different")

        return self


class ProcessingJob(BaseModel):
    """
    Immutable Python representation of the Rust
    ProcessingJob protocol.
    """

    model_config = ConfigDict(
        frozen=True,
        use_enum_values=True,
    )

    protocol_version: int = PROCESSING_PROTOCOL_VERSION

    job_id: int = Field(
        ge=0,
    )

    dataset_id: int = Field(
        ge=0,
    )

    operation: ProcessingOperation

    tasks: tuple[WorkTask, ...] = ()

    merge_inputs: tuple[MergeInput, ...] = ()

    streams: tuple[StreamConfig, ...]

    ordering: OrderingConfig

    integrity_policy: IntegrityPolicy

    resources: ResourceConfig

    output: OutputConfig

    time_range: TimeRange | None = None

    @model_validator(mode="after")
    def validate_job(
        self,
    ) -> ProcessingJob:
        if self.operation == ProcessingOperation.PROCESS:
            if not self.tasks:
                raise ValueError("Process job contains no tasks")

            if self.merge_inputs:
                raise ValueError("Process job cannot contain " "merge inputs")

        elif self.operation == ProcessingOperation.MERGE:
            if not self.merge_inputs:
                raise ValueError("Merge job contains no inputs")

            if self.tasks:
                raise ValueError("Merge job cannot contain " "raw processing tasks")

            if len(self.merge_inputs) < 2:
                raise ValueError("Merge job requires at least " "two inputs")

        if not self.streams:
            raise ValueError("Processing job contains no streams")

        task_ids = [task.task_id for task in self.tasks]

        if len(task_ids) != len(set(task_ids)):
            raise ValueError("Processing job contains " "duplicate task IDs")

        merge_dataset_ids = [item.dataset_id for item in self.merge_inputs]

        if len(merge_dataset_ids) != len(set(merge_dataset_ids)):
            raise ValueError("Merge job contains duplicate " "dataset inputs")

        stream_ids = [stream.stream_id for stream in self.streams]

        if len(stream_ids) != len(set(stream_ids)):
            raise ValueError("Processing job contains " "duplicate stream IDs")

        stream_ranks = [stream.stream_rank for stream in self.streams]

        if len(stream_ranks) != len(set(stream_ranks)):
            raise ValueError("Processing job contains " "duplicate stream ranks")

        if self.operation == ProcessingOperation.MERGE and self.time_range is None:
            raise ValueError("Merge job requires a resolved time range")
        if (
            self.operation == ProcessingOperation.PROCESS
            and self.time_range is not None
        ):
            raise ValueError("Process job cannot define merge " "time-range clipping")
        known_streams = set(stream_ids)

        for task in self.tasks:
            if task.stream_id not in known_streams:
                raise ValueError(
                    f"Task {task.task_id} references "
                    f"unknown stream "
                    f"{task.stream_id!r}"
                )

        for merge_input in self.merge_inputs:
            if merge_input.stream_id not in known_streams:
                raise ValueError(
                    "Merge input "
                    f"{merge_input.dataset_id} "
                    "references unknown stream "
                    f"{merge_input.stream_id!r}"
                )

        return self


class MergeInput(BaseModel):
    """
    One canonical dataset consumed by a merge operation.

    Unlike WorkTask, this input is already normalized canonical
    Parquet and therefore requires no raw-format or normalization
    metadata.
    """

    model_config = ConfigDict(frozen=True)

    dataset_id: int = Field(
        gt=0,
    )

    stream_id: str = Field(
        min_length=1,
    )

    input_path: Path

    data_type: str = Field(
        pattern=r"^(trade|l2|trade_l2)$",
    )

    start_timestamp_ns: int
    end_timestamp_ns: int

    @model_validator(mode="after")
    def validate_range(
        self,
    ) -> MergeInput:
        if self.end_timestamp_ns <= self.start_timestamp_ns:
            raise ValueError(
                "Merge input end timestamp must " "be greater than start timestamp"
            )

        return self


class TimeRange(BaseModel):
    model_config = ConfigDict(frozen=True)

    start_timestamp_ns: int
    end_timestamp_ns: int

    @model_validator(mode="after")
    def validate_range(
        self,
    ) -> TimeRange:
        if self.end_timestamp_ns <= self.start_timestamp_ns:
            raise ValueError("Time range end must be greater " "than start")

        return self
