from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from pathlib import Path

from marketforge.processing.archives import RawArchive
from marketforge.processing.metadata import NormalizationMetadata, ProcessingMetadata
from marketforge.processing.models import (
    ContentType,
    ContractKind,
    InstrumentKind,
    InstrumentSpec,
    IntegrityAction,
    IntegrityPolicy,
    IntegrityProfile,
    IntegrityRule,
    NormalizationConfig,
    OrderingConfig,
    OutputConfig,
    ParquetResourceConfig,
    ProcessingJob,
    ProcessingOperation,
    QuantityEncoding,
    RawSchema,
    ResourceConfig,
    SourceContainer,
    SourceOrdering,
    StreamConfig,
    TimestampEncoding,
    WorkTask,
)
from marketforge.processing.serialization import write_processing_job


@dataclass(frozen=True)
class ProcessDatasetPlan:
    """
    Metadata required to register the canonical output dataset
    before the Rust processing job is created.
    """

    exchange_id: int
    instrument_id: int

    data_type: str

    start_timestamp_ns: int
    end_timestamp_ns: int


DATASET_MAP = {
    "trade_ticks": "trade",
    "order_book_l2": "l2",
}


def _build_raw_schema(
    metadata: ProcessingMetadata,
) -> RawSchema:
    """
    Preserve the complete resolved raw-format schema in the job.
    """

    return RawSchema.model_validate(metadata.raw_format.schema)


def build_process_job(
    *,
    job_id: int,
    dataset_id: int,
    archives: Sequence[RawArchive],
    metadata: ProcessingMetadata,
    data_root: Path = Path("data"),
) -> ProcessingJob:
    """
    Construct one immutable Rust-compatible processing job.

    All selected archives belong to one logical output dataset.
    Each physical archive becomes one independent WorkTask.
    """

    if not archives:
        raise ValueError("Cannot build processing job without archives")

    _validate_archives(
        archives=archives,
        metadata=metadata,
    )

    stream_id = _stream_id(metadata)

    tasks = tuple(
        _build_task(
            task_id=index,
            stream_id=stream_id,
            archive=archive,
            metadata=metadata,
        )
        for index, archive in enumerate(archives)
    )

    streams = (
        StreamConfig(
            stream_id=stream_id,
            stream_rank=0,
        ),
    )

    resources = _build_resources(
        metadata,
        data_root=data_root,
    )

    integrity_policy = _build_integrity_policy(metadata)

    output = _build_output(
        dataset_id=dataset_id,
        metadata=metadata,
        data_root=data_root,
    )

    return ProcessingJob(
        job_id=job_id,
        dataset_id=dataset_id,
        operation=ProcessingOperation.PROCESS,
        tasks=tasks,
        streams=streams,
        ordering=OrderingConfig(),
        integrity_policy=integrity_policy,
        resources=resources,
        output=output,
    )


def _build_task(
    *,
    task_id: int,
    stream_id: str,
    archive: RawArchive,
    metadata: ProcessingMetadata,
) -> WorkTask:
    return WorkTask(
        task_id=task_id,
        stream_id=stream_id,
        input_path=archive.path.resolve(),
        format_code=(metadata.raw_format.format_code),
        exchange=(metadata.instrument.exchange),
        instrument_id=(metadata.instrument.id),
        symbol=(metadata.instrument.symbol),
        raw_schema=_build_raw_schema(metadata),
        instrument=_build_instrument_spec(metadata),
        normalizations=_build_normalizations(metadata),
        source_ordering=_source_ordering(metadata),
        source_compression=_source_container(metadata),
        archive_member=None,
    )


def _build_instrument_spec(
    metadata: ProcessingMetadata,
) -> InstrumentSpec:
    instrument = metadata.instrument

    instrument_kind = InstrumentKind(instrument.instrument_type)

    contract_kind = None

    if instrument.market_category in {
        "linear",
        "inverse",
    }:
        contract_kind = ContractKind(instrument.market_category)

    return InstrumentSpec(
        instrument_kind=instrument_kind,
        contract_kind=contract_kind,
        tick_size=instrument.tick_size,
        contract_value=instrument.contract_value,
        contract_value_asset=(instrument.contract_value_asset),
    )


def _build_normalizations(
    metadata: ProcessingMetadata,
) -> tuple[NormalizationConfig, ...]:
    return tuple(
        NormalizationConfig(
            timestamp_encoding=(
                _timestamp_encoding(
                    metadata,
                    normalization,
                )
            ),
            quantity_encoding=(_quantity_encoding(metadata)),
            target_schema=(normalization.target_schema),
            rules=normalization.rules,
        )
        for normalization in metadata.normalizations
    )


def _timestamp_encoding(
    metadata: ProcessingMetadata,
    normalization: NormalizationMetadata,
) -> TimestampEncoding:
    """
    Resolve timestamp encoding from raw-format schema metadata.
    """

    fields = metadata.raw_format.schema.get("fields", [])

    timestamp_source = normalization.rules.get(
        "event_timestamp",
        {},
    ).get("source")

    if timestamp_source is None:
        raise ValueError("Normalization rules do not define " "event_timestamp source")

    field = next(
        (field for field in fields if field.get("name") == timestamp_source),
        None,
    )

    if field is None:
        raise ValueError(
            "Timestamp source field not found " f"in raw schema: {timestamp_source}"
        )

    unit = field.get("unit")

    mapping = {
        "seconds": TimestampEncoding.SECONDS,
        "milliseconds": (TimestampEncoding.MILLISECONDS),
        "microseconds": (TimestampEncoding.MICROSECONDS),
        "nanoseconds": (TimestampEncoding.NANOSECONDS),
    }

    try:
        return mapping[unit]
    except KeyError as exc:
        raise ValueError("Unsupported timestamp encoding: " f"{unit!r}") from exc


def _quantity_encoding(
    metadata: ProcessingMetadata,
) -> QuantityEncoding:
    mapping = {
        "base": QuantityEncoding.BASE,
        "quote": QuantityEncoding.QUOTE,
        "contracts": (QuantityEncoding.CONTRACTS),
    }

    value = metadata.instrument.quantity_type

    try:
        return mapping[value]
    except KeyError as exc:
        raise ValueError("Unsupported quantity encoding: " f"{value!r}") from exc


def _source_ordering(
    metadata: ProcessingMetadata,
) -> SourceOrdering:
    ordering = metadata.raw_format.schema.get("ordering")

    if ordering == "chronological":
        return SourceOrdering.SOURCE_ORDERED

    return SourceOrdering.TIMESTAMP_SORTABLE


def _source_container(
    metadata: ProcessingMetadata,
) -> SourceContainer:
    container = metadata.raw_format.container_format
    compression = metadata.raw_format.compression

    if compression in {"tar.gz", "tgz"}:
        return SourceContainer.TAR_GZIP

    if compression == "zip":
        return SourceContainer.ZIP

    if compression == "gzip":
        return SourceContainer.GZIP

    if compression is None or compression == "none":
        return SourceContainer.PLAIN

    raise ValueError(
        "Unsupported source container: "
        f"container={container!r}, "
        f"compression={compression!r}"
    )


def _build_resources(
    metadata: ProcessingMetadata,
    *,
    data_root: Path,
) -> ResourceConfig:
    profile = metadata.profile

    scratch_path = profile.scratch_path

    if not scratch_path.is_absolute():
        if scratch_path.parts[:1] == ("data",):
            scratch_path = data_root.parent / scratch_path
        else:
            scratch_path = data_root / scratch_path

    return ResourceConfig(
        workers=profile.workers,
        memory_budget_bytes=(profile.memory_budget_bytes),
        scratch_path=scratch_path.resolve(),
        scratch_budget_bytes=(profile.scratch_budget_bytes),
        parquet=ParquetResourceConfig(
            row_group_target_bytes=(profile.parquet_row_group_target_bytes),
            file_target_bytes=(profile.parquet_file_target_bytes),
        ),
    )


def _build_integrity_policy(
    metadata: ProcessingMetadata,
) -> IntegrityPolicy:
    profile = metadata.profile.integrity_profile

    if profile == "strict":
        return _strict_integrity_policy()

    if profile == "standard":
        return _standard_integrity_policy()

    raise ValueError("Unsupported integrity profile: " f"{profile!r}")


def _standard_integrity_policy() -> IntegrityPolicy:
    degrade = IntegrityRule(
        action=IntegrityAction.DEGRADE,
    )

    fail = IntegrityRule(
        action=IntegrityAction.FAIL,
    )

    return IntegrityPolicy(
        profile=IntegrityProfile.STANDARD,
        parse_failure=degrade,
        invalid_record=degrade,
        sequence_gap=degrade,
        timestamp_regression=degrade,
        missing_snapshot=fail,
        invalid_book=fail,
        transformation_failure=fail,
    )


def _strict_integrity_policy() -> IntegrityPolicy:
    fail = IntegrityRule(
        action=IntegrityAction.FAIL,
    )

    return IntegrityPolicy(
        profile=IntegrityProfile.STRICT,
        parse_failure=fail,
        invalid_record=fail,
        sequence_gap=fail,
        timestamp_regression=fail,
        missing_snapshot=fail,
        invalid_book=fail,
        transformation_failure=fail,
    )


def _build_output(
    *,
    dataset_id: int,
    metadata: ProcessingMetadata,
    data_root: Path,
) -> OutputConfig:
    content_type = _content_type(metadata)

    dataset_path = (data_root / "processed" / str(dataset_id)).resolve()

    staging_path = (data_root / ".staging" / f"dataset-{dataset_id}").resolve()

    return OutputConfig(
        content_type=content_type,
        staging_path=staging_path,
        dataset_path=dataset_path,
    )


def _content_type(
    metadata: ProcessingMetadata,
) -> ContentType:
    targets = {normalization.target_schema for normalization in metadata.normalizations}

    if targets == {"trade"}:
        return ContentType.TRADES

    if targets and targets <= {
        "l2",
        "l2_snapshot",
        "l2_update",
    }:
        return ContentType.DEPTH

    raise ValueError("Unsupported canonical target schemas: " f"{sorted(targets)!r}")


def _stream_id(
    metadata: ProcessingMetadata,
) -> str:
    instrument = metadata.instrument
    raw_format = metadata.raw_format

    return ":".join(
        (
            instrument.exchange,
            instrument.instrument_type,
            instrument.market_category,
            instrument.symbol,
            raw_format.dataset,
        )
    )


def _validate_archives(
    *,
    archives: Sequence[RawArchive],
    metadata: ProcessingMetadata,
) -> None:
    expected_dataset = metadata.raw_format.dataset

    for archive in archives:
        if archive.exchange != metadata.instrument.exchange:
            raise ValueError(
                "Archive exchange does not match "
                "resolved metadata: "
                f"{archive.path}"
            )

        if archive.instrument_type != metadata.instrument.instrument_type:
            raise ValueError(
                "Archive instrument type does not "
                "match resolved metadata: "
                f"{archive.path}"
            )

        if archive.market_category != metadata.instrument.market_category:
            raise ValueError(
                "Archive market category does not "
                "match resolved metadata: "
                f"{archive.path}"
            )

        if archive.symbol != metadata.instrument.symbol:
            raise ValueError(
                "Archive symbol does not match " "resolved metadata: " f"{archive.path}"
            )

        archive_dataset = DATASET_MAP.get(archive.data_type)

        if archive_dataset is None:
            raise ValueError(
                "Unsupported raw archive data type: " f"{archive.data_type!r}"
            )

        if archive_dataset != expected_dataset:
            raise ValueError(
                "Archive dataset does not match "
                "resolved raw format: "
                f"{archive.data_type!r} "
                f"-> {archive_dataset!r}, "
                f"expected {expected_dataset!r}"
            )


def write_process_job(
    *,
    job_id: int,
    dataset_id: int,
    archives: Sequence[RawArchive],
    metadata: ProcessingMetadata,
    data_root: Path = Path("data"),
) -> Path:
    """
    Build and persist the current immutable process ProcessingJob.

    The process job is written to:

        data/.jobs/process.json

    Existing process.json is replaced by the new job.
    """

    job = build_process_job(
        job_id=job_id,
        dataset_id=dataset_id,
        archives=archives,
        metadata=metadata,
        data_root=data_root,
    )

    filename = _process_job_filename(
        dataset_id=dataset_id,
        archives=archives,
        metadata=metadata,
    )

    job_path = data_root / ".jobs" / "process" / filename

    return write_processing_job(
        job,
        job_path,
    )


def plan_process_dataset(
    *,
    archives: Sequence[RawArchive],
    metadata: ProcessingMetadata,
) -> ProcessDatasetPlan:
    """
    Resolve canonical dataset metadata from selected raw archives.

    Archive date ranges are converted to a half-open UTC
    nanosecond interval:

        [start_timestamp_ns, end_timestamp_ns)

    Monthly and daily archives therefore produce one contiguous
    logical dataset range covering the selected archives.
    """

    if not archives:
        raise ValueError("Cannot plan process dataset without archives")

    _validate_archives(
        archives=archives,
        metadata=metadata,
    )

    dated = [
        archive
        for archive in archives
        if (archive.start_date is not None and archive.end_date is not None)
    ]

    if len(dated) != len(archives):
        missing = [
            archive.filename
            for archive in archives
            if (archive.start_date is None or archive.end_date is None)
        ]

        values = ", ".join(missing)

        raise ValueError(
            "Cannot resolve dataset time range from " f"archive filename(s): {values}"
        )

    start_date = min(archive.start_date for archive in dated)

    end_date = max(archive.end_date for archive in dated)

    start = datetime.combine(
        start_date,
        time.min,
        tzinfo=timezone.utc,
    )

    # RawArchive.end_date is inclusive.
    # Canonical dataset ranges are half-open.
    end = datetime.combine(
        end_date + timedelta(days=1),
        time.min,
        tzinfo=timezone.utc,
    )

    return ProcessDatasetPlan(
        instrument_id=metadata.instrument.id,
        data_type=_dataset_data_type(metadata),
        start_timestamp_ns=_datetime_to_ns(start),
        end_timestamp_ns=_datetime_to_ns(end),
        exchange_id=metadata.instrument.exchange_id,
    )


def _dataset_data_type(
    metadata: ProcessingMetadata,
) -> str:
    targets = {normalization.target_schema for normalization in metadata.normalizations}

    if targets == {"trade"}:
        return "trade"

    if targets and targets <= {
        "l2",
        "l2_snapshot",
        "l2_update",
    }:
        return "l2"

    raise ValueError("Unsupported canonical target schemas: " f"{sorted(targets)!r}")


def _datetime_to_ns(
    value: datetime,
) -> int:
    """
    Convert an aware UTC datetime to Unix epoch nanoseconds
    without floating-point timestamp conversion.
    """

    epoch = datetime(
        1970,
        1,
        1,
        tzinfo=timezone.utc,
    )

    delta = value - epoch

    return (
        delta.days * 86_400_000_000_000
        + delta.seconds * 1_000_000_000
        + delta.microseconds * 1_000
    )


def _process_job_filename(
    *,
    dataset_id: int,
    archives: Sequence[RawArchive],
    metadata: ProcessingMetadata,
) -> str:
    """
    Build a deterministic, human-readable process-job filename.

    Format:

        {exchange}-{instrument_type}-{market_category}-{symbol}
        -{data_type}-{start}-{end}-d{dataset_id}.json

    Dates represent the canonical half-open dataset interval [start, end).
    """

    plan = plan_process_dataset(
        archives=archives,
        metadata=metadata,
    )

    start = _ns_to_date(plan.start_timestamp_ns)
    end = _ns_to_date(plan.end_timestamp_ns)

    instrument = metadata.instrument

    return (
        f"{instrument.exchange}-"
        f"{instrument.instrument_type}-"
        f"{instrument.market_category}-"
        f"{instrument.symbol}-"
        f"{plan.data_type}-"
        f"{start:%Y%m%d}-"
        f"{end:%Y%m%d}-"
        f"d{dataset_id}.json"
    )


def _ns_to_date(
    value: int,
) -> datetime:
    return datetime.fromtimestamp(
        value / 1_000_000_000,
        tz=timezone.utc,
    )
