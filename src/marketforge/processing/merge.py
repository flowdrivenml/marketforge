from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Iterable

from psycopg import Connection

from marketforge.config.repository import ConfigRepository
from marketforge.market_metrics import MarketMetricsService
from marketforge.processing.datasets import DatasetRepository, ResolvedDataset
from marketforge.processing.models import (
    ContentType,
    IntegrityAction,
    IntegrityPolicy,
    IntegrityProfile,
    IntegrityRule,
    MergeInput,
    OrderingConfig,
    OutputConfig,
    ParquetResourceConfig,
    ProcessingJob,
    ProcessingOperation,
    ResourceConfig,
    StreamConfig,
    TimeRange,
)
from marketforge.processing.serialization import write_processing_job

CANONICAL_DATA_TYPES = {
    "trade",
    "l2",
    "trade_l2",
}


@dataclass(frozen=True)
class RankedMergeInput:
    """One canonical dataset participating in a merge."""

    dataset: ResolvedDataset

    stream_id: str
    stream_rank: int


@dataclass(frozen=True)
class MergePlan:
    """Fully resolved and validated merge plan."""

    inputs: tuple[RankedMergeInput, ...]

    data_type: str

    start_timestamp_ns: int
    end_timestamp_ns: int


class MergePlanner:
    """
    Resolve and validate canonical datasets for one merge.

    Responsibilities:

        - resolve selected datasets
        - validate dataset state
        - validate instrument compatibility
        - resolve output data type
        - resolve common/clipped time range
        - assign deterministic stream ranks

    This planner does not mutate PostgreSQL, create output
    datasets, write job JSON, or invoke Rust.
    """

    def __init__(
        self,
        conn: Connection,
    ) -> None:
        self.conn = conn

        self.datasets = DatasetRepository(conn)

        self.metrics = MarketMetricsService(conn)

    def plan(
        self,
        dataset_ids: Iterable[int],
        *,
        start_timestamp_ns: int | None = None,
        end_timestamp_ns: int | None = None,
    ) -> MergePlan:
        ids = list(dict.fromkeys(dataset_ids))

        if len(ids) < 2:
            raise ValueError("Merge requires at least two " "different datasets")

        datasets = self.datasets.get_resolved_many(ids)

        leaves = self.datasets.resolve_leaf_datasets(ids)

        self._validate_all_found(
            requested_ids=ids,
            datasets=datasets,
        )

        self._validate_dataset_state(datasets)

        self._validate_instruments(leaves)

        data_type = self._resolve_data_type(datasets)

        (
            resolved_start,
            resolved_end,
        ) = self._resolve_time_range(
            datasets,
            requested_start=start_timestamp_ns,
            requested_end=end_timestamp_ns,
        )

        ranks = self._rank_datasets(datasets)

        inputs = tuple(
            sorted(
                (
                    RankedMergeInput(
                        dataset=dataset,
                        stream_id=self._stream_id(dataset),
                        stream_rank=ranks[dataset.id],
                    )
                    for dataset in datasets
                ),
                key=lambda item: (
                    item.stream_rank,
                    item.dataset.id,
                ),
            )
        )

        return MergePlan(
            inputs=inputs,
            data_type=data_type,
            start_timestamp_ns=resolved_start,
            end_timestamp_ns=resolved_end,
        )

    @staticmethod
    def _validate_all_found(
        *,
        requested_ids: list[int],
        datasets: list[ResolvedDataset],
    ) -> None:
        found = {dataset.id for dataset in datasets}

        missing = set(requested_ids) - found

        if not missing:
            return

        values = ", ".join(str(dataset_id) for dataset_id in sorted(missing))

        raise LookupError("Dataset(s) not found: " f"{values}")

    @staticmethod
    def _validate_dataset_state(
        datasets: list[ResolvedDataset],
    ) -> None:
        """
        Merge inputs must be complete canonical Parquet datasets
        with published storage paths.
        """

        for dataset in datasets:
            if dataset.status != "complete":
                raise ValueError(
                    f"Dataset {dataset.id} is "
                    f"{dataset.status!r}; only complete "
                    "datasets can be merged"
                )

            if dataset.storage_format != "parquet":
                raise ValueError(
                    f"Dataset {dataset.id} uses "
                    "unsupported storage format "
                    f"{dataset.storage_format!r}"
                )

            if dataset.storage_path is None:
                raise ValueError(f"Dataset {dataset.id} has no " "storage path")

            if dataset.data_type not in CANONICAL_DATA_TYPES:
                raise ValueError(
                    f"Dataset {dataset.id} has "
                    "unsupported canonical data type "
                    f"{dataset.data_type!r}"
                )

    @staticmethod
    def _validate_instruments(
        datasets: list[ResolvedDataset],
    ) -> None:
        """
        Validate economic compatibility using recursively resolved
        leaf datasets.
        """

        if not datasets:
            raise ValueError("Merge contains no underlying " "instrument datasets")

        for dataset in datasets:
            if dataset.instrument_id is None:
                raise RuntimeError(
                    "Resolved leaf dataset has no " f"instrument: {dataset.id}"
                )

            if dataset.instrument_type is None:
                raise RuntimeError(
                    "Resolved leaf dataset has no " f"instrument type: {dataset.id}"
                )

            if dataset.market_category is None:
                raise RuntimeError(
                    "Resolved leaf dataset has no " f"market category: {dataset.id}"
                )

        instrument_types = {dataset.instrument_type for dataset in datasets}

        market_categories = {dataset.market_category for dataset in datasets}

        if "option" in instrument_types or "option" in market_categories:
            raise ValueError("Options merging is postponed")

        has_spot = "spot" in market_categories

        has_contract = bool(
            market_categories
            & {
                "linear",
                "inverse",
            }
        )

        if has_spot and has_contract:
            raise ValueError("Spot and contract datasets " "cannot be merged together")

        if has_spot:
            quote_assets = {dataset.quote_asset for dataset in datasets}

            if None in quote_assets:
                raise ValueError(
                    "Spot merge contains an " "instrument without quote asset"
                )

            if len(quote_assets) != 1:
                values = ", ".join(sorted(str(value) for value in quote_assets))

                raise ValueError(
                    "Spot datasets require a common "
                    "quote denominator; received: "
                    f"{values}"
                )

    @staticmethod
    def _resolve_data_type(
        datasets: list[ResolvedDataset],
    ) -> str:
        """
        Resolve the canonical output type.

        trade + trade
            -> trade

        l2 + l2
            -> l2

        any mixture involving trade and l2
            -> trade_l2

        trade_l2 combined with anything
            -> trade_l2
        """

        data_types = {dataset.data_type for dataset in datasets}

        if data_types == {"trade"}:
            return "trade"

        if data_types == {"l2"}:
            return "l2"

        if data_types == {"trade_l2"}:
            return "trade_l2"

        if "trade_l2" in data_types:
            return "trade_l2"

        if data_types == {
            "trade",
            "l2",
        }:
            return "trade_l2"

        values = ", ".join(sorted(data_types))

        raise ValueError("Unsupported canonical data-type " f"combination: {values}")

    @staticmethod
    def _resolve_time_range(
        datasets: list[ResolvedDataset],
        *,
        requested_start: int | None,
        requested_end: int | None,
    ) -> tuple[int, int]:
        """
        Resolve the interval shared by every selected dataset.

        Explicit clipping may narrow this interval but may not
        extend beyond it.
        """

        common_start = max(dataset.start_timestamp_ns for dataset in datasets)

        common_end = min(dataset.end_timestamp_ns for dataset in datasets)

        if common_start >= common_end:
            raise ValueError("Selected datasets have no common " "time range")

        start = requested_start if requested_start is not None else common_start

        end = requested_end if requested_end is not None else common_end

        if start < common_start:
            raise ValueError(
                "Requested merge start precedes " "the common dataset range"
            )

        if end > common_end:
            raise ValueError("Requested merge end exceeds " "the common dataset range")

        if start >= end:
            raise ValueError("Merge time range is empty")

        return (
            start,
            end,
        )

    def _rank_datasets(
        self,
        datasets: list[ResolvedDataset],
    ) -> dict[int, int]:
        """
        Rank selected physical input datasets.

        Single-instrument dataset:
            use its instrument turnover.

        Previously merged dataset:
            recursively resolve its leaf instruments and sum the
            current turnover of unique underlying instruments.

        Final tie-break:
            dataset ID ASC.
        """

        turnover_by_dataset: dict[
            int,
            Decimal | None,
        ] = {}

        for dataset in datasets:
            leaves = self.datasets.resolve_leaf_datasets([dataset.id])

            instrument_ids = {
                leaf.instrument_id for leaf in leaves if leaf.instrument_id is not None
            }

            metrics = self.metrics.get_many(instrument_ids)

            turnovers = {
                metric.instrument_id: metric.turnover_24h
                for metric in metrics
                if metric.turnover_24h is not None
            }

            values = [
                turnovers[instrument_id]
                for instrument_id in instrument_ids
                if instrument_id in turnovers
            ]

            turnover_by_dataset[dataset.id] = (
                sum(
                    values,
                    Decimal(0),
                )
                if values
                else None
            )

        def key(
            dataset: ResolvedDataset,
        ) -> tuple[
            bool,
            Decimal,
            int,
        ]:
            turnover = turnover_by_dataset[dataset.id]

            if turnover is None:
                return (
                    True,
                    Decimal(0),
                    dataset.id,
                )

            return (
                False,
                -turnover,
                dataset.id,
            )

        ordered = sorted(
            datasets,
            key=key,
        )

        return {dataset.id: rank for rank, dataset in enumerate(ordered)}

    @staticmethod
    def _stream_id(
        dataset: ResolvedDataset,
    ) -> str:
        """
        Dataset ID provides stable unique stream identity.
        """

        return f"dataset:{dataset.id}"


def build_merge_job(
    *,
    job_id: int,
    dataset_id: int,
    plan: MergePlan,
    conn: Connection,
    profile_name: str = "default",
    data_root: Path = Path("data"),
) -> ProcessingJob:
    """
    Convert a fully resolved MergePlan into one immutable
    merge ProcessingJob.
    """

    config = ConfigRepository(conn)

    profile = config.get_processing_profile(profile_name)

    if profile is None:
        raise LookupError("Processing profile not found: " f"{profile_name}")

    merge_inputs = tuple(
        MergeInput(
            dataset_id=item.dataset.id,
            stream_id=item.stream_id,
            input_path=(item.dataset.storage_path),
            data_type=(item.dataset.data_type),
            start_timestamp_ns=(item.dataset.start_timestamp_ns),
            end_timestamp_ns=(item.dataset.end_timestamp_ns),
        )
        for item in plan.inputs
    )

    streams = tuple(
        StreamConfig(
            stream_id=item.stream_id,
            stream_rank=item.stream_rank,
        )
        for item in plan.inputs
    )

    resources = _build_merge_resources(
        profile=profile,
        data_root=data_root,
    )

    integrity_policy = _build_merge_integrity_policy(profile.integrity_profile)

    output = OutputConfig(
        content_type=_merge_content_type(plan.data_type),
        staging_path=(data_root / ".staging" / f"dataset-{dataset_id}").resolve(),
        dataset_path=(data_root / "processed" / str(dataset_id)).resolve(),
    )

    return ProcessingJob(
        job_id=job_id,
        dataset_id=dataset_id,
        operation=(ProcessingOperation.MERGE),
        tasks=(),
        merge_inputs=merge_inputs,
        streams=streams,
        ordering=OrderingConfig(),
        time_range=TimeRange(
            start_timestamp_ns=(plan.start_timestamp_ns),
            end_timestamp_ns=(plan.end_timestamp_ns),
        ),
        integrity_policy=(integrity_policy),
        resources=resources,
        output=output,
    )


def _build_merge_resources(
    *,
    profile,
    data_root: Path,
) -> ResourceConfig:
    scratch_path = profile.scratch_path

    if not scratch_path.is_absolute():
        if scratch_path.parts[:1] == ("data",):
            scratch_path = data_root.parent / scratch_path
        else:
            scratch_path = data_root / scratch_path

    return ResourceConfig(
        workers=profile.workers,
        memory_budget_bytes=(profile.memory_budget_bytes),
        scratch_path=(scratch_path.resolve()),
        scratch_budget_bytes=(profile.scratch_budget_bytes),
        parquet=ParquetResourceConfig(
            row_group_target_bytes=(profile.parquet_row_group_target_bytes),
            file_target_bytes=(profile.parquet_file_target_bytes),
        ),
    )


def _merge_content_type(
    data_type: str,
) -> ContentType:
    if data_type == "trade":
        return ContentType.TRADES

    if data_type == "l2":
        return ContentType.DEPTH

    if data_type == "trade_l2":
        return ContentType.COMBINED

    raise ValueError("Unsupported canonical merge " f"data type: {data_type!r}")


def _build_merge_integrity_policy(
    profile: str,
) -> IntegrityPolicy:
    if profile == "strict":
        fail = IntegrityRule(action=IntegrityAction.FAIL)

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

    if profile == "standard":
        degrade = IntegrityRule(action=IntegrityAction.DEGRADE)

        fail = IntegrityRule(action=IntegrityAction.FAIL)

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

    raise ValueError("Unsupported integrity profile: " f"{profile!r}")


def write_merge_job(
    *,
    job_id: int,
    dataset_id: int,
    plan: MergePlan,
    conn: Connection,
    profile_name: str = "default",
    data_root: Path = Path("data"),
) -> Path:
    """
    Build and persist the current immutable merge ProcessingJob.

    The merge job is written to:

        data/.jobs/merge.json

    Existing merge.json is replaced by the new job.
    """

    job = build_merge_job(
        job_id=job_id,
        dataset_id=dataset_id,
        plan=plan,
        conn=conn,
        profile_name=profile_name,
        data_root=data_root,
    )

    job_path = data_root / ".jobs" / "merge.json"

    return write_processing_job(
        job,
        job_path,
    )
