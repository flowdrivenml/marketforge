"""Physical locators and exact queries shared by research and registry inspection."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

from . import parquet
from .dataset import Dataset, Rows
from .metadata import content

COLUMN_MEANINGS = {
    "event_timestamp_ns": "Market event time in Unix nanoseconds",
    "system_timestamp_ns": "Recorded system time in Unix nanoseconds",
    "instrument_id": "Historical instrument ID in the publication namespace",
    "stream_id": "Logical stream identity",
    "event_kind": "Combined discriminator: 0 trade, 1 depth",
    "timeline_sequence": "Physical causal sequence",
    "reconstruction_only": "Retained for reconstruction rather than market coverage",
    "target_quote": "Target valuation currency",
}


def schema_fields(source: Rows) -> list[dict[str, object]]:
    return [
        {
            "name": field.name,
            "type": str(field.type),
            "nullable": field.nullable,
            "meaning": COLUMN_MEANINGS.get(field.name),
        }
        for field in source.schema()
    ]


@dataclass(frozen=True)
class Selection(Rows):
    footers: tuple[parquet.Footer, ...]
    predicates: tuple[parquet.Predicate, ...] = ()
    omitted: tuple[str, ...] = ()
    groups: Mapping[Path, Sequence[int]] | None = None

    def _row_source(
        self,
    ) -> tuple[tuple[Path, ...], tuple[parquet.Footer, ...], tuple[parquet.Predicate, ...]]:
        return tuple(item.path for item in self.footers), self.footers, self.predicates

    def _physical_groups(self) -> Mapping[Path, Sequence[int]] | None:
        return self.groups

    def head(self, n: int = 10, *, columns: Sequence[str] | None = None, **options: Any) -> Any:
        return super().head(n, columns=self.projection(columns), **options)

    def batches(self, *, columns: Sequence[str] | None = None, **options: Any) -> Any:
        return super().batches(columns=self.projection(columns), **options)

    def scan(self, *, columns: Sequence[str] | None = None, **options: Any) -> Any:
        if columns is None:
            columns = [
                name for name in parquet.native_columns(self.schema()) if name not in self.omitted
            ]
        if not columns:
            raise ValueError("No Polars-compatible columns selected; use exact Arrow .batches().")
        return super().scan(columns=columns, **options)

    def projection(self, columns: Sequence[str] | None) -> Sequence[str] | None:
        if columns is None and self.omitted:
            return [field.name for field in self.schema() if field.name not in self.omitted]
        return columns

    def explain(
        self, *, start: Any = None, end: Any = None, columns: Sequence[str] | None = None
    ) -> dict[str, object]:
        terms = self._time(start, end, "event_timestamp_ns")
        return {
            "predicates": terms,
            "projection": self.projection(columns),
            "files": [
                {
                    "path": str(item.path),
                    "eligible_row_groups": parquet.eligible_groups(
                        item, terms, self.groups.get(item.path) if self.groups else None
                    )
                    if terms
                    else tuple(self.groups[item.path])
                    if self.groups
                    else tuple(range(item.metadata.num_row_groups)),
                }
                for item in self.footers
            ],
        }


@dataclass(frozen=True)
class PhysicalPartition(Rows):
    reference: str
    footers: tuple[parquet.Footer, ...]
    publication: Path
    partition_id: int | None
    data_type: str | None = None

    def _row_source(
        self,
    ) -> tuple[tuple[Path, ...], tuple[parquet.Footer, ...], tuple[parquet.Predicate, ...]]:
        return tuple(item.path for item in self.footers), self.footers, ()

    def info(self) -> dict[str, object]:
        lo, hi = self.extent()
        return {
            "reference": self.reference,
            "publication": str(self.publication),
            "partition_id": self.partition_id,
            "data_type": self.data_type,
            **parquet.totals(self.footers),
            "paths": [str(item.path) for item in self.footers],
            "observed_start_ns": lo,
            "observed_end_ns": hi,
        }

    def extent(self) -> tuple[int | None, int | None]:
        bounds = [item.extent("event_timestamp_ns") for item in self.footers]
        starts = [lo for lo, _ in bounds if lo is not None]
        ends = [hi for _, hi in bounds if hi is not None]
        return (
            (min(starts), max(ends))
            if len(starts) == len(ends) == len(bounds) and bounds
            else (None, None)
        )

    def row_groups(self) -> list[dict[str, object]]:
        return [
            {
                "file": number,
                "path": str(item.path),
                "row_group": index,
                "rows": group.num_rows,
                **row_group_bounds(group),
                "compressed_bytes": sum(
                    group.column(i).total_compressed_size for i in range(group.num_columns)
                ),
            }
            for number, item in enumerate(self.footers, 1)
            for index, group in enumerate(item.groups())
        ]

    def row_group(self, index: int, *, file: int = 1) -> Selection:
        return self.select_row_groups([index], file=file)

    def select_row_groups(self, indices: Sequence[int], *, file: int = 1) -> Selection:
        if type(file) is not int or not 1 <= file <= len(self.footers):
            raise ValueError(f"Unknown file {file} in partition {self.reference}.")
        item = self.footers[file - 1]
        if not indices:
            raise ValueError("Select at least one row group.")
        for index in indices:
            if type(index) is not int or not 0 <= index < item.metadata.num_row_groups:
                raise ValueError(
                    f"Unknown row group {index} in partition {self.reference}, file {file}."
                )
        return Selection((item,), groups={item.path: tuple(sorted(set(indices)))})


def row_group_bounds(group: Any) -> dict[str, int | None]:
    for index in range(group.num_columns):
        column = group.column(index)
        if column.path_in_schema == "event_timestamp_ns":
            stats = column.statistics
            if stats is not None and stats.has_min_max:
                lo, hi = stats.min_raw, stats.max_raw
                if type(lo) is int and type(hi) is int:
                    return {"observed_start_ns": lo, "observed_end_ns": hi}
            break
    return {"observed_start_ns": None, "observed_end_ns": None}


def partitions(dataset: Dataset) -> tuple[PhysicalPartition, ...]:
    groups: dict[str, PhysicalPartition] = {}
    for index, item in enumerate(dataset.footers, 1):
        partition_id = (
            dataset.file_partitions[index - 1] if dataset.file_partitions else dataset.partition_id
        )
        reference = f"partition:{partition_id}" if partition_id is not None else f"file:{index}"
        if prior := groups.get(reference):
            groups[reference] = replace(prior, footers=(*prior.footers, item))
        else:
            groups[reference] = PhysicalPartition(
                reference,
                (item,),
                dataset.publication_path or dataset.path,
                partition_id,
                dataset.data_type,
            )
    return tuple(groups.values())


def select(
    source: Rows,
    stored_kind: str | None,
    data_type: str | None,
    instrument_id: int | None,
    stream_id: str | None,
) -> Selection:
    files, footers, previous = source._row_source()  # pyright: ignore[reportPrivateUsage]
    footers = footers or tuple(parquet.footer(path) for path in files)
    terms = list(previous)
    omitted: tuple[str, ...] = ()
    names = source.schema().names
    if data_type is not None:
        kind = content(data_type)
        if kind == "combined" or stored_kind not in {kind, "combined"}:
            raise ValueError(f"Cannot select {kind!r} from {stored_kind or 'unknown'} data.")
        if stored_kind == "combined":
            if "event_kind" not in names:
                raise ValueError("Combined data requires the physical event_kind column.")
            pa, _ = parquet.arrow_modules()
            if not pa.types.is_uint8(source.schema().field("event_kind").type):
                raise ValueError("Unsupported combined event_kind type. Expected Arrow uint8.")
            terms.append(("event_kind", "=", 0 if kind == "trades" else 1))
            omitted = ("depth", "depth_target") if kind == "trades" else ("trade", "trade_target")
    if instrument_id is not None:
        if type(instrument_id) is not int or not 0 < instrument_id < 2**63:
            raise ValueError("Use a positive signed-64-bit instrument ID from .streams().")
        terms.append(("instrument_id", "=", instrument_id))
    if stream_id is not None:
        if not stream_id.strip():
            raise ValueError("Use an exact nonempty stream ID.")
        terms.append(("stream_id", "=", stream_id))
    for name, _, _ in terms:
        if name not in names:
            raise ValueError(f"Physical predicate column {name!r} is unavailable.")
    return Selection(tuple(footers), tuple(terms), omitted, source._physical_groups())  # pyright: ignore[reportPrivateUsage]
