"""Cached footer inspection and explicit, budgeted physical-prefix profiling."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, replace
from functools import cached_property
from importlib import import_module
from itertools import combinations
from pathlib import Path
from typing import Any

from forge.datasets import parquet
from forge.datasets.dataset import Dataset, Stream
from forge.datasets.discovery import open_dataset
from forge.datasets.display import LIMIT, handle_details, panel, table, text
from forge.datasets.metadata import utc


@dataclass(frozen=True, repr=False)
class Inspection:
    target: Dataset | Stream
    footers: tuple[parquet.Footer, ...] = ()

    def __post_init__(self) -> None:
        inventory = (
            self.footers
            or self.target.footers
            or tuple(parquet.footer(path) for path in self.target.files)
        )
        object.__setattr__(self, "footers", inventory)
        if not self.target.footers:
            object.__setattr__(self, "target", replace(self.target, footers=inventory))
        _ = self._columns, self._row_groups, self._timestamps, self._schema_count

    @property
    def stream(self) -> Stream | None:
        """The selected stream for a stream inspection."""
        return self.target if isinstance(self.target, Stream) else None

    @cached_property
    def _schema_count(self) -> int:
        return len({str(item.schema.remove_metadata()) for item in self.footers})

    def schema(self) -> Any:
        return parquet.schema(self.target.files, footers=self.footers)

    def columns(self) -> list[dict[str, Any]]:
        return [dict(record) for record in self._columns]

    @cached_property
    def _columns(self) -> list[dict[str, Any]]:
        """Return Parquet leaf statistics. Repeated fields count values, not rows."""
        chunks: dict[str, list[Any]] = {}
        number_groups = sum(item.metadata.num_row_groups for item in self.footers)
        for item in self.footers:
            for group in item.groups():
                for index in range(group.num_columns):
                    column = group.column(index)
                    chunks.setdefault(column.path_in_schema, []).append(column)
        output: list[dict[str, Any]] = []
        for name, leaves in chunks.items():
            stats = [leaf.statistics for leaf in leaves]
            complete = (
                self._schema_count == 1
                and len(stats) == number_groups
                and all(stat is not None for stat in stats)
            )
            counts = [stat.null_count for stat in stats] if complete else []
            nulls = sum(counts) if counts and None not in counts else None
            low = high = None
            bounded = [stat for stat in stats if stat is not None and stat.has_min_max]
            if (
                complete
                and bounded
                and all(
                    stat.has_min_max or (stat.num_values == 0 and stat.null_count is not None)
                    for stat in stats
                )
            ):
                try:
                    # Raw integer timestamp bounds retain all nine digits.
                    timestamp = name.endswith("_timestamp_ns")
                    low = min(stat.min_raw if timestamp else stat.min for stat in bounded)
                    high = max(stat.max_raw if timestamp else stat.max for stat in bounded)
                except (TypeError, ValueError, NotImplementedError):
                    pass
            output.append(
                {
                    "column": name,
                    "physical_type": leaves[0].physical_type,
                    "null_count": nulls,
                    "min": low,
                    "max": high,
                    "scope": "physical Parquet leaf values",
                }
            )
        return output

    def row_groups(self) -> list[dict[str, Any]]:
        return [dict(record) for record in self._row_groups]

    @cached_property
    def _row_groups(self) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        for item in self.footers:
            for index, group in enumerate(item.groups()):
                columns = [group.column(i) for i in range(group.num_columns)]
                timestamp = next(
                    (c.statistics for c in columns if c.path_in_schema == "event_timestamp_ns"),
                    None,
                )
                result.append(
                    {
                        "file": str(item.path),
                        "group": index,
                        "rows": group.num_rows,
                        "uncompressed_bytes": group.total_byte_size,
                        "compressed_bytes": sum(c.total_compressed_size for c in columns),
                        "compression": ", ".join(sorted({c.compression for c in columns})),
                        "event_min_ns": timestamp.min_raw
                        if timestamp is not None and timestamp.has_min_max
                        else None,
                        "event_max_ns": timestamp.max_raw
                        if timestamp is not None and timestamp.has_min_max
                        else None,
                    }
                )
        return result

    def timestamps(self) -> list[dict[str, Any]]:
        return [dict(record) for record in self._timestamps]

    @cached_property
    def _timestamps(self) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        for name in (
            "event_timestamp_ns",
            "system_timestamp_ns",
            "receive_timestamp_ns",
            "ts_recv_ns",
        ):
            for item in self.footers:
                if name not in item.schema.names:
                    continue
                dtype = str(item.schema.field(name).type)
                if dtype != "int64" and not dtype.startswith("timestamp[ns"):
                    continue
                first, last = item.extent(name)
                records.append(
                    {
                        "file": str(item.path),
                        "column": name,
                        "min_ns": first,
                        "max_ns": last,
                        "first_utc": utc(first),
                        "last_utc": utc(last),
                        "evidence": "physical footer"
                        if first is not None
                        else "statistics unavailable",
                    }
                )
        return records

    def summary(self) -> dict[str, object]:
        return {
            **self.target.info(),
            "null_counts": {column["column"]: column["null_count"] for column in self.columns()},
            "caveat": "Physical footer statistics may include other streams. No new integrity verification.",
            "statistic_scope": "whole physical files",
        }

    def profile(self, columns: Sequence[str], *, input_rows: int = 1000) -> dict[str, object]:
        """Aggregate supported columns over matching rows in a bounded physical prefix."""
        if not columns or not 1 <= input_rows <= 1_000_000:
            raise ValueError("Choose columns and an input_rows budget from 1 to 1,000,000.")
        pc: Any = import_module("pyarrow.compute")

        selected = dict.fromkeys(columns)
        pa, _ = parquet.arrow_modules()
        fields = {name: self.schema().field(name).type for name in selected}
        stats: dict[str, dict[str, Any]] = {
            name: {"min": None, "max": None, "null_count": 0, "limitation": None}
            for name in selected
        }
        matching = 0
        for batch in self.target.batches(columns=list(selected), input_rows=input_rows):
            matching += batch.num_rows
            for name, dtype in fields.items():
                field = batch.column(batch.schema.get_field_index(name))
                row = stats[name]
                row["null_count"] += field.null_count
                if pa.types.is_timestamp(dtype) and dtype.unit == "ns":
                    field = field.cast(pa.int64())
                if (
                    pa.types.is_decimal(dtype)
                    or pa.types.is_integer(dtype)
                    or pa.types.is_string(dtype)
                    or pa.types.is_boolean(dtype)
                    or pa.types.is_timestamp(dtype)
                    and dtype.unit == "ns"
                ):
                    try:
                        values = pc.min_max(field).as_py()
                    except (NotImplementedError, pa.ArrowNotImplementedError):
                        row["limitation"] = "Exact min/max are unavailable for this Arrow type."
                        continue
                    lo, hi = values["min"], values["max"]
                    if lo is not None:
                        row["min"] = lo if row["min"] is None else min(row["min"], lo)
                        row["max"] = hi if row["max"] is None else max(row["max"], hi)
                else:
                    row["limitation"] = "Exact min/max are unavailable for this type."
        return {
            "input_row_budget": input_rows,
            "physical_prefix_rows": min(input_rows, sum(f.metadata.num_rows for f in self.footers)),
            "rows_examined": matching,
            "selection": self.target.predicates,
            "sample": "physical prefix before selection",
            "columns": stats,
        }

    def __repr__(self) -> str:
        return f"Inspection({self.target!r}, {len(self.footers)} files)"

    def _repr_html_(self) -> str:
        fields = (
            [
                (field.name, str(field.type), "yes" if field.nullable else "no")
                for field in self.footers[0].schema
            ]
            if self.footers
            else []
        )
        columns, groups, times = self._columns, self._row_groups, self._timestamps
        body = handle_details(self.target)
        body += "<h4>Arrow schema</h4>" + table(("Field", "Exact type", "Nullable"), fields[:18])
        if self._schema_count > 1:
            body += f"<p>{self._schema_count} distinct Arrow schemas. The table shows the first file.</p>"
        body += f"<p class=mf-note>Showing {min(18, len(fields))} of {len(fields)} fields.</p>"
        for title, records, headings, keys, numeric in (
            (
                "Timestamp evidence",
                times,
                ("File", "Column", "First UTC", "Last UTC", "Evidence"),
                ("file", "column", "first_utc", "last_utc", "evidence"),
                (),
            ),
            (
                "Parquet leaf statistics",
                columns,
                ("Leaf", "Physical type", "Nulls", "Stored lower bound", "Stored upper bound"),
                ("column", "physical_type", "null_count", "min", "max"),
                (2, 3, 4),
            ),
            (
                "Row groups",
                groups,
                (
                    "File",
                    "Group",
                    "Rows",
                    "Compressed B",
                    "Uncompressed B",
                    "Codec",
                    "First event UTC",
                    "Last event UTC",
                ),
                (
                    "file",
                    "group",
                    "rows",
                    "compressed_bytes",
                    "uncompressed_bytes",
                    "compression",
                    "event_min_ns",
                    "event_max_ns",
                ),
                (1, 2, 3, 4),
            ),
        ):
            body += f"<h4>{text(title)}</h4>"
            if records:
                body += table(
                    headings,
                    [
                        [
                            Path(row[key]).name
                            if key == "file"
                            else utc(row[key])
                            if key in {"event_min_ns", "event_max_ns"}
                            else row[key]
                            for key in keys
                        ]
                        for row in records[:LIMIT]
                    ],
                    numeric,
                )
                body += f"<p class=mf-note>Showing {min(LIMIT, len(records))} of {len(records)} entries.</p>"
            else:
                body += "<p>Unavailable.</p>"
        return panel(
            "MarketForge · Parquet inspection",
            body
            + "<p class=mf-note>Footer statistics describe physical files. Recorded integrity retains its original scope. This report performs no fresh verification.</p>",
        )


def inspect(target: Dataset | Stream | str | Path) -> Inspection:
    item = open_dataset(target) if isinstance(target, (str, Path)) else target
    return Inspection(item)


def compare(*targets: Dataset | Stream) -> dict[str, object]:
    """Compare stored metadata without counting events or certifying coverage."""
    reports = [inspect(target) for target in targets]
    pairs: list[dict[str, object]] = []
    for (left, a), (right, b) in combinations(enumerate(reports), 2):
        try:
            compatible = a.schema().equals(b.schema(), check_metadata=False)
        except ValueError:
            compatible = False
        pairs.append({"left": left, "right": right, "schema_compatible": compatible})
    return {
        "datasets": [
            report.summary()
            | {
                "memberships": [s.membership() for s in report.target.streams_by_key]
                if isinstance(report.target, Dataset)
                else [report.target.membership()],
            }
            for report in reports
        ],
        "comparisons": pairs,
        "scope": "metadata comparison",
    }
