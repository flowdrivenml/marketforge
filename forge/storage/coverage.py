"""Nominal interval unions and missing ranges; no file or event verification."""

from dataclasses import dataclass, field
from datetime import UTC, datetime

from forge.storage.models import StorageFilters, interval

GROUP_FIELDS = (
    "origin",
    "external_source_id",
    "external_source_key",
    "exchange",
    "instrument_type",
    "market_category",
    "scope_kind",
    "scope_identifier",
    "data_type",
    "feed",
)


def union(intervals: list[tuple[datetime, datetime]]) -> list[tuple[datetime, datetime]]:
    result: list[tuple[datetime, datetime]] = []
    for start, end in sorted(intervals):
        if result and start <= result[-1][1]:
            result[-1] = (result[-1][0], max(end, result[-1][1]))
        else:
            result.append((start, end))
    return result


@dataclass
class CoverageGroup:
    identity: dict[str, object]
    intervals: list[tuple[datetime, datetime]] = field(
        default_factory=lambda: list[tuple[datetime, datetime]]()
    )
    base_asset_values: set[str | None] = field(default_factory=lambda: set[str | None]())
    quote_asset_values: set[str | None] = field(default_factory=lambda: set[str | None]())
    local_file_count: int = 0
    recorded_bytes: int = 0
    unknown_interval_count: int = 0
    unknown_size_count: int = 0
    source_objects: set[tuple[str, str]] = field(default_factory=lambda: set[tuple[str, str]]())
    diagnostics: dict[str, list[int]] = field(default_factory=lambda: dict[str, list[int]]())

    def add(self, row: dict[str, object]) -> None:
        identity = int(str(row["id"]))
        self.local_file_count += 1
        self.base_asset_values.add(
            str(row["base_asset"]) if row.get("base_asset") is not None else None
        )
        self.quote_asset_values.add(
            str(row["quote_asset"]) if row.get("quote_asset") is not None else None
        )
        self.source_objects.add(
            (
                str(row["origin"]),
                str(row.get("remote_archive_id") or row.get("external_archive_id")),
            )
        )
        if row.get("bytes_written") is None:
            self.unknown_size_count += 1
        else:
            self.recorded_bytes += int(str(row["bytes_written"]))
        bounds, issue = interval(row.get("start_at"), row.get("end_at"))
        if bounds is not None:
            self.intervals.append(bounds)
        else:
            self.unknown_interval_count += 1
        issues = [issue] if issue else []
        for key in (
            "exchange",
            "instrument_type",
            "market_category",
            "scope_kind",
            "scope_identifier",
            "base_asset",
            "quote_asset",
        ):
            if row.get(key) is None:
                issues.append(f"unknown or ambiguous {key}")
        if row.get("data_type") == "order_book_l2" and row.get("feed") is None:
            issues.append("unknown feed identity")
        for message in issues:
            self.diagnostics.setdefault(message, []).append(identity)

    def document(self, filters: StorageFilters, *, include_gaps: bool) -> dict[str, object]:
        merged = union(self.intervals)
        earliest = merged[0][0] if merged else None
        latest = merged[-1][1] if merged else None
        start = filters.start.astimezone(UTC) if filters.start else earliest
        end = filters.end.astimezone(UTC) if filters.end else latest
        covered: list[tuple[datetime, datetime]] = []
        gaps: list[tuple[datetime, datetime]] = []
        if start is not None and end is not None and start < end:
            covered = [(max(a, start), min(b, end)) for a, b in merged if a < end and b > start]
            cursor = start
            for a, b in covered:
                if cursor < a:
                    gaps.append((cursor, a))
                cursor = max(cursor, b)
            if cursor < end:
                gaps.append((cursor, end))
        known_range = start is not None and end is not None and start < end
        return {
            **self.identity,
            "base_asset": next(iter(self.base_asset_values), None)
            if len(self.base_asset_values) == 1
            else None,
            "quote_asset": next(iter(self.quote_asset_values), None)
            if len(self.quote_asset_values) == 1
            else None,
            "base_asset_values": sorted(self.base_asset_values, key=lambda value: value or ""),
            "quote_asset_values": sorted(self.quote_asset_values, key=lambda value: value or ""),
            "metadata_conflicts": [
                key
                for key, values in (
                    ("base_asset", self.base_asset_values),
                    ("quote_asset", self.quote_asset_values),
                )
                if len(values) > 1
            ],
            "earliest_start": earliest,
            "latest_end_exclusive": latest,
            "evaluation_start": start,
            "evaluation_end_exclusive": end,
            "range_basis": "requested_bounds_with_observed_envelope_for_unspecified_bounds",
            "local_file_count": self.local_file_count,
            "unique_registered_source_count": len(self.source_objects),
            "recorded_bytes": self.recorded_bytes,
            "unknown_size_count": self.unknown_size_count,
            "unknown_interval_count": self.unknown_interval_count,
            "covered_seconds": sum((b - a).total_seconds() for a, b in covered)
            if known_range
            else None,
            "merged_intervals": [{"start": a, "end_exclusive": b} for a, b in merged],
            "gap_count": len(gaps) if known_range else None,
            "gaps": [{"start": a, "end_exclusive": b} for a, b in gaps] if include_gaps else None,
            "diagnostics": self.diagnostics,
        }
