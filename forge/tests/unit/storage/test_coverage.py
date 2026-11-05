"""Interval unions distinguish nominal envelopes, requested gaps and unknowns."""

from datetime import UTC, datetime, timedelta

import pytest
from pydantic import ValidationError

from forge.storage.coverage import CoverageGroup
from forge.storage.models import StorageFilters, interval

BASE = datetime(2026, 10, 1, tzinfo=UTC)


def day(value: int) -> datetime:
    return BASE + timedelta(days=value)


def group(bounds: list[tuple[object, object]]) -> CoverageGroup:
    result = CoverageGroup({"scope_identifier": "ETH", "feed": "snapshot"})
    for index, (start, end) in enumerate(bounds, 1):
        result.add(
            {
                "id": index,
                "origin": "remote",
                "remote_archive_id": index,
                "bytes_written": 10,
                "start_at": start,
                "end_at": end,
            }
        )
    return result


def test_overlapping_adjacent_nested_duplicate_and_internal_gaps() -> None:
    sample = group(
        [
            (day(0), day(2)),
            (day(1), day(3)),
            (day(0), day(2)),
            (day(1), day(2)),
            (day(3), day(4)),
            (day(6), day(8)),
        ]
    )
    result = sample.document(StorageFilters(), include_gaps=True)
    assert result["local_file_count"] == 6
    assert result["covered_seconds"] == 6 * 86400
    assert result["gap_count"] == 1
    assert result["gaps"] == [{"start": day(4), "end_exclusive": day(6)}]
    assert result["earliest_start"] == day(0) and result["latest_end_exclusive"] == day(8)


def test_requested_leading_trailing_clipping_and_one_bound() -> None:
    sample = group([(day(1), day(3)), (day(5), day(7))])
    bounded = sample.document(StorageFilters(start=day(0), end=day(9)), include_gaps=True)
    assert bounded["gap_count"] == 3
    assert bounded["gaps"] == [
        {"start": day(0), "end_exclusive": day(1)},
        {"start": day(3), "end_exclusive": day(5)},
        {"start": day(7), "end_exclusive": day(9)},
    ]
    clip = sample.document(StorageFilters(start=day(2), end=day(6)), include_gaps=True)
    assert clip["covered_seconds"] == 2 * 86400
    assert clip["gap_count"] == 1
    assert sample.document(StorageFilters(start=day(0)), include_gaps=True)["gap_count"] == 2
    assert sample.document(StorageFilters(end=day(9)), include_gaps=True)["gap_count"] == 2


@pytest.mark.parametrize(
    "bounds",
    [
        (None, None),
        (day(1), None),
        (day(1), day(0)),
        (day(1), day(1)),
        (datetime(2026, 1, 1), day(1)),
    ],
)
def test_unusable_intervals_never_claim_zero_gaps(bounds: tuple[object, object]) -> None:
    assert interval(*bounds)[0] is None
    result = group([bounds]).document(StorageFilters(), include_gaps=True)
    assert result["gap_count"] is None and result["covered_seconds"] is None
    assert result["unknown_interval_count"] == 1 and result["diagnostics"]


def test_timezone_and_interval_validation() -> None:
    for payload in (
        {"start": "2026-01-01"},
        {"start": day(2), "end": day(1)},
        {"start": day(1), "end": day(1)},
        {"limit": 0},
        {"offset": -1},
    ):
        with pytest.raises(ValidationError):
            StorageFilters.model_validate(payload)
    selected = StorageFilters.model_validate({"start": "2026-10-01T02:00:00+02:00"})
    assert selected.start is not None
    assert selected.start == BASE and selected.start.tzinfo == UTC
