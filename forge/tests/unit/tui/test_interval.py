"""Validate UTC intervals through the authoritative pipeline recipe model."""

from datetime import UTC, datetime, timedelta

import pytest

from forge.historical.merge.service import Recipe as DirectMerge
from forge.historical.pipelines.config import PipelineRecipe
from forge.tui.interval import combine_timestamp
from forge.tui.recipe_forms import RecipeForm


@pytest.mark.parametrize(
    "day,clock",
    [
        ("", "00:00:00"),
        ("2025-02-29", "00:00:00"),
        ("2026-04-31", "00:00:00"),
        ("2026-13-01", "00:00:00"),
        ("2026-04-01", "24:00:00"),
        ("2026-04-01", "00:60:00"),
        ("2026-04-01", "00:00:60"),
        ("2026-04-01", "00:00:00Z"),
    ],
)
def test_invalid_interval_input(day: str, clock: str) -> None:
    with pytest.raises(ValueError):
        combine_timestamp(day, clock, nanoseconds=False)


@pytest.mark.parametrize(
    "end",
    [
        "",
        "2024-02-28",
        "2024-02-29",
        "2024-03-01",
        "2025-01-01",
    ],
)
def test_pipeline_end_order(end: str) -> None:
    form = RecipeForm(
        PipelineRecipe,
        {
            "version": 1,
            "requests": [
                {
                    "exchange": "bybit",
                    "instrument_type": "spot",
                    "market_category": "spot",
                    "scope": {"kind": "instrument", "identifier": "BTCUSDT"},
                    "data_type": "trades",
                }
            ],
        },
        new=True,
    )
    _, values = form.build()
    values["version"] = 1
    values["interval--start--date"] = "2024-02-29"
    values["interval--end--date"] = end
    data, issues = form.candidate(values)
    if end <= "2024-02-29":
        assert issues
    else:
        interval = PipelineRecipe.model_validate(data).interval
        assert interval.start == datetime(2024, 2, 29, tzinfo=UTC)
        assert interval.end.isoformat() == f"{end}T00:00:00+00:00"


def test_exact_direct_merge_nanoseconds_round_trip() -> None:
    original: dict[str, object] = {"dataset_ids": [4], "start_ns": -1, "end_ns": 1_000_000_001}
    form = RecipeForm(DirectMerge, original)
    _, values = form.build()
    assert values["start_ns--time"] == "23:59:59.999999999"
    assert values["end_ns--time"] == "00:00:01.000000001"
    assert form.candidate(values) == (original, {})


def test_fractional_datetime_utc() -> None:
    stamp = combine_timestamp("2024-01-01", "00:00:00.123456", nanoseconds=False)
    assert stamp == datetime(2024, 1, 1, tzinfo=UTC) + timedelta(microseconds=123456)
