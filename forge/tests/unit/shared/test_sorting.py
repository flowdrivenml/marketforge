"""Typed local ordering and view capability validation."""

from datetime import datetime
from decimal import Decimal
from pathlib import Path
from unittest.mock import Mock

import pytest

from forge.datasets import query
from forge.shared.config.library import list_config_names
from forge.shared.errors import MarketForgeError
from forge.shared.paths import ProjectPaths
from forge.shared.sorting import SortField, SortSpec, ordered

SORT = SortSpec({"value": SortField("value", "desc")}, "value", aliases={"time": "value"})


def test_ordering_retains_exact_numeric_and_temporal_values(tmp_path: Path) -> None:
    rows = [
        (2**62 + 2, Decimal("10.000000000000000002")),
        (2**62 + 1, Decimal("10.000000000000000002")),
        (2**62, Decimal("10.000000000000000001")),
        (4, Decimal("0")),
        (1, None),
    ]
    for direction, ids in (
        ("asc", [4, 2**62, 2**62 + 1, 2**62 + 2, 1]),
        ("desc", [2**62 + 1, 2**62 + 2, 2**62, 4, 1]),
    ):
        result = ordered(
            rows, SORT.resolve("time", direction), lambda row: row[1], lambda row: row[0]
        )
        assert [row[0] for row in result] == ids
    date_rows = [
        (3, datetime.fromisoformat("2026-01-01T01:00:00+01:00")),
        (2, datetime.fromisoformat("2026-01-01T00:00:00+00:00")),
    ]
    assert [
        r[0] for r in ordered(date_rows, SORT.resolve(), lambda row: row[1], lambda row: row[0])
    ] == [2, 3]
    assert query.timestamp("2026-01-01T01:00:00.123456789+01:00") == (
        query.timestamp("2026-01-01T00:00:00.123456788Z") + 1
    )
    for name in ("zeta", "alpha", "middle"):
        (tmp_path / f"{name}.toml").touch()
    assert list_config_names(tmp_path) == ("alpha", "middle", "zeta")
    assert list_config_names(tmp_path, order="desc") == ("zeta", "middle", "alpha")


@pytest.mark.asyncio
async def test_unsupported_dataset_sort_fails_before_database(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dsn = Mock(side_effect=AssertionError("sort validation must precede registry access"))
    monkeypatch.setattr(query, "database_dsn", dsn)
    with pytest.raises(MarketForgeError, match="Unsupported sort field 'size'"):
        await query.browse(
            ProjectPaths(tmp_path, tmp_path / "data"),
            query.DatasetRequest("list", view="timelines", sort="size"),
        )
    dsn.assert_not_called()


def test_managed_inventory_commands_sort_names_without_registry(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    import json

    from forge.cli.main import main

    (tmp_path / "forge").mkdir()
    (tmp_path / "forge" / "pyproject.toml").touch()
    (tmp_path / "engine").mkdir()
    paths = ProjectPaths(tmp_path, tmp_path / "data")
    for command, family in (
        ("acquisition", "acquisition"),
        ("normalize", "normalization"),
        ("merge", "merge"),
        ("convert", "conversion"),
        ("pipeline", "pipeline"),
    ):
        directory = paths.workflow_dir(family)
        directory.mkdir(parents=True)
        for name in ("zeta", "alpha"):
            (directory / f"{name}.toml").write_text("# placeholder to exercise ordering")
        for order, expected in (("asc", ["alpha", "zeta"]), ("desc", ["zeta", "alpha"])):
            assert (
                main(["--project-root", str(tmp_path), "--json", command, "list", "--order", order])
                == 0
            )
            result = json.loads(capsys.readouterr().out)
            rows = result["configurations" if command == "acquisition" else "recipes"]
            assert [row if isinstance(row, str) else row["name"] for row in rows] == expected
