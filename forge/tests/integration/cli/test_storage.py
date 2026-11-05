"""Storage entry points, filters, read-only transaction and JSON/human presentation."""

import json
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import cast
from unittest.mock import patch

import pytest
from psycopg import AsyncConnection
from psycopg.rows import TupleRow

from forge.cli.commands import storage
from forge.cli.main import main
from forge.shared.paths import ProjectPaths
from forge.storage.browsing import archive_document, pagination
from forge.storage.coverage import CoverageGroup
from forge.storage.models import StorageFilters

START = datetime(2026, 10, 27, tzinfo=UTC)
END = datetime(2026, 10, 30, tzinfo=UTC)


def sample(root: Path) -> dict[str, object]:
    return archive_document(
        {
            "id": 536,
            "origin": "remote",
            "remote_archive_id": 1,
            "exchange": "gateio",
            "instrument_type": "perpetual",
            "market_category": "linear",
            "scope_kind": "family",
            "scope_identifier": "ETH_USDT",
            "base_asset": "ETH",
            "quote_asset": "USDT",
            "data_type": "order_book_l2",
            "feed": "snapshot",
            "feed_source": "registered_source_feed",
            "start_at": START,
            "end_at": END,
            "bytes_written": 1024,
            "status": "complete",
            "relative_path": "opaque-file.bin",
            "filename": "opaque-file.bin",
            "sha256": "a" * 64,
            "last_verified_at": START,
            "normalization_readiness": None,
        },
        root,
    )


def result(
    root: Path, action: str, selected: StorageFilters, gaps: bool = False
) -> dict[str, object]:
    row = sample(root)
    value: dict[str, object] = {
        "schema_version": 1,
        "pagination": pagination(1, selected, 1),
        "filters": selected.model_dump(mode="json"),
        "semantics": "Nominal archive coverage; browsing does not reverify files.",
        "totals": {
            "total": 1,
            "recorded_bytes": 1024,
            "unknown_size_count": 0,
            "unusable_intervals_before_date_filter": 0,
        },
    }
    if action == "coverage":
        group = CoverageGroup(
            {
                key: row[key]
                for key in (
                    "origin",
                    "exchange",
                    "instrument_type",
                    "market_category",
                    "scope_kind",
                    "scope_identifier",
                    "data_type",
                    "feed",
                )
            }
        )
        group.add(row)
        value["coverage"] = [group.document(selected, include_gaps=gaps)]
    else:
        value["archives"] = [row]
    return value


class Connection:
    @asynccontextmanager
    async def transaction(self) -> AsyncGenerator[None]:
        yield

    async def execute(self, query: str) -> None:
        assert query == "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"


@pytest.mark.parametrize("action", [None, "coverage", "list", "show"])
@pytest.mark.parametrize("as_json", [False, True])
def test_entry_points_use_guarded_read_only_services(
    workspace: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    action: str | None,
    as_json: bool,
) -> None:
    expected = action or "coverage"
    root = ProjectPaths.resolve(root=workspace).raw_dir
    calls: list[str] = []

    def dsn(path: Path) -> str:
        assert path == workspace
        return "guarded fixture"

    @asynccontextmanager
    async def connect(dsn: str) -> AsyncGenerator[AsyncConnection[TupleRow]]:
        assert dsn == "guarded fixture"
        yield cast(AsyncConnection[TupleRow], cast(object, Connection()))

    async def coverage(
        c: AsyncConnection[TupleRow], path: Path, filters: StorageFilters, *, include_gaps: bool
    ) -> dict[str, object]:
        assert path == root
        calls.append("coverage")
        if action is not None:
            assert filters.exchange == "gateio" and filters.match == "ETH*"
            assert filters.quote_asset == "usdt" and filters.base_asset == "eTh"
            assert filters.start == START and include_gaps
        return result(path, "coverage", filters, include_gaps)

    async def listing(
        c: AsyncConnection[TupleRow],
        path: Path,
        filters: StorageFilters,
        *,
        archive_id: int | None = None,
    ) -> dict[str, object]:
        assert path == root
        calls.append(expected)
        if expected == "show":
            assert archive_id == 536
        else:
            assert (
                filters.exchange == "gateio"
                and filters.match == "ETH*"
                and filters.quote_asset == "usdt"
            )
        return result(path, "list", filters)

    monkeypatch.setattr(storage, "testing_dsn", dsn)
    monkeypatch.setattr(storage, "testing_connection", connect)
    monkeypatch.setattr(storage, "coverage", coverage)
    monkeypatch.setattr(storage, "list_archives", listing)
    args = ["--project-root", str(workspace), *(["--json"] if as_json else []), "storage"]
    if action is not None:
        args.append(action)
    if action == "show":
        args.append("local:536")
    args.append("--testing")
    if action in {"coverage", "list"}:
        args += [
            "--exchange",
            "gateio",
            "--base-asset",
            "eTh",
            "--quote-asset",
            "usdt",
            "--match",
            "ETH*",
            "--start",
            "2026-10-27T02:00:00+02:00",
        ]
        if action == "coverage":
            args.append("--gaps")
    with patch("httpx.AsyncClient.request", side_effect=AssertionError("upstream call")):
        assert main(args) == 0
    captured = capsys.readouterr()
    assert calls == [expected] and captured.err == "" and "\x1b" not in captured.out
    if as_json:
        document = json.loads(captured.out)
        assert document["pagination"]["total"] == 1
        if expected == "coverage":
            assert document["coverage"][0]["earliest_start"] == "2026-10-27T00:00:00Z"
        else:
            assert document["archives"][0]["local_id"] == "local:536"
    else:
        assert "Storage" in captured.out and "family" in captured.out
        assert "local:536" in captured.out if expected != "coverage" else "Gaps" in captured.out


@pytest.mark.parametrize(
    "flags",
    [
        ["--start", "2026-10-27"],
        ["--start", "2026-10-30T00:00:00Z", "--end", "2026-10-27T00:00:00Z"],
        ["--offset", "-1"],
        ["--limit", "0"],
    ],
)
def test_invalid_browsing_filters_before_connection(
    workspace: Path,
    flags: list[str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def forbidden(root: Path) -> str:
        raise AssertionError("invalid request reached database")

    monkeypatch.setattr(storage, "testing_dsn", forbidden)
    assert (
        main(
            ["--project-root", str(workspace), "--json", "storage", "coverage", "--testing", *flags]
        )
        == 2
    )
    assert json.loads(capsys.readouterr().err)["error"]


@pytest.mark.parametrize("value", ["536", "local:536"])
def test_existing_local_id_syntax(value: str) -> None:
    assert storage.local_identity(value) == 536


def test_custom_storage_root_is_read_only_and_not_created(
    workspace: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    config = workspace / "data/.config/marketforge.toml"
    config.parent.mkdir(parents=True)
    config.write_text('[storage]\ndata_root = "alternate-data"\n')
    expected = workspace / "alternate-data/raw"

    def dsn(root: Path) -> str:
        return "fixture"

    @asynccontextmanager
    async def connect(dsn: str) -> AsyncGenerator[AsyncConnection[TupleRow]]:
        yield cast(AsyncConnection[TupleRow], cast(object, Connection()))

    async def controlled(
        c: AsyncConnection[TupleRow], root: Path, selected: StorageFilters, *, include_gaps: bool
    ) -> dict[str, object]:
        assert root == expected and selected.instrument == "ETH_USDT"
        return {"schema_version": 1, "coverage": [], "pagination": pagination(0, selected, 0)}

    monkeypatch.setattr(storage, "testing_dsn", dsn)
    monkeypatch.setattr(storage, "testing_connection", connect)
    monkeypatch.setattr(storage, "coverage", controlled)
    assert (
        main(
            [
                "--project-root",
                str(workspace),
                "--json",
                "storage",
                "--testing",
                "--instrument",
                "ETH_USDT",
            ]
        )
        == 0
    )
    assert json.loads(capsys.readouterr().out)["coverage"] == []
    assert not expected.parent.exists()


def test_explicit_testing_is_required(workspace: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["--project-root", str(workspace), "--json", "storage"]) == 2
    assert "requires --testing" in capsys.readouterr().err
