"""Timeline grouping and historical references over rollback-only registry evidence."""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import replace
from io import StringIO
from pathlib import Path
from typing import Any
from unittest.mock import Mock
from uuid import uuid4

import pytest
from psycopg import AsyncConnection, AsyncCursor, OperationalError
from psycopg.rows import TupleRow
from psycopg.types.json import Jsonb
from rich.console import Console

from forge.datasets import parquet, query, views
from forge.shared.errors import MarketForgeError
from forge.shared.paths import ProjectPaths
from forge.shared.terminal.dataset_timelines import render_timeline, render_timelines
from forge.shared.terminal.theme import build_theme

pytestmark = [pytest.mark.integration, pytest.mark.database]
START = 1793059200123456789


@pytest.mark.asyncio
async def test_timeline_membership_paging_and_snapshot(
    database: AsyncConnection[TupleRow],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    namespace = uuid4().hex
    root = str(tmp_path)
    first: dict[str, Any] = {
        "exchange": "bybit",
        "instrument_id": 123,
        "instrument_type": "perpetual",
        "market_category": "linear",
        "symbol": "BTCUSDT",
        "stream_id": namespace,
        "kind": "trade",
    }

    def member(lo: int, hi: int, **fields: Any) -> dict[str, Any]:
        return {**first, "start_timestamp_ns": START + lo, "end_timestamp_ns": START + hi, **fields}

    async def publish(
        members: list[dict[str, Any]], *, integrity: str = "clean", protocol: int = 1
    ) -> tuple[int, int]:
        engine = str(uuid4().int % 2**63)
        cursor = await database.execute(
            """INSERT INTO historical.dataset_publications
            (instrument_namespace,engine_dataset_id,operation,storage_root,manifest_path,evidence)
            VALUES (%s,%s,'normalize',%s,%s,%s) RETURNING id""",
            (
                namespace,
                engine,
                root,
                f"{root}/manifest.json",
                Jsonb({"manifest": {"protocol_version": protocol}}),
            ),
        )
        row = await cursor.fetchone()
        assert row is not None
        publication = int(row[0])
        cursor = await database.execute(
            """INSERT INTO historical.datasets
            (data_type,start_timestamp_ns,end_timestamp_ns,storage_format,storage_path,status,
            completed_at,operation,storage_root_key,manifest_path,operation_details,
            publication_id,primary_file,file_sha256)
            VALUES ('trade_l2',%s,%s,'parquet',%s,'complete',now(),'normalize',%s,%s,%s,%s,%s,%s)
            RETURNING id""",
            (
                min(
                    m["start_timestamp_ns"] for m in members if m["start_timestamp_ns"] is not None
                ),
                max(m["end_timestamp_ns"] for m in members if m["end_timestamp_ns"] is not None),
                f"{root}/{publication}.parquet",
                root,
                f"{root}/manifest.json",
                Jsonb({"memberships": members, "integrity": integrity}),
                publication,
                f"{publication}.parquet",
                "a" * 64,
            ),
        )
        row = await cursor.fetchone()
        assert row is not None
        return publication, int(row[0])

    seed, file_id = await publish(
        [
            member(0, 15),
            member(900, 1000, instrument_id=124, symbol="ETHUSDT", kind="depth"),
            member(0, 3, instrument_id=123, exchange="okx"),
            member(0, 3, instrument_type="spot", market_category="spot"),
            member(0, 3, market_category="inverse"),
            member(0, 3, instrument_id=125),
            member(0, 3, instrument_id=126, symbol="BTCUSDT-25DEC26", instrument_type="future"),
            member(0, 3, instrument_id=127, symbol="BTCUSDT-26MAR27", instrument_type="future"),
            member(
                0,
                3,
                instrument_id=129,
                symbol="UNKNOWN",
                start_timestamp_ns=None,
                end_timestamp_ns=None,
            ),
        ]
    )
    for idx in range(1, 56):
        # Nested spans exercise the running maximum. A new span starts at exactly
        # the previous endpoint, and needs neither float math nor a tolerance.
        await publish(
            [member(idx, idx + 15), member(idx, idx)], integrity="degraded" if idx == 1 else "clean"
        )
    separated, _ = await publish([member(71, 75)])
    incompatible, _ = await publish([member(10, 20)], protocol=2)
    request = query.DatasetRequest(
        "list",
        view="timelines",
        operation="normalize",
        search=namespace,
        limit=1,
        sort="start",
        instrument_type="perpetual",
        market_category="linear",
        exchange="bybit",
        data_type="trades",
        instrument="123",
    )

    forbidden = Mock(side_effect=AssertionError("Timeline browsing must use registry metadata"))
    monkeypatch.setattr(parquet, "schema", forbidden)
    monkeypatch.setattr(query, "checksum", forbidden)
    calls = Mock(wraps=AsyncCursor.execute)

    async def measured(self: AsyncCursor[Any], sql: Any, *args: Any, **kwargs: Any) -> Any:
        return await calls(self, sql, *args, **kwargs)

    with monkeypatch.context() as measured_query:
        measured_query.setattr(AsyncCursor, "execute", measured)
        result = await views.page(database, request)
        assert calls.call_count == 2  # registry identity plus one set-based page query
    assert result["total"] == 3
    assert result["timeline_counts"]["identities"] == 2
    assert result["timeline_counts"]["observed_separations"] == 1
    assert result["matching_publications"] == 58
    row = result["timelines"][0]
    assert row["start_timestamp_ns"] == START and row["end_timestamp_ns"] == START + 70
    assert row["coverage"] == "observed_extent"
    assert row["integrity"] == "mixed" and row["publication_count"] == 56
    assert row["matching_primary_files"] == 56
    assert row["publications"][0]["registry_publication_id"] == seed
    assert row["publications"][0]["engine_publication_id"] != str(seed)
    expanded = await views.page(database, replace(request, limit=20))
    assert expanded["timelines"][0] == row
    next_page = await views.page(database, replace(request, offset=1))
    assert next_page["timelines"] == expanded["timelines"][1:2]
    by_instrument = await views.page(database, replace(request, limit=20, sort="instrument"))
    assert {r["timeline_reference"] for r in by_instrument["timelines"]} == {
        r["timeline_reference"] for r in expanded["timelines"]
    }
    assert expanded["timelines"][1]["publications"][0]["registry_publication_id"] == incompatible
    assert expanded["timelines"][2]["publications"][0]["registry_publication_id"] == separated
    assert expanded["timelines"][2]["observed_separation"] == {
        "after_ns": START + 70,
        "before_ns": START + 71,
        "coverage": "unknown",
    }
    by_publication = await views.page(database, replace(request, publication_id=seed))
    assert by_publication["timeline_counts"]["observed_segments"] == 1
    assert by_publication["matching_publications"] == 56
    assert by_publication["timelines"][0] == row

    mixed = await views.page(
        database,
        query.DatasetRequest(
            "list",
            view="timelines",
            publication_id=seed,
            start=START + 899,
            end=START + 1001,
        ),
    )
    assert {item["symbol"] for item in mixed["timelines"]} == {"ETHUSDT"}
    assert mixed["timelines"][0]["data_type"] == "l2"
    all_members = await views.page(
        database,
        query.DatasetRequest(
            "list",
            view="timelines",
            publication_id=seed,
        ),
    )
    assert all_members["timeline_counts"]["identities"] == 9
    assert all_members["timeline_counts"]["unknown_segments"] == 1
    assert not (await views.page(database, replace(request, end=START)))["timelines"]
    at_last = await views.page(database, replace(request, start=START + 70, end=START + 71))
    assert at_last["matching_publications"] == 1
    assert at_last["timelines"][0]["end_timestamp_ns"] == START + 70
    # Append a publication across both observed segments. The list extends
    # and merges, but the earlier reference resolves exactly its original set.
    await publish([member(70, 90)])
    latest = await views.page(database, request)
    assert latest["total"] == 2
    assert latest["timelines"][0]["publication_count"] == 58

    @asynccontextmanager
    async def connection(_: str) -> AsyncGenerator[AsyncConnection[TupleRow]]:
        yield database

    def fixture_dsn(_: Path) -> str:
        return "fixture"

    monkeypatch.setattr(query, "database_connection", connection)
    monkeypatch.setattr(query, "database_dsn", fixture_dsn)
    paths = ProjectPaths(tmp_path, tmp_path / "data")
    detail = await query.browse(
        paths, query.DatasetRequest("timeline", timeline_reference=row["timeline_reference"])
    )
    assert detail["timeline"] == row
    assert (await query.browse(paths, query.DatasetRequest("show", publication_id=seed)))[
        "publication"
    ]["publication_id"] == seed
    assert (await query.browse(paths, query.DatasetRequest("show", id=file_id)))["file"][
        "id"
    ] == file_id
    with pytest.raises(MarketForgeError, match="exact tl1"):
        await query.browse(paths, query.DatasetRequest("timeline", timeline_reference="invalid"))

    for width in (80, 120, 160):
        output = StringIO()
        console = Console(file=output, width=width, theme=build_theme(), color_system=None)
        render_timelines(console, expanded, request)
        render_timeline(console, detail, precise=True)
        rendered = output.getvalue()
        assert "3 segments" in rendered and "Observed only" in rendered
        assert str(seed) in rendered and ".123456789Z" in rendered
        assert "\x1b" not in rendered
    forbidden.assert_not_called()


@pytest.mark.asyncio
async def test_timeline_query_errors_do_not_become_empty_pages(
    database: AsyncConnection[TupleRow],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    execute = Mock(wraps=AsyncCursor.execute)

    async def fail_page(self: AsyncCursor[Any], sql: Any, *args: Any, **kwargs: Any) -> Any:
        if isinstance(sql, str) and "WITH registered" in sql:
            raise OperationalError("test timeline query failure")
        return await execute(self, sql, *args, **kwargs)

    monkeypatch.setattr(AsyncCursor, "execute", fail_page)
    with pytest.raises(OperationalError, match="test timeline query failure"):
        await views.page(
            database, query.DatasetRequest("list", view="timelines", operation="normalize")
        )
