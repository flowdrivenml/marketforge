"""Content grouping, stream priorities and the single-file Forge wire boundary."""

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast

import pytest
from psycopg import AsyncConnection
from psycopg.rows import TupleRow

from forge.historical.archives.config import AcquisitionConfig
from forge.historical.normalization.batch import plan_batch, publish_batch, used_ids
from forge.historical.normalization.config import InputSelection
from forge.markets.models import array, obj
from forge.shared.errors import MarketForgeError
from forge.shared.paths import ProjectPaths
from forge.storage.dependencies import prepared_references


@pytest.mark.asyncio
@pytest.mark.parametrize("trades_only", [False, True])
async def test_content_jobs_ranks_feeds_diagnostics_and_single_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    trades_only: bool,
) -> None:
    paths = ProjectPaths(root=tmp_path, data_root=tmp_path / "data")
    connection = cast(AsyncConnection[TupleRow], object())
    base = datetime(2026, 10, 27, tzinfo=UTC)
    requests: list[dict[str, object]] = []
    rows: list[dict[str, object]] = []
    for symbol, instrument_id, content, feed in [
        ("A", 1, "trades", None),
        ("B", 2, "trades", None),
        ("C", 3, "trades", None),
        ("D", 4, "trades", None),
        *(
            []
            if trades_only
            else [
                ("A", 1, "order_book_l2", None),
                ("G", 5, "order_book_l2", "snapshot"),
                ("G", 5, "order_book_l2", "incremental"),
            ]
        ),
    ]:
        exchange = "gateio" if feed else "bybit"
        request: dict[str, object] = {
            "exchange": exchange,
            "instrument_type": "spot",
            "market_category": "spot",
            "data_type": content,
            "scope": {"kind": "instrument", "identifier": symbol},
        }
        if feed:
            request["source_options"] = {"feed": feed}
        requests.append(request)
        for day in range(2 if symbol == "A" and content == "trades" else 1):
            identity = len(rows) + 1
            rows.append(
                {
                    "id": identity,
                    "origin": "remote",
                    "remote_archive_id": identity,
                    "external_archive_id": None,
                    "scope_identifier": symbol,
                    "scope_kind": "instrument",
                    "instrument_id": instrument_id,
                    "exchange": exchange,
                    "instrument_type": "spot",
                    "market_category": "spot",
                    "data_type": content,
                    "feed": feed,
                    "base_asset": symbol,
                    "quote_asset": "USDT",
                    "bytes_written": 4,
                    "start_at": base + timedelta(days=day),
                    "end_at": base + timedelta(days=day + 1),
                }
            )
    # Repeated requests do not duplicate inputs, while an unresolved input remains diagnosed.
    requests.append(requests[0])
    rows.append({**rows[0], "id": 99, "remote_archive_id": 99})
    recipe = AcquisitionConfig.model_validate(
        {
            "version": 1,
            "interval": {"start": base, "end": base + timedelta(days=2)},
            "requests": requests,
        }
    )

    async def ranking(c: AsyncConnection[TupleRow]) -> tuple[str, dict[int, int]]:
        return "fixture-snapshot", {1: 100, 2: 3, 3: 3}

    async def occupied(c: AsyncConnection[TupleRow], paths: ProjectPaths) -> set[int]:
        return set()

    async def scan(
        c: AsyncConnection[TupleRow], params: dict[str, object], after: int
    ) -> list[dict[str, object]]:
        return [
            r
            for r in rows
            if int(str(r["id"])) > after
            and all(
                r[key] == params[value]
                for key, value in [
                    ("exchange", "exchange"),
                    ("scope_identifier", "instrument"),
                    ("data_type", "data_type"),
                    ("feed", "feed"),
                ]
            )
        ]

    async def selection(
        c: AsyncConnection[TupleRow], paths: ProjectPaths, row: dict[str, object], content: str
    ) -> InputSelection:
        return InputSelection(
            task_id=1,
            archive_id=int(str(row["id"])),
            instrument_id=int(str(row["instrument_id"])),
            format_profile="fixture",
        )

    async def resolve(
        c: AsyncConnection[TupleRow], paths: ProjectPaths, selection: InputSelection, content: str
    ) -> tuple[dict[str, object], str]:
        if selection.archive_id == 99:
            raise MarketForgeError("Missing authoritative tick_size metadata.")
        row = next(r for r in rows if r["id"] == selection.archive_id)
        stream = f"{row['exchange']}.spot.spot.{row['scope_identifier']}.{content}"
        return {
            "task_id": 1,
            "stream_id": stream,
            "input_path": str(paths.raw_dir / str(selection.archive_id)),
            "instrument_id": selection.instrument_id,
            "format_profile": "fixture",
            "source_compression": "gzip",
        }, stream

    monkeypatch.setattr("forge.historical.normalization.batch.ranking_positions", ranking)
    monkeypatch.setattr("forge.historical.normalization.batch.used_ids", occupied)
    monkeypatch.setattr("forge.historical.normalization.batch.scan", scan)
    monkeypatch.setattr("forge.historical.normalization.batch.selection_for", selection)
    monkeypatch.setattr("forge.historical.normalization.batch.resolve_input", resolve)
    plan = await plan_batch(connection, paths, recipe)
    assert plan.report["status"] == "partial"
    assert plan.report["ranking_snapshot"] == "fixture-snapshot"
    assert len(plan.jobs) == (1 if trades_only else 2)
    for job in plan.jobs.values():
        tasks = [obj(t) for t in array(job["tasks"])]
        streams = [obj(s) for s in array(job["streams"])]
        defined = {s["stream_id"] for s in streams}
        assert {t["stream_id"] for t in tasks} == defined
        assert [t["task_id"] for t in tasks] == list(range(1, len(tasks) + 1))
        assert [s["stream_rank"] for s in streams] == list(range(1, len(streams) + 1))
        if obj(job["output"])["content_type"] == "trades":
            assert len(tasks) == 5
            assert [str(s["stream_id"]).split(".")[-2] for s in streams] == ["B", "C", "A", "D"]
        else:
            assert len(tasks) == 3 and len(defined) == 3
            assert {
                "gateio.spot.spot.G.depth.snapshot",
                "gateio.spot.spot.G.depth.incremental",
            } <= defined
    wrapper = obj(json.loads(json.dumps(plan.report, default=str)))
    publish_batch(paths, plan, wrapper)
    assert [p.name for p in plan.run.iterdir()] == ["normalize-batch.json"]
    saved = obj(json.loads((plan.run / "normalize-batch.json").read_text()))
    assert saved == wrapper
    assert len(array(saved["jobs"])) == (1 if trades_only else 2)
    assert any(array(obj(r)["blockers"]) for r in array(saved["requests"]))
    assert any(obj(obj(r)["coverage"])["gap_count"] for r in array(saved["requests"]))
    assert "engine_invocation" not in saved
    assert prepared_references(paths).paths


@pytest.mark.asyncio
async def test_embedded_job_ids_are_in_collision_checks(tmp_path: Path) -> None:
    from unittest.mock import AsyncMock

    paths = ProjectPaths(root=tmp_path, data_root=tmp_path / "data")
    run = paths.runs_dir / "example"
    run.mkdir(parents=True)
    (run / "normalize-batch.json").write_text(
        json.dumps(
            {
                "kind": "normalization_batch",
                "jobs": [{"job_id": 123, "dataset_id": 456}, {"job_id": 789, "dataset_id": 1011}],
            }
        )
    )
    c = AsyncMock()
    c.execute.return_value.fetchone.return_value = (None,)
    assert await used_ids(cast(AsyncConnection[TupleRow], c), paths) == {123, 456, 789, 1011}
