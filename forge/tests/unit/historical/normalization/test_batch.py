"""Content grouping, stream priorities and the single-file Forge wire boundary."""

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast

import pytest
from psycopg import AsyncConnection
from psycopg.rows import TupleRow

from forge.historical.archives.config import AcquisitionConfig
from forge.historical.normalization.batch import dense_ranks, plan_batch, publish_batch, used_ids
from forge.historical.normalization.mapping import ResolvedArchive
from forge.markets.models import array, obj
from forge.shared.errors import MarketForgeError
from forge.shared.paths import ProjectPaths
from forge.storage.dependencies import prepared_references


@pytest.mark.asyncio
@pytest.mark.parametrize("trades_only", [False, True])
@pytest.mark.parametrize("has_snapshot", [False, True])
async def test_content_jobs_ranks_feeds_diagnostics_and_single_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    trades_only: bool,
    has_snapshot: bool,
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

    async def ranking(c: AsyncConnection[TupleRow]) -> tuple[str | None, dict[int, int]]:
        return ("fixture-snapshot", {1: 100, 2: 3, 3: 3}) if has_snapshot else (None, {})

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

    async def resolve(
        c: AsyncConnection[TupleRow], paths: ProjectPaths, row: dict[str, object], content: str
    ) -> ResolvedArchive:
        if row["id"] == 99:
            raise MarketForgeError("Missing authoritative tick_size metadata.")
        stream = f"{row['exchange']}.spot.spot.{row['scope_identifier']}.{content}"
        if row["feed"]:
            stream += f".{row['feed']}"
        task: dict[str, object] = {
            "task_id": 1,
            "stream_id": stream,
            "input_path": str(paths.raw_dir / str(row["id"])),
            "exchange": row["exchange"],
            "instrument_type": "spot",
            "market_category": "spot",
            "symbol": row["scope_identifier"],
            "base_asset": row["base_asset"],
            "quote_asset": "USDT",
            "instrument": {
                "tick_size": "0.1",
                "contract_value": None,
                "contract_value_asset": None,
            },
        }
        if row["feed"]:
            task["source_options"] = {"feed": row["feed"]}
        return ResolvedArchive(task, int(str(row["instrument_id"])))

    monkeypatch.setattr("forge.historical.normalization.batch.ranking_positions", ranking)
    monkeypatch.setattr("forge.historical.normalization.batch.used_ids", occupied)
    monkeypatch.setattr("forge.historical.normalization.batch.scan", scan)
    monkeypatch.setattr("forge.historical.normalization.batch.resolve_archive", resolve)
    plan = await plan_batch(connection, paths, recipe)
    assert plan.report["status"] == "partial"
    assert plan.report["ranking_snapshot"] == ("fixture-snapshot" if has_snapshot else None)
    assert obj(plan.report["ranking_resolution"])["fallback"] is (not has_snapshot)
    assert len(plan.jobs) == (1 if trades_only else 2)
    for job in plan.jobs.values():
        tasks = [obj(t) for t in array(job["tasks"])]
        assert "streams" not in job
        assert "protocol_version" not in job
        by_stream: dict[str, int] = {}
        for task in tasks:
            rank = task["stream_rank"]
            assert type(rank) is int and rank > 0
            stream = str(task["stream_id"])
            assert by_stream.setdefault(stream, rank) == rank
        streams = [
            {"stream_id": stream, "stream_rank": rank}
            for stream, rank in sorted(by_stream.items(), key=lambda item: item[1])
        ]
        defined = {s["stream_id"] for s in streams}
        assert {t["stream_id"] for t in tasks} == defined
        assert [t["task_id"] for t in tasks] == list(range(1, len(tasks) + 1))
        assert [s["stream_rank"] for s in streams] == list(range(1, len(streams) + 1))
        if obj(job["output"])["content_type"] == "trades":
            assert len(tasks) == 5
            assert [str(s["stream_id"]).split(".")[-2] for s in streams] == (
                ["B", "C", "A", "D"] if has_snapshot else ["A", "B", "C", "D"]
            )
        else:
            assert len(tasks) == 3 and len(defined) == 3
            assert {
                "gateio.spot.spot.G.depth.snapshot",
                "gateio.spot.spot.G.depth.incremental",
            } <= defined
    wrapper = obj(json.loads(json.dumps(plan.report, default=str)))
    publish_batch(paths, plan, wrapper)
    assert sorted(p.name for p in plan.run.iterdir()) == [
        "normalize-batch.json",
        "prepare-report.json",
    ]
    saved = obj(json.loads((plan.run / "normalize-batch.json").read_text()))
    assert saved == json.loads(json.dumps(plan.batch, default=str))
    assert saved["schema_version"] == 3
    assert not {"requests", "job_summaries", "prepared_job_count", "engine_ready"} & saved.keys()
    report = obj(json.loads((plan.run / "prepare-report.json").read_text()))
    assert report == wrapper
    assert "jobs" not in report
    assert report["engine_ready"] is False
    assert report["prepared_task_count"] == sum(len(array(j["tasks"])) for j in plan.jobs.values())
    assert not (paths.data_root / "test/normalized").exists()
    assert len(array(saved["jobs"])) == (1 if trades_only else 2)
    assert any(array(obj(r)["blockers"]) for r in array(report["requests"]))
    assert any(obj(obj(r)["coverage"])["gap_count"] for r in array(report["requests"]))
    assert "engine_invocation" not in saved
    assert set(prepared_references(paths).paths) == {
        str(task["input_path"])
        for job in plan.jobs.values()
        for task in (obj(t) for t in array(job["tasks"]))
    }
    # A report write failure must leave neither artifact visible.
    from dataclasses import replace

    failed = replace(plan, run=paths.runs_dir / "failed")
    from forge.historical.normalization.service import write_job

    def fail_report(destination: Path, document: dict[str, object]) -> None:
        if destination.name == "prepare-report.json":
            raise OSError("simulated report write failure")
        write_job(destination, document)

    monkeypatch.setattr("forge.historical.normalization.batch.write_job", fail_report)
    with pytest.raises(OSError, match="report write failure"):
        publish_batch(paths, failed, wrapper)
    assert not failed.run.exists()
    assert not list(paths.runs_dir.glob(".prepare-normalize-*"))


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


@pytest.mark.parametrize("positions", [{}, {1: 100, 2: 3, 3: 3}])
def test_rank_order_is_independent_of_input_order(positions: dict[int, int]) -> None:
    streams = {"D": 4, "C": 3, "B": 2, "A": 1}
    before = positions.copy()
    ranks = dense_ranks(streams, positions)
    assert ranks == dense_ranks(dict(reversed(list(streams.items()))), positions)
    assert positions == before
    assert [item["stream_rank"] for item in ranks] == [1, 2, 3, 4]
    assert [item["stream_id"] for item in ranks] == (
        ["B", "C", "A", "D"] if positions else ["A", "B", "C", "D"]
    )


def test_unresolved_scopes_follow_ranked_concrete_streams() -> None:
    assert dense_ranks({"family:a": None, "concrete:z": 1, "unranked:b": 2}, {1: 10}) == [
        {"stream_id": "concrete:z", "stream_rank": 1},
        {"stream_id": "family:a", "stream_rank": 2},
        {"stream_id": "unranked:b", "stream_rank": 3},
    ]
