"""Dense priority ordering and collision checks are independent of engine execution."""

from pathlib import Path

import pytest

from forge.historical.normalization.batch import allocate, dense_ranks
from forge.shared.paths import ProjectPaths


@pytest.mark.parametrize("positions", [{1: 3, 2: 5, 3: 10, 4: 100}, {1: 5, 2: 5}, {}])
def test_dense_ranking_sparse_ties_and_missing(positions: dict[int, int]) -> None:
    streams = {"z": 5, "d": 4, "c": 3, "b": 2, "a": 1}
    rows = dense_ranks(streams, positions)
    assert [row["stream_id"] for row in rows] == ["a", "b", "c", "d", "z"]
    assert [row["stream_rank"] for row in rows] == [1, 2, 3, 4, 5]
    assert dense_ranks({"only": 5}, positions) == [{"stream_id": "only", "stream_rank": 1}]
    assert dense_ranks({"b": 1, "a": 1}, {1: 9}) == [
        {"stream_id": "a", "stream_rank": 1},
        {"stream_id": "b", "stream_rank": 2},
    ]


def test_id_collisions_and_unused_output(tmp_path: Path) -> None:
    paths = ProjectPaths(root=tmp_path, data_root=tmp_path / "data")
    first = allocate("seed", set(), paths)
    assert allocate("seed", set(), paths) == first
    used = {first}
    second = allocate("seed", used, paths)
    assert second != first
    target = paths.data_root / "test/normalized" / str(first)
    target.mkdir(parents=True)
    assert allocate("seed", set(), paths) == second
    target.rmdir()
    target.symlink_to(tmp_path / "missing")
    assert allocate("seed", set(), paths) == second


def test_dense_ranking_uses_positions_before_names() -> None:
    assert dense_ranks({"a": 1, "b": 2, "z": 3}, {1: 100, 3: 3}) == [
        {"stream_id": "z", "stream_rank": 1},
        {"stream_id": "a", "stream_rank": 2},
        {"stream_id": "b", "stream_rank": 3},
    ]


@pytest.mark.asyncio
async def test_no_published_snapshot_is_optional() -> None:
    from typing import cast
    from unittest.mock import AsyncMock

    from psycopg import AsyncConnection
    from psycopg.rows import TupleRow

    from forge.historical.normalization.batch import ranking_positions

    c = AsyncMock()
    c.execute.return_value.fetchone.return_value = None
    snapshot, positions = await ranking_positions(cast(AsyncConnection[TupleRow], c))
    assert snapshot is None and positions == {}
    assert c.execute.await_count == 1


def test_prepared_run_jobs_are_visible_to_storage_dependencies(tmp_path: Path) -> None:
    import json

    from forge.storage.dependencies import prepared_references

    paths = ProjectPaths(root=tmp_path, data_root=tmp_path / "data")
    jobs = paths.runs_dir / "example/jobs/normalize"
    jobs.mkdir(parents=True)
    file = paths.raw_dir / "managed.gz"
    (jobs / "1.json").write_text(json.dumps({"tasks": [{"input_path": str(file)}]}))
    refs = prepared_references(paths)
    assert str(file) in refs.paths
    assert "Prepared job" in refs.paths[str(file)][0]


@pytest.mark.asyncio
async def test_dry_run_and_publication_share_read_only_planner(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import json
    import tomllib
    from collections.abc import AsyncGenerator
    from contextlib import asynccontextmanager
    from datetime import UTC, datetime
    from typing import cast
    from unittest.mock import patch

    from psycopg import AsyncConnection
    from psycopg.rows import TupleRow

    from forge.historical.archives.config import AcquisitionConfig
    from forge.historical.archives.templates import ACQUISITION_TEMPLATE
    from forge.historical.normalization.batch import BatchPlan, prepare_batch

    paths = ProjectPaths(root=tmp_path, data_root=tmp_path / "data")
    recipe = AcquisitionConfig.model_validate(tomllib.loads(ACQUISITION_TEMPLATE))
    run = paths.runs_dir / "fixture"
    plan = BatchPlan(
        {"status": "prepared", "date": datetime(2026, 1, 1, tzinfo=UTC)},
        {"schema_version": 3, "kind": "normalization_batch"},
        {
            7: {
                "output": {
                    "staging_path": str(tmp_path / "staging"),
                    "dataset_path": str(tmp_path / "dataset"),
                }
            }
        },
        run,
    )
    plan.batch["jobs"] = list(plan.jobs.values())
    queries: list[str] = []

    class Connection:
        @asynccontextmanager
        async def transaction(self) -> AsyncGenerator[None]:
            yield

        async def execute(self, query: str) -> None:
            assert query == "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"
            queries.append(query)

    @asynccontextmanager
    async def connect(dsn: str) -> AsyncGenerator[AsyncConnection[TupleRow]]:
        assert dsn == "fixture"
        yield cast(AsyncConnection[TupleRow], cast(object, Connection()))

    def dsn(root: Path) -> str:
        return "fixture"

    async def planner(
        c: AsyncConnection[TupleRow],
        paths: ProjectPaths,
        configuration: AcquisitionConfig,
        ranking: object = None,
    ) -> BatchPlan:
        assert configuration == recipe
        return plan

    from forge.historical.normalization.ranking import RankingSelection
    from forge.shared.config.models import HttpSettings

    @asynccontextmanager
    async def lock(c: AsyncConnection[TupleRow], *, shared: bool = False) -> AsyncGenerator[None]:
        yield

    async def ensure(
        c: AsyncConnection[TupleRow],
        dsn: str,
        directory: Path,
        policy: str,
        settings: HttpSettings,
        *,
        dry_run: bool,
    ) -> RankingSelection:
        return RankingSelection(None, {"decision": "would_create", "policy": policy})

    monkeypatch.setattr("forge.historical.normalization.batch.ranking_lock", lock)
    monkeypatch.setattr("forge.historical.normalization.batch.ensure_ranking", ensure)
    monkeypatch.setattr("forge.historical.normalization.batch.testing_connection", connect)
    monkeypatch.setattr("forge.historical.normalization.batch.testing_dsn", dsn)
    monkeypatch.setattr("forge.historical.normalization.batch.plan_batch", planner)
    with patch("subprocess.run", side_effect=AssertionError("Engine must never launch")):
        dry = await prepare_batch(paths, recipe, dry_run=True)
        assert dry["dry_run"] is True and not paths.runs_dir.exists()
        actual = await prepare_batch(paths, recipe)
    assert {**dry, "dry_run": False} == actual
    assert len(queries) == 2
    assert sorted(p.name for p in run.iterdir()) == ["normalize-batch.json", "prepare-report.json"]
    assert json.loads((run / "normalize-batch.json").read_text()) == plan.batch
    assert json.loads((run / "prepare-report.json").read_text()) == actual
