"""Automatic preparation priorities reuse the standard ranking workflow exactly once."""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import cast
from unittest.mock import AsyncMock
from uuid import UUID

import psycopg
import pytest
from psycopg import AsyncConnection
from psycopg.rows import TupleRow

from forge.historical.normalization import ranking
from forge.markets.models import obj
from forge.markets.ranking import workflow
from forge.markets.ranking.policy import RankingPolicy
from forge.shared.config.models import HttpSettings

SNAPSHOT = UUID(int=123)
ROOT = Path(__file__).resolve().parents[5]


def connection() -> tuple[AsyncConnection[TupleRow], AsyncMock]:
    mock = AsyncMock()
    cursor = AsyncMock()

    def cursor_factory(**kwargs: object) -> AsyncMock:
        return cursor

    mock.cursor = cursor_factory
    cursor.__aenter__.return_value = cursor
    cursor.fetchone.return_value = {
        "created_at": datetime(2001, 1, 1, tzinfo=UTC),
        "partial_universe": True,
        "metric": "turnover_24h",
    }
    return cast(AsyncConnection[TupleRow], mock), mock


@pytest.mark.asyncio
async def test_old_partial_snapshot_is_reused_without_policy_or_refresh(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    c, _ = connection()
    latest = AsyncMock(return_value=SNAPSHOT)
    update = AsyncMock(side_effect=AssertionError("Must reuse existing snapshot"))
    monkeypatch.setattr(ranking, "find_latest_snapshot", latest)
    monkeypatch.setattr(ranking, "refresh_and_publish", update)
    selected = await ranking.ensure_ranking(
        c, "dsn", tmp_path, "turnover", HttpSettings(), dry_run=False
    )
    assert selected.snapshot == SNAPSHOT
    assert selected.details["decision"] == "reused" and selected.details["partial_universe"] is True
    assert str(selected.details["created_at"]).startswith("2001-")
    latest.assert_awaited_once_with(c, "turnover")
    update.assert_not_awaited()


@pytest.mark.asyncio
async def test_missing_dry_run_has_pending_order_and_no_collection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    c, _ = connection()
    monkeypatch.setattr(ranking, "find_latest_snapshot", AsyncMock(return_value=None))
    update = AsyncMock(side_effect=AssertionError("Dry-run cannot collect"))
    monkeypatch.setattr(ranking, "refresh_and_publish", update)
    selected = await ranking.ensure_ranking(
        c, "dsn", ROOT / "data/.config/markets/rankings", "turnover", HttpSettings(), dry_run=True
    )
    assert selected.snapshot is None
    assert (
        selected.details["decision"] == "would_create" and selected.details["order_final"] is False
    )
    update.assert_not_awaited()


@pytest.mark.asyncio
async def test_missing_collects_once_builds_only_requested_policy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    c, _ = connection()
    monkeypatch.setattr(ranking, "find_latest_snapshot", AsyncMock(return_value=None))
    collect = AsyncMock(return_value={"status": "complete", "refresh_id": 42, "sources": []})
    builds: list[str] = []

    async def build(
        connection: AsyncConnection[TupleRow], name: str, policy: RankingPolicy, *, refresh_id: int
    ) -> UUID:
        assert refresh_id == 42 and policy.metric == "turnover_24h"
        builds.append(name)
        return SNAPSHOT

    @asynccontextmanager
    async def connect(dsn: str) -> AsyncGenerator[AsyncConnection[TupleRow]]:
        yield c

    monkeypatch.setattr(workflow, "collect", collect)
    monkeypatch.setattr(workflow, "build", build)
    monkeypatch.setattr(workflow, "testing_connection", connect)
    monkeypatch.setattr(
        workflow,
        "show",
        AsyncMock(
            return_value={"snapshot": {"created_at": "2001-01-01", "partial_universe": True}}
        ),
    )
    selected = await ranking.ensure_ranking(
        c, "dsn", ROOT / "data/.config/markets/rankings", "turnover", HttpSettings(), dry_run=False
    )
    assert selected.details["decision"] == "created" and selected.snapshot == SNAPSHOT
    assert builds == ["turnover"] and collect.await_count == 1
    assert collect.call_args.kwargs["metadata"] is False


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["collection", "build", "policy", "database"])
async def test_real_failures_are_not_missing_snapshot_fallbacks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    c, _ = connection()
    directory = ROOT / "data/.config/markets/rankings"
    latest = AsyncMock(return_value=None)
    monkeypatch.setattr(ranking, "find_latest_snapshot", latest)
    if failure == "database":
        latest.side_effect = psycopg.OperationalError("controlled database error")
        with pytest.raises(psycopg.OperationalError, match="controlled database error"):
            await ranking.ensure_ranking(
                c, "dsn", directory, "turnover", HttpSettings(), dry_run=False
            )
        return
    if failure == "policy":
        directory = tmp_path
        (directory / "turnover.toml").write_text("invalid = true")
    else:
        outcome: dict[str, object] = {
            "status": "failed",
            "rankings": [{"status": "failed", "error": "controlled build error"}],
            "refresh": {"status": "failed", "error": "controlled collection error"},
        }
        monkeypatch.setattr(ranking, "refresh_and_publish", AsyncMock(return_value=outcome))
    with pytest.raises(ranking.RankingPreparationError) as error:
        await ranking.ensure_ranking(c, "dsn", directory, "turnover", HttpSettings(), dry_run=False)
    assert error.value.details["decision"] == "failed"
    if failure != "policy":
        assert f"controlled {failure} error" in str(error.value)


@pytest.mark.asyncio
async def test_actual_collection_failure_is_retained(monkeypatch: pytest.MonkeyPatch) -> None:
    from forge.shared.errors import MarketForgeError

    c, _ = connection()
    monkeypatch.setattr(ranking, "find_latest_snapshot", AsyncMock(return_value=None))
    monkeypatch.setattr(
        workflow,
        "collect",
        AsyncMock(side_effect=MarketForgeError("Controlled upstream statistics failure")),
    )
    build = AsyncMock()
    monkeypatch.setattr(workflow, "build", build)
    with pytest.raises(
        ranking.RankingPreparationError, match="Controlled upstream statistics failure"
    ):
        await ranking.ensure_ranking(
            c,
            "dsn",
            ROOT / "data/.config/markets/rankings",
            "turnover",
            HttpSettings(),
            dry_run=False,
        )
    build.assert_not_awaited()


def test_missing_ranking_plan_is_provisional_and_does_not_publish(tmp_path: Path) -> None:
    from forge.historical.normalization.batch import build_plan
    from forge.historical.normalization.mapping import ResolvedArchive
    from forge.shared.paths import ProjectPaths

    paths = ProjectPaths(root=tmp_path, data_root=tmp_path / "data")
    selected = ranking.RankingSelection(
        None,
        {"decision": "would_create", "policy": "turnover", "snapshot": None, "order_final": False},
    )
    plan = build_plan(
        paths,
        "seed",
        None,
        {
            "trades": {
                ("local:1", "stream"): ResolvedArchive(
                    {"stream_id": "stream", "input_path": "/raw/archive"}, 42
                )
            }
        },
        [],
        False,
        None,
        {},
        set(),
        ranking=selected,
    )
    details = obj(plan.report["ranking_resolution"])
    assert details["decision"] == "would_create" and details["order_final"] is False
    assert details["matched_stream_count"] == 0 and details["unmatched_stream_count"] == 1
    assert plan.report["ranking_pending"] is True and plan.report["status"] == "partial"
    assert plan.report["prepared_task_count"] == 1
    assert not paths.runs_dir.exists() and not (paths.data_root / "test/normalized").exists()
