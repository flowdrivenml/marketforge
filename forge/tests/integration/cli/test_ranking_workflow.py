"""CLI orchestration with deterministic collection/build boundaries, no network."""

import json
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import cast
from uuid import UUID

import pytest
from psycopg import AsyncConnection
from psycopg.rows import TupleRow

from forge.cli.commands import initialization, rankings
from forge.cli.main import main
from forge.markets.models import MarketDataError, array, obj
from forge.markets.progress import ProgressCallback
from forge.markets.ranking import workflow
from forge.markets.ranking.policy import RankingPolicy
from forge.shared.config.models import HttpSettings
from forge.shared.database.migrations import MigrationResult

ROOT = Path(__file__).resolve().parents[4]
SNAPSHOT = UUID("00000000-0000-0000-0000-000000000001")


@pytest.fixture
def presets(workspace: Path) -> Path:
    directory = workspace / "data/.config/markets/rankings"
    directory.mkdir(parents=True)
    for name in ("turnover", "volume"):
        (directory / f"{name}.toml").write_bytes(
            (ROOT / f"data/.config/markets/rankings/{name}.toml").read_bytes()
        )
    return directory


@pytest.mark.parametrize("action", ["init", "update"])
@pytest.mark.parametrize("partial", [False, True])
def test_one_refresh_independent_policy_outcomes_and_safe_repeat(
    workspace: Path,
    presets: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    action: str,
    partial: bool,
) -> None:
    calls: list[bool] = []
    builds: list[str] = []
    migrations: list[str] = []
    originals = {p: p.read_bytes() for p in presets.iterdir()}

    def dsn(root: Path) -> str:
        assert root == workspace
        return "controlled"

    def migrate(*, dsn: str) -> MigrationResult:
        migrations.append(dsn)
        return MigrationResult("marketforge_test", (1,) if len(migrations) == 1 else (), ())

    async def collect(
        dsn: str, settings: HttpSettings, *, metadata: bool, progress: ProgressCallback | None
    ) -> dict[str, object]:
        calls.append(metadata)
        return {
            "status": "partial" if partial and not metadata else "complete",
            "refresh_id": 44,
            "sources": [
                {"exchange": "bybit", "market": "linear", "status": "complete"},
                {
                    "exchange": "bitget",
                    "market": "spot",
                    "status": "failed" if partial and not metadata else "complete",
                    "error": "Controlled Bitget spot failure",
                },
            ],
        }

    @asynccontextmanager
    async def connection(dsn: str) -> AsyncGenerator[AsyncConnection[TupleRow]]:
        yield cast(AsyncConnection[TupleRow], object())

    async def build(
        c: AsyncConnection[TupleRow], name: str, policy: RankingPolicy, *, refresh_id: int
    ) -> UUID:
        assert refresh_id == 44
        builds.append(name)
        return SNAPSHOT

    async def show(c: AsyncConnection[TupleRow], snapshot: UUID) -> dict[str, object]:
        return {"snapshot": {"created_at": datetime.now(UTC), "partial_universe": partial}}

    monkeypatch.setattr(initialization, "testing_dsn", dsn)
    monkeypatch.setattr(rankings, "testing_dsn", dsn)
    monkeypatch.setattr(initialization, "migrate_testing_database", migrate)
    monkeypatch.setattr(initialization, "collect", collect)
    monkeypatch.setattr(workflow, "collect", collect)
    monkeypatch.setattr(workflow, "testing_connection", connection)
    monkeypatch.setattr(workflow, "build", build)
    monkeypatch.setattr(workflow, "show", show)
    command = ["init"] if action == "init" else ["markets", "rank", "update"]
    for _ in range(2):
        assert main(["--project-root", str(workspace), "--json", *command, "--testing"]) == int(
            partial
        )
        captured = capsys.readouterr()
        assert captured.err == ""
        value = json.loads(captured.out)
        outcomes = {obj(r)["policy"]: obj(r) for r in array(value["rankings"])}
        assert outcomes["turnover"]["snapshot_id"] == str(SNAPSHOT)
        assert outcomes["volume"]["snapshot_id"] == str(SNAPSHOT)
        assert len(value["failed_scopes"]) == int(partial)
    assert calls == ([True, False] * 2 if action == "init" else [False] * 2)
    assert builds == ["turnover", "volume", "turnover", "volume"]
    assert len(migrations) == (2 if action == "init" else 0)
    assert all(path.read_bytes() == original for path, original in originals.items())


@pytest.mark.parametrize("identifier", ["turnover", str(SNAPSHOT)])
@pytest.mark.parametrize("as_json", [False, True])
def test_name_uuid_filter_before_limit_and_preserved_positions(
    workspace: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    identifier: str,
    as_json: bool,
) -> None:
    @asynccontextmanager
    async def connection(dsn: str) -> AsyncGenerator[AsyncConnection[TupleRow]]:
        yield cast(AsyncConnection[TupleRow], object())

    def dsn(root: Path) -> str:
        return "controlled"

    async def latest(c: AsyncConnection[TupleRow], name: str) -> UUID:
        assert name == "turnover"
        return SNAPSHOT

    async def show(
        c: AsyncConnection[TupleRow], snapshot: UUID, *, instrument_type: str
    ) -> dict[str, object]:
        assert snapshot == SNAPSHOT and instrument_type == "perpetual"
        return {
            "snapshot": {
                "id": SNAPSHOT,
                "policy_name": "turnover",
                "created_at": datetime(2026, 11, 5, tzinfo=UTC),
                "comparison_currency": "USDT",
                "policy_json": {},
                "included_count": 8,
                "universe_count": 9,
                "partial_universe": True,
            },
            "instrument_type": instrument_type,
            "entries": [
                {
                    "position": position,
                    "instrument_reference": f"bybit.{market}.PERP{position}",
                    "instrument_type": "perpetual",
                    "score": 100,
                    "exclusion_reason": None,
                }
                for position, market in ((3, "linear"), (7, "inverse"))
            ],
        }

    monkeypatch.setattr(rankings, "testing_dsn", dsn)
    monkeypatch.setattr(rankings, "testing_connection", connection)
    monkeypatch.setattr(rankings, "latest_snapshot", latest)
    monkeypatch.setattr(rankings, "show", show)
    args = [
        "--project-root",
        str(workspace),
        *(["--json"] if as_json else []),
        "markets",
        "rank",
        "show",
        identifier,
        "--testing",
        "--instrument-type",
        "perpetual",
        "--limit",
        "1",
    ]
    assert main(args) == 0
    output = capsys.readouterr()
    if as_json:
        assert [r["position"] for r in json.loads(output.out)["entries"]] == [3, 7]
    else:
        assert "PERP3" in output.out and "PERP7" not in output.out
        assert "PARTIAL" in output.out and "2026-11-05" in output.out
    assert output.err == ""


def test_missing_snapshot_explains_update(
    workspace: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    @asynccontextmanager
    async def connection(dsn: str) -> AsyncGenerator[AsyncConnection[TupleRow]]:
        yield cast(AsyncConnection[TupleRow], object())

    def dsn(root: Path) -> str:
        return "controlled"

    async def latest(c: AsyncConnection[TupleRow], name: str) -> UUID:
        raise MarketDataError(
            f"No published snapshot. marketforge markets rank update {name} --testing"
        )

    monkeypatch.setattr(rankings, "testing_dsn", dsn)
    monkeypatch.setattr(rankings, "testing_connection", connection)
    monkeypatch.setattr(rankings, "latest_snapshot", latest)
    assert (
        main(
            [
                "--project-root",
                str(workspace),
                "--json",
                "markets",
                "rank",
                "show",
                "volume",
                "--testing",
            ]
        )
        == 2
    )
    assert "rank update volume --testing" in capsys.readouterr().err


@pytest.mark.parametrize("failed", ["turnover", "volume", None])
def test_update_reports_independent_build_failures(
    workspace: Path,
    presets: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    failed: str | None,
) -> None:
    calls: list[str] = []

    def dsn(root: Path) -> str:
        return "controlled"

    def policy(path: Path) -> RankingPolicy:
        # Synthetic resolved policies test independent publication, not volume semantics.
        return RankingPolicy()

    @asynccontextmanager
    async def connection(dsn: str) -> AsyncGenerator[AsyncConnection[TupleRow]]:
        yield cast(AsyncConnection[TupleRow], object())

    async def collect(
        dsn: str, settings: HttpSettings, *, metadata: bool, progress: ProgressCallback | None
    ) -> dict[str, object]:
        calls.append("refresh")
        return {"status": "complete", "refresh_id": 44, "sources": []}

    async def build(
        c: AsyncConnection[TupleRow], name: str, policy: RankingPolicy, *, refresh_id: int
    ) -> UUID:
        calls.append(name)
        if name == failed:
            raise MarketDataError("Controlled policy rejection; previous snapshot retained")
        return SNAPSHOT

    async def show(c: AsyncConnection[TupleRow], snapshot: UUID) -> dict[str, object]:
        return {"snapshot": {"created_at": datetime.now(UTC), "partial_universe": False}}

    monkeypatch.setattr(rankings, "testing_dsn", dsn)
    monkeypatch.setattr(workflow, "collect", collect)
    monkeypatch.setattr(workflow, "load_policy", policy)
    monkeypatch.setattr(workflow, "testing_connection", connection)
    monkeypatch.setattr(workflow, "build", build)
    monkeypatch.setattr(workflow, "show", show)
    assert main(
        ["--project-root", str(workspace), "--json", "markets", "rank", "update", "--testing"]
    ) == int(failed is not None)
    result = json.loads(capsys.readouterr().out)
    assert calls == ["refresh", "turnover", "volume"]
    for outcome in result["rankings"]:
        assert outcome["status"] == ("failed" if outcome["policy"] == failed else "published")
        if outcome["status"] == "failed":
            assert "previous snapshot retained" in outcome["error"]


@pytest.mark.parametrize("selected", ["turnover", "volume"])
def test_update_selects_only_named_preset(
    workspace: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    selected: str,
) -> None:
    def dsn(root: Path) -> str:
        return "controlled"

    async def refresh_and_publish(
        dsn: str,
        settings: HttpSettings,
        directory: Path,
        names: tuple[str, ...],
        *,
        progress: ProgressCallback,
    ) -> dict[str, object]:
        assert names == (selected,)
        return {"schema_version": 1, "status": "complete", "rankings": []}

    monkeypatch.setattr(rankings, "testing_dsn", dsn)
    monkeypatch.setattr(rankings, "refresh_and_publish", refresh_and_publish)
    assert (
        main(
            [
                "--project-root",
                str(workspace),
                "--json",
                "markets",
                "rank",
                "update",
                selected,
                "--testing",
            ]
        )
        == 0
    )
    assert json.loads(capsys.readouterr().out)["status"] == "complete"


def test_init_connection_failure_is_actionable_and_does_not_collect(
    workspace: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from forge.shared.database.testing import TestingDatabaseError

    def dsn(root: Path) -> str:
        raise TestingDatabaseError("Set MARKETFORGE_DATABASE_URL_TEST explicitly.")

    monkeypatch.setattr(initialization, "testing_dsn", dsn)
    assert main(["--project-root", str(workspace), "--json", "init", "--testing"]) == 1
    captured = capsys.readouterr()
    value = json.loads(captured.out)
    assert value["stages"][-1]["stage"] == "migrations"
    assert "MARKETFORGE_DATABASE_URL_TEST" in value["stages"][-1]["error"]
    assert all(outcome["status"] == "failed" for outcome in value["rankings"])
    assert captured.err == ""


@pytest.mark.parametrize("as_json", [False, True])
def test_volume_validate_build_and_base_asset_filter(
    workspace: Path,
    presets: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    as_json: bool,
) -> None:
    args = ["--project-root", str(workspace), *(["--json"] if as_json else []), "markets", "rank"]
    assert main([*args, "validate", "volume"]) == 0
    captured = capsys.readouterr()
    assert "volume" in captured.out and captured.err == ""

    @asynccontextmanager
    async def connection(dsn: str) -> AsyncGenerator[AsyncConnection[TupleRow]]:
        yield cast(AsyncConnection[TupleRow], object())

    def dsn(root: Path) -> str:
        return "controlled"

    async def build(c: AsyncConnection[TupleRow], name: str, policy: RankingPolicy) -> UUID:
        assert name == "volume" and policy.metric == "volume" and not policy.conversion.automatic
        return SNAPSHOT

    async def latest(c: AsyncConnection[TupleRow], name: str) -> UUID:
        assert name == "volume"
        return SNAPSHOT

    async def show(
        c: AsyncConnection[TupleRow],
        snapshot: UUID,
        *,
        instrument_type: str | None,
        base_asset: str,
    ) -> dict[str, object]:
        assert base_asset == "ETH" and snapshot == SNAPSHOT
        return {
            "snapshot": {
                "id": SNAPSHOT,
                "policy_name": "volume",
                "created_at": datetime.now(UTC),
                "policy_json": {"metric": "volume"},
                "partial_universe": False,
            },
            "base_asset": "ETH",
            "entries": [
                {
                    "position": rank + 3,
                    "group_rank": rank,
                    "base_asset": "ETH",
                    "volume_unit": "ETH",
                    "volume_estimated": False,
                    "score": 10 - rank,
                    "instrument_reference": f"bybit.linear.ETH{rank}",
                    "exclusion_reason": None,
                    "instrument_type": "future",
                }
                for rank in (2, 3)
            ],
        }

    monkeypatch.setattr(rankings, "testing_dsn", dsn)
    monkeypatch.setattr(rankings, "testing_connection", connection)
    monkeypatch.setattr(rankings, "build", build)
    monkeypatch.setattr(rankings, "latest_snapshot", latest)
    monkeypatch.setattr(rankings, "show", show)
    assert main([*args, "build", "volume", "--testing"]) == 0
    capsys.readouterr()
    for identifier in ("volume", str(SNAPSHOT)):
        assert (
            main([*args, "show", identifier, "--testing", "--base-asset", "ETH", "--limit", "1"])
            == 0
        )
        captured = capsys.readouterr()
        if as_json:
            assert [r["group_rank"] for r in json.loads(captured.out)["entries"]] == [2, 3]
        else:
            assert (
                "Group rank" in captured.out
                and "ETH2" in captured.out
                and "ETH3" not in captured.out
            )
        assert captured.err == ""
