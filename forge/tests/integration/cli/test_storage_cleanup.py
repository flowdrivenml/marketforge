"""Preview-first CLI dispatch, immutable review selection and explicit apply."""

import json
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Literal, cast

import pytest
from psycopg import AsyncConnection
from psycopg.rows import TupleRow

from forge.cli.commands import storage_cleanup
from forge.cli.main import main
from forge.shared.ownership import WorkspaceOwnership
from forge.shared.paths import ProjectPaths
from forge.storage.cleanup import digest
from forge.storage.cleanup_plans import CleanupPlan, Target
from forge.storage.models import StorageFilters


class Connection:
    @asynccontextmanager
    async def transaction(self) -> AsyncGenerator[None]:
        yield

    async def execute(self, query: str) -> None:
        assert query == "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"


@pytest.mark.parametrize("operation", ["prune", "delete"])
@pytest.mark.parametrize("busy", [False, True])
def test_json_preview_and_exact_saved_apply(
    workspace: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    operation: Literal["prune", "delete"],
    busy: bool,
) -> None:
    paths = ProjectPaths.resolve(root=workspace)
    applied: list[str] = []

    def dsn(root: Path) -> str:
        return "guarded fixture"

    @asynccontextmanager
    async def connect(dsn: str) -> AsyncGenerator[AsyncConnection[TupleRow]]:
        yield cast(AsyncConnection[TupleRow], cast(object, Connection()))

    async def controlled(
        c: AsyncConnection[TupleRow],
        paths: ProjectPaths,
        filters: StorageFilters,
        action: Literal["prune", "delete"],
        *,
        archive_id: int | None,
        overlap: bool,
        inactive: bool,
        review_all: bool,
    ) -> CleanupPlan:
        assert action == operation and inactive is not busy and review_all
        assert filters.instrument == "BTCUSDT" and filters.start is not None
        row: dict[str, object] = {
            "id": 536,
            "relative_path": "1/" + "a" * 32 + "/archive.csv",
            "status": "failed",
            "scope_identifier": "BTCUSDT",
        }
        target = Target(
            local_id=536,
            record=row,
            record_digest=digest(row),
            relative_path=str(row["relative_path"]),
            partial_relative_path=str(row["relative_path"]) + ".part",
            state="blocked" if busy else "eligible",
            blockers=["activity unknown"] if busy else [],
            action="none",
            actual_bytes=0,
        )
        return CleanupPlan(
            plan_id="test-plan",
            operation=action,
            project_root=str(paths.root),
            data_root=str(paths.data_root),
            raw_root=str(paths.raw_dir),
            created_at="2026-11-05T00:00:00Z",
            filters={**filters.model_dump(mode="json"), "archive_id": None},
            overlap=overlap,
            selected_count=1,
            review_complete=review_all,
            targets=[target],
        )

    async def apply(
        c: AsyncConnection[TupleRow], p: ProjectPaths, source: Path, plan: CleanupPlan
    ) -> dict[str, object]:
        assert plan.targets[0].local_id == 536
        applied.append(plan.plan_id)
        return {
            "schema_version": 1,
            "mode": "apply",
            "status": "partial" if busy else "complete",
            "outcomes": [],
        }

    monkeypatch.setattr(storage_cleanup, "testing_dsn", dsn)
    monkeypatch.setattr(storage_cleanup, "testing_connection", connect)
    monkeypatch.setattr(storage_cleanup, "preview", controlled)
    monkeypatch.setattr(storage_cleanup, "apply", apply)
    args = ["--project-root", str(workspace), "--json", "storage", operation, "--testing"]
    selected = [
        "--instrument",
        "BTCUSDT",
        "--start",
        "2026-10-27T00:00:00Z",
        "--end",
        "2026-10-30T00:00:00Z",
    ]
    if busy:
        with WorkspaceOwnership(paths):
            assert main([*args, *selected]) == 0
    else:
        assert main([*args, *selected]) == 0
    value = json.loads(capsys.readouterr().out)
    assert value["mode"] == "preview" and not applied
    assert value["totals"]["eligible"] == int(not busy)
    if busy:
        assert "apply_command" not in value and value["preview_path"] is None
        return
    assert main([*args, "--apply", "--preview", value["preview_path"]]) == int(busy)
    assert applied == ["test-plan"]
    assert json.loads(capsys.readouterr().out)["mode"] == "apply"


@pytest.mark.parametrize(
    "arguments",
    [
        ["delete"],
        ["delete", "--status", "complete"],
        ["delete", "--overlap", "--instrument", "BTCUSDT"],
        ["delete", "local:536", "--apply"],
    ],
)
def test_invalid_destructive_commands_fail_before_connection(
    workspace: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    arguments: list[str],
) -> None:
    def forbidden(root: Path) -> str:
        raise AssertionError("invalid request reached PostgreSQL")

    monkeypatch.setattr(storage_cleanup, "testing_dsn", forbidden)
    assert (
        main(["--project-root", str(workspace), "--json", "storage", *arguments, "--testing"]) == 2
    )
    assert json.loads(capsys.readouterr().err)["error"]


@pytest.mark.parametrize(
    "eligible,terminal,answer,expected",
    [
        (0, True, False, 0),
        (0, False, False, 0),
        (2, True, True, 0),
        (2, True, False, 0),
        (2, False, False, 2),
    ],
)
def test_prune_interactive_review_confirms_once_or_skips_prompt(
    workspace: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    eligible: int,
    terminal: bool,
    answer: bool,
    expected: int,
) -> None:
    from forge.startup.context import ApplicationContext

    events: list[str] = []
    before_prompt: list[str] = []

    def dsn(root: Path) -> str:
        return "fixture"

    @asynccontextmanager
    async def connect(dsn: str) -> AsyncGenerator[AsyncConnection[TupleRow]]:
        yield cast(AsyncConnection[TupleRow], cast(object, Connection()))

    async def controlled(
        c: AsyncConnection[TupleRow],
        paths: ProjectPaths,
        filters: StorageFilters,
        operation: Literal["prune", "delete"],
        *,
        archive_id: int | None,
        overlap: bool,
        inactive: bool,
        review_all: bool,
    ) -> CleanupPlan:
        events.append("preview")
        assert filters.status == "failed" and filters.limit == 1 and review_all == terminal
        targets: list[Target] = []
        for identity in (536, 537):
            row: dict[str, object] = {
                "id": identity,
                "relative_path": f"1/{identity:032x}/archive.csv",
                "status": "failed",
                "scope_identifier": "BTCUSDT",
                "scope_kind": "instrument",
                "exchange": "bybit",
                "instrument_type": "spot",
                "market_category": "spot",
                "data_type": "trades",
            }
            targets.append(
                Target(
                    local_id=identity,
                    record=row,
                    record_digest=digest(row),
                    relative_path=str(row["relative_path"]),
                    partial_relative_path=str(row["relative_path"]) + ".part",
                    state="eligible" if eligible else "blocked",
                    blockers=[] if eligible else ["Activity cannot be established"],
                    action="retire_absent_failed_reservation",
                    actual_bytes=0,
                )
            )
        return CleanupPlan(
            plan_id="interactive",
            operation=operation,
            project_root=str(paths.root),
            data_root=str(paths.data_root),
            raw_root=str(paths.raw_dir),
            created_at="2026-11-05T00:00:00Z",
            filters={**filters.model_dump(mode="json"), "archive_id": None},
            overlap=False,
            selected_count=2,
            review_complete=review_all,
            targets=targets,
        )

    def confirm(context: ApplicationContext, counts: dict[str, object]) -> bool:
        events.append("confirm")
        before_prompt.append(capsys.readouterr().out)
        assert "local:536" in before_prompt[0] and "local:537" in before_prompt[0]
        assert counts["eligible"] == 2 and counts["eligible_partial_files"] == 0
        return answer

    async def applied(
        c: AsyncConnection[TupleRow], paths: ProjectPaths, source: Path, plan: CleanupPlan
    ) -> dict[str, object]:
        events.append("apply")
        assert plan.review_complete and [t.local_id for t in plan.targets] == [536, 537]
        return {
            "operation": "prune",
            "mode": "apply",
            "status": "complete",
            "receipt_path": str(source.with_suffix(".apply.json")),
            "totals": {
                "selected": 2,
                "eligible": 2,
                "blocked": 0,
                "absent": 0,
                "reclaimable_bytes": 0,
            },
            "outcomes": [],
            "result_totals": {
                "retired_failed_reservations": 2,
                "partial_files_removed": 0,
                "reclaimed_bytes": 0,
            },
        }

    monkeypatch.setattr(storage_cleanup, "testing_dsn", dsn)
    monkeypatch.setattr(storage_cleanup, "testing_connection", connect)
    monkeypatch.setattr(storage_cleanup, "preview", controlled)
    monkeypatch.setattr(storage_cleanup, "apply", applied)
    monkeypatch.setattr(storage_cleanup, "interactive_terminal", lambda: terminal)
    monkeypatch.setattr(storage_cleanup, "confirm_prune", confirm)
    assert (
        main(
            [
                "--project-root",
                str(workspace),
                "storage",
                "prune",
                "--testing",
                "--apply",
                "--limit",
                "1",
            ]
        )
        == expected
    )
    captured = capsys.readouterr()
    if not eligible:
        assert events == ["preview"]
        assert "--apply" not in captured.out and "After reviewing" not in captured.out
    elif terminal:
        assert events == (["preview", "confirm", "apply"] if answer else ["preview", "confirm"])
        if answer:
            assert (
                "2 failed records retired · 0 partial files removed · 0 B reclaimed" in captured.out
            )
        else:
            assert "cancelled" in captured.out
    else:
        assert events == ["preview"] and "terminal input and output" in captured.err
    assert "\x1b" not in captured.out


def test_mf_console_alias_is_the_same_cli() -> None:
    import tomllib

    root = Path(__file__).resolve().parents[4]
    with (root / "forge/pyproject.toml").open("rb") as source:
        scripts = tomllib.load(source)["project"]["scripts"]
    assert scripts["mf"] == scripts["marketforge"] == "forge.cli.main:main"
