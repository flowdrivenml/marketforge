"""Deletion CLI selects one source and presents the actual transaction result."""

import json
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import cast
from uuid import UUID

import pytest
from psycopg import AsyncConnection
from psycopg.rows import TupleRow

from forge.cli.commands import rankings
from forge.cli.main import main
from forge.markets.ranking.deletion import selection
from forge.shared.paths import ProjectPaths

SNAPSHOT = str(UUID(int=1))


@pytest.mark.parametrize("args", [[], ["turnover", "--all"], ["volume", SNAPSHOT]])
def test_delete_requires_exactly_one_selector(workspace: Path, args: list[str]) -> None:
    with pytest.raises(SystemExit):
        main(["--project-root", str(workspace), "markets", "rank", "delete", *args, "--testing"])


@pytest.mark.parametrize("selector", ["turnover", "volume", SNAPSHOT, "--all"])
@pytest.mark.parametrize("apply", [False, True])
@pytest.mark.parametrize("as_json", [False, True])
def test_preview_apply_and_output(
    workspace: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    selector: str,
    apply: bool,
    as_json: bool,
) -> None:
    @asynccontextmanager
    async def connect(dsn: str) -> AsyncGenerator[AsyncConnection[TupleRow]]:
        yield cast(AsyncConnection[TupleRow], object())

    def dsn(root: Path) -> str:
        return "fixture"

    async def delete(
        c: AsyncConnection[TupleRow],
        paths: ProjectPaths,
        selected: str | None,
        *,
        all_snapshots: bool,
        apply: bool,
    ) -> dict[str, object]:
        assert selected == (None if selector == "--all" else selector)
        assert all_snapshots == (selector == "--all")
        return {
            "status": "complete",
            "action": "apply" if apply else "preview",
            "matched_count": 1,
            "eligible_count": 1,
            "deleted_count": int(apply),
            "blocked_count": 0,
            "deleted_ids": [SNAPSHOT] if apply else [],
            "message": "Resolved at apply time.",
            "snapshots": [
                {
                    "id": SNAPSHOT,
                    "policy_name": "turnover",
                    "created_at": "2001-01-01",
                    "included_count": 1,
                    "excluded_count": 0,
                    "result": "deleted" if apply else "eligible",
                    "blockers": [],
                }
            ],
        }

    monkeypatch.setattr(rankings, "testing_connection", connect)
    monkeypatch.setattr(rankings, "testing_dsn", dsn)
    monkeypatch.setattr(rankings, "delete_snapshots", delete)
    args = [
        "--project-root",
        str(workspace),
        *(["--json"] if as_json else []),
        "markets",
        "rank",
        "delete",
        selector,
        "--testing",
        *(["--apply"] if apply else []),
    ]
    assert main(args) == 0
    output = capsys.readouterr().out
    if as_json:
        assert json.loads(output)["deleted_count"] == int(apply)
    else:
        assert SNAPSHOT in output and ("deleted" if apply else "eligible") in output


def test_service_selection_defends_missing_and_conflicts() -> None:
    from forge.shared.errors import MarketForgeError

    for selector, all_snapshots in [(None, False), ("turnover", True)]:
        with pytest.raises(MarketForgeError, match="exactly one"):
            selection(selector, all_snapshots)
    assert selection(SNAPSHOT, False) == (UUID(SNAPSHOT), None)
    assert selection("turnover", False) == (None, "turnover")
    assert selection(None, True) == (None, None)
