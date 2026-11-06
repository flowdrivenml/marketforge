"""CLI presentation/dispatch contracts using controlled service results."""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import cast
from uuid import uuid4

import pytest
from psycopg import AsyncConnection
from psycopg.rows import TupleRow

from forge.cli.commands import markets, rankings
from forge.cli.main import main
from forge.markets.models import Exchange, Market
from forge.markets.progress import CollectionProgress, ProgressCallback, ScopeProgress
from forge.shared.config.models import HttpSettings
from forge.shared.ownership import WorkspaceOwnership
from forge.shared.paths import ProjectPaths


@pytest.fixture(autouse=True)
def ranking_ownership(monkeypatch: pytest.MonkeyPatch) -> None:
    @asynccontextmanager
    async def lock(c: AsyncConnection[TupleRow], *, shared: bool = False) -> AsyncGenerator[None]:
        yield

    monkeypatch.setattr(rankings, "ranking_lock", lock)


def selected_dsn(root: Path) -> str:
    return "controlled-fixture"


@asynccontextmanager
async def connection(dsn: str) -> AsyncGenerator[AsyncConnection[TupleRow]]:
    yield cast(AsyncConnection[TupleRow], object())


@pytest.mark.parametrize("mode", ["human", "silent", "json"])
def test_collection_final_summaries_and_filters(
    workspace: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    mode: str,
) -> None:
    async def controlled(
        dsn: str,
        settings: HttpSettings,
        *,
        metadata: bool,
        exchanges: list[Exchange] | None,
        markets: list[Market] | None,
        symbol: str | None,
        progress: ProgressCallback | None,
    ) -> dict[str, object]:
        assert exchanges is None and markets is None and metadata
        assert progress is not None
        progress(
            CollectionProgress(
                True,
                (
                    ScopeProgress("bybit", "linear", "complete", 2),
                    ScopeProgress("okx", "inverse", "failed", 0),
                ),
            )
        )
        return {
            "status": "partial",
            "sources": [
                {
                    "exchange": "bybit",
                    "market": "linear",
                    "status": "complete",
                    "collected": 2,
                    "observations": 2,
                    "instrument_counts": {"future": 1},
                },
                {
                    "exchange": "okx",
                    "market": "inverse",
                    "status": "failed",
                    "collected": 0,
                    "error": "Controlled failure",
                },
            ],
        }

    monkeypatch.setattr(markets, "collect", controlled)
    monkeypatch.setattr(markets, "testing_dsn", selected_dsn)
    args = ["--project-root", str(workspace)]
    if mode == "json":
        args.append("--json")
    args.extend(["markets", "sync", "--testing"])
    if mode == "silent":
        args.append("--silent")
    assert main(args) == 1
    output = capsys.readouterr()
    assert "\x1b" not in output.out
    if mode == "json":
        import json

        assert json.loads(output.out)["status"] == "partial" and output.err == ""
    else:
        assert "Metadata records" in output.out and "Controlled failure" in output.out
        assert "Observations" not in output.out
        assert (output.err == "") == (mode == "silent")


def test_collection_uses_workspace_owner(
    workspace: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(markets, "testing_dsn", selected_dsn)
    with WorkspaceOwnership(ProjectPaths.resolve(root=workspace)):
        assert main(["--project-root", str(workspace), "markets", "sync", "--testing"]) != 0
    assert "Another historical application instance" in capsys.readouterr().err


def test_json_ranking_show_never_uses_human_display_limit(
    workspace: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    import json

    snapshot = uuid4()

    async def detail(c: AsyncConnection[TupleRow], identity: object) -> dict[str, object]:
        assert identity == snapshot
        return {
            "snapshot": {
                "id": str(snapshot),
                "policy_name": "test",
                "created_at": datetime.now(UTC),
                "comparison_currency": "USDT",
                "policy_json": {},
                "included_count": 75,
                "universe_count": 75,
                "partial_universe": False,
            },
            "entries": [
                {
                    "position": i,
                    "instrument_reference": f"bybit.spot.BTC{i}",
                    "score": Decimal("123.456789123456789"),
                    "exclusion_reason": None,
                }
                for i in range(1, 76)
            ],
        }

    monkeypatch.setattr(rankings, "testing_dsn", selected_dsn)
    monkeypatch.setattr(rankings, "testing_connection", connection)
    monkeypatch.setattr(rankings, "show", detail)
    assert (
        main(
            [
                "--project-root",
                str(workspace),
                "--json",
                "markets",
                "rank",
                "show",
                str(snapshot),
                "--testing",
                "--limit",
                "1",
            ]
        )
        == 0
    )
    output = capsys.readouterr()
    value = json.loads(output.out)
    assert len(value["entries"]) == 75
    assert value["entries"][0]["score"] == "123.456789123456789"
    assert output.err == "" and "\x1b" not in output.out


def test_json_exposes_all_conversion_evidence_despite_entry_selection_and_limit(
    workspace: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    import json

    snapshot = uuid4()
    evidence: dict[str, object] = {
        "method": "bridged",
        "rate": "1.0869565217391304347826086956521739130434782608696",
        "numerator": "25",
        "denominator": "23",
        "path": ["EUR", "BTC", "USDT"],
        "legs": [
            {"observation_id": 501, "original_price": "92000", "inverted": True},
            {"observation_id": 502, "original_price": "100000", "inverted": False},
        ],
    }

    async def detail(c: AsyncConnection[TupleRow], identity: object) -> dict[str, object]:
        return {
            "snapshot": {"id": str(snapshot)},
            "entries": [
                {
                    "position": 1,
                    "instrument_reference": "bybit.spot.BTCEUR",
                    "native_turnover": Decimal(92000),
                    "native_unit": "EUR",
                    "score": Decimal(100000),
                    "conversion_id": 7,
                    "conversion_evidence": evidence,
                },
                {
                    "position": 2,
                    "instrument_reference": "bybit.linear.BTCUSDT",
                    "native_turnover": Decimal(100),
                    "native_unit": "USDT",
                    "score": Decimal(100),
                },
            ],
            "conversions": [{"id": 7, "evidence_json": evidence}],
            "conversion_summary": {"bridged": 1, "identity": 1},
        }

    monkeypatch.setattr(rankings, "testing_dsn", selected_dsn)
    monkeypatch.setattr(rankings, "testing_connection", connection)
    monkeypatch.setattr(rankings, "show", detail)
    assert (
        main(
            [
                "--project-root",
                str(workspace),
                "--json",
                "markets",
                "rank",
                "show",
                str(snapshot),
                "--testing",
                "--conversion",
                "1",
                "--limit",
                "1",
            ]
        )
        == 0
    )
    output = capsys.readouterr()
    result = json.loads(output.out)
    assert (
        len(result["entries"]) == 2 and len(result["conversions"][0]["evidence_json"]["legs"]) == 2
    )
    assert result["conversions"][0]["evidence_json"]["rate"] == evidence["rate"]
    assert output.err == "" and "\x1b" not in output.out
