"""CLI forwards combined filters and limits human ranking output only."""

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

from forge.cli.commands import markets, rankings
from forge.cli.main import main

SNAPSHOT = UUID("00000000-0000-0000-0000-000000000001")


@pytest.mark.parametrize("command", ["list", "turnover", "volume"])
@pytest.mark.parametrize("as_json", [False, True])
@pytest.mark.parametrize("metadata_source", ["publication", "current_catalogue"])
def test_combined_filters_and_display_limit(
    workspace: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    command: str,
    as_json: bool,
    metadata_source: str,
) -> None:
    def dsn(root: Path) -> str:
        return "controlled"

    @asynccontextmanager
    async def connection(dsn: str) -> AsyncGenerator[AsyncConnection[TupleRow]]:
        yield cast(AsyncConnection[TupleRow], object())

    rows: list[dict[str, object]] = [
        {
            "symbol": f"ETH{position}USDT",
            "reference": f"bybit.spot.ETH{position}USDT",
            "instrument_reference": f"bybit.spot.ETH{position}USDT",
            "exchange": "bybit",
            "instrument_type": "spot",
            "market_category": "spot",
            "base_asset": "ETH",
            "quote_asset": "USDT",
            "position": position,
            "group_rank": group_rank,
            "score": 10,
            "exclusion_reason": None,
            "volume_unit": "ETH",
            "volume_estimated": False,
            "metadata_sources": {
                "symbol": metadata_source,
                "base_asset": metadata_source,
                "quote_asset": metadata_source,
            },
        }
        for position, group_rank in ((4, 2), (7, 5))
    ]

    async def browse(
        c: AsyncConnection[TupleRow],
        *,
        exchanges: list[str] | None,
        markets: list[str] | None,
        symbol: str | None,
        limit: int,
        sort: str,
        reference: str | None,
        base_asset: str | None,
        quote_asset: str | None,
        match: str | None,
    ) -> list[dict[str, object]]:
        assert base_asset == "eTh" and quote_asset == "usdt" and match == "*UsDt"
        assert limit == 2 and symbol is None
        return rows

    async def latest(c: AsyncConnection[TupleRow], name: str) -> UUID:
        assert name == command
        return SNAPSHOT

    async def show(
        c: AsyncConnection[TupleRow],
        snapshot: UUID,
        *,
        instrument_type: str | None,
        base_asset: str | None,
        quote_asset: str | None,
        match: str | None,
    ) -> dict[str, object]:
        assert (
            snapshot == SNAPSHOT
            and base_asset == "eTh"
            and quote_asset == "usdt"
            and match == "*UsDt"
        )
        return {
            "snapshot": {
                "id": SNAPSHOT,
                "policy_name": command,
                "created_at": datetime.now(UTC),
                "policy_json": {"metric": "volume" if command == "volume" else "turnover_24h"},
                "comparison_currency": None if command == "volume" else "USDT",
                "partial_universe": False,
                "included_count": 10,
                "universe_count": 10,
            },
            "entries": rows,
            "base_asset": base_asset,
            "quote_asset": quote_asset,
            "match": match,
        }

    monkeypatch.setattr(markets, "testing_dsn", dsn)
    monkeypatch.setattr(markets, "testing_connection", connection)
    monkeypatch.setattr(markets, "browse", browse)
    monkeypatch.setattr(rankings, "testing_dsn", dsn)
    monkeypatch.setattr(rankings, "testing_connection", connection)
    monkeypatch.setattr(rankings, "latest_snapshot", latest)
    monkeypatch.setattr(rankings, "show", show)
    for identifier in (command, str(SNAPSHOT)) if command != "list" else ("list",):
        action = ["list"] if command == "list" else ["rank", "show", identifier]
        assert (
            main(
                [
                    "--project-root",
                    str(workspace),
                    *(["--json"] if as_json else []),
                    "markets",
                    *action,
                    "--testing",
                    "--base-asset",
                    "eTh",
                    "--quote-asset",
                    "usdt",
                    "--match",
                    "*UsDt",
                    "--limit",
                    "1",
                ]
            )
            == 0
        )
        captured = capsys.readouterr()
        assert captured.err == "" and "\x1b" not in captured.out
        if as_json:
            value = json.loads(captured.out)
            if command == "list":
                assert len(value["instruments"]) == 1 and value["truncated"]
            else:
                assert [row["position"] for row in value["entries"]] == [4, 7]
                assert [row["group_rank"] for row in value["entries"]] == [2, 5]
        else:
            assert "ETH4USDT" in captured.out and "ETH7USDT" not in captured.out
            if command != "list" and metadata_source == "current_catalogue":
                assert "older entries" in captured.out


@pytest.mark.parametrize("command", [["markets", "list"], ["markets", "rank", "show"]])
def test_filter_help_documents_shell_quoting(
    command: list[str], capsys: pytest.CaptureFixture[str]
) -> None:
    with pytest.raises(SystemExit) as result:
        main([*command, "--help"])
    assert result.value.code == 0
    text = capsys.readouterr().out
    assert "--quote-asset" in text and "--base-asset" in text
    assert "'ETH*'" in text and "'*BTC'" in text
