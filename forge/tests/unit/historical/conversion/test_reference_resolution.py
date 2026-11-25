"""Automatic selection discovers reachable references before considering priorities."""

from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from pydantic import ValidationError

from forge.historical.conversion import resolution
from forge.historical.conversion.contract import Policy
from forge.historical.conversion.selection import ReferenceOptions, ReferenceSelection
from forge.shared.errors import MarketForgeError
from forge.shared.paths import ProjectPaths


def trade(
    symbol: str,
    base: str,
    quote: str,
    exchange: str = "binance",
    **values: Any,
) -> dict[str, Any]:
    return {
        "stream_id": f"{exchange}.spot.spot.{symbol}.trades",
        "kind": "trade",
        "market_observed": True,
        "identity": {
            "instrument_kind": "spot",
            "exchange": exchange,
            "symbol": symbol,
        },
        "market_category": "spot",
        "base_currency": base,
        "quote_currency": quote,
        **values,
    }


def publication(*streams: dict[str, Any], **values: Any) -> dict[str, Any]:
    return {
        "streams": list(streams),
        "mode": "trades",
        "registry_publication_id": 1375,
        "engine_publication_id": "624",
        "root": "/merged/624",
        "observed_interval_inclusive": {
            "start_ns": 1795046400000000000,
            "end_ns": 1795305600000000000,
        },
        **values,
    }


def test_legacy_manual_and_automatic_omission_round_trip() -> None:
    bare = ReferenceOptions()
    assert bare.selection_policy(1).mode == "manual"
    assert bare.selection_policy(2).mode == "automatic"
    assert (
        ReferenceOptions.model_validate(bare.model_dump()).selection_policy(2).mode == "automatic"
    )
    for ids in ([], ["exact.trades"]):
        override = ReferenceOptions(reference_stream_ids=ids)
        override.validate_reference_options(2)
        assert (
            ReferenceOptions.model_validate(override.model_dump()).selection_policy(2).mode
            == "manual"
        )
    with pytest.raises(ValueError, match="Remove reference_stream_ids"):
        ReferenceOptions(
            reference_stream_ids=[], reference_selection=ReferenceSelection()
        ).validate_reference_options(2)
    with pytest.raises(ValidationError, match="exact ranking snapshot"):
        ReferenceSelection(temporal_policy="ex_post")
    assert ReferenceSelection.model_validate({"ranking_snapshot": str(uuid4())}).ranking_snapshot


@pytest.mark.asyncio
async def test_exact_eth_bridge_and_identity_without_ranking(tmp_path: Path) -> None:
    source = publication(
        trade("ETHUSDC", "ETH", "USDC"),
        trade("ETHUSDT", "ETH", "USDT"),
        trade("ABSENT", "USDC", "USDT", market_observed=False),
    )
    report: dict[str, Any] = {}
    selected = await resolution.resolve_references(
        ProjectPaths(tmp_path, tmp_path / "data"),
        source,
        None,
        Policy(target_quote="USDT", max_staleness_ns=10),
        ReferenceSelection(),
        [],
        report,
    )
    assert [item["stream_id"] for item in selected] == [
        "binance.spot.spot.ETHUSDC.trades",
        "binance.spot.spot.ETHUSDT.trades",
    ]
    assert report["routes"]["USDC"]["potential"] == [
        (("USDC", "ETH", "USDT"), tuple(item["stream_id"] for item in selected))
    ]
    assert report["routes"]["USDT"]["potential"] == [(("USDT",), ())]
    assert report["ranking"]["status"] == "Ranking not required for route selection."
    assert report["candidates"][-1]["eligible"] is False


@pytest.mark.asyncio
async def test_parallel_alternatives_require_ranking_only_for_primary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    left = trade("ETHUSDT", "ETH", "USDT")
    right = trade("ETHUSDT", "ETH", "USDT", "bybit")
    source = publication(trade("ETHUSDC", "ETH", "USDC"), left, right)
    ranking = AsyncMock(return_value=({}, {"status": "Unavailable."}))
    monkeypatch.setattr(resolution, "rank_candidates", ranking)
    paths = ProjectPaths(tmp_path, tmp_path / "data")
    all_routes = Policy(target_quote="USDT", max_staleness_ns=10)
    report: dict[str, Any] = {}
    selected = await resolution.resolve_references(
        paths, source, None, all_routes, ReferenceSelection(), [], report
    )
    assert len(selected) == 3
    assert report["routes"]["USDC"]["preference_order"] is None

    primary = all_routes.model_copy(update={"path_policy": "primary"})
    with pytest.raises(MarketForgeError, match="Ambiguous.*USDC"):
        await resolution.resolve_references(
            paths, source, None, primary, ReferenceSelection(), [], {}
        )
    ranking.return_value = ({left["stream_id"]: 2, right["stream_id"]: 1}, {"status": "historical"})
    selected = await resolution.resolve_references(
        paths, source, None, primary, ReferenceSelection(), [], report
    )
    assert right in [{key: candidate[key] for key in right} for candidate in selected]
    assert left["stream_id"] not in {candidate["stream_id"] for candidate in selected}
    assert report["routes"]["USDC"]["selected_primary"][1][-1] == right["stream_id"]

    manual = await resolution.resolve_references(
        paths, source, None, primary, ReferenceSelection(mode="manual"), [left["stream_id"]], {}
    )
    assert [item["stream_id"] for item in manual] == [left["stream_id"]]


@pytest.mark.asyncio
async def test_missing_route_and_combined_internal_precedence(tmp_path: Path) -> None:
    paths = ProjectPaths(tmp_path, tmp_path / "data")
    pair = trade("USDCUSDT", "USDC", "USDT")
    source = publication(
        trade("ETHUSDC", "ETH", "USDC", kind="depth"),
        {**pair, "market_observed": False},
        mode="combined",
    )
    external = publication(pair, registry_publication_id=222, root="/merged/222")
    report: dict[str, Any] = {}
    with pytest.raises(MarketForgeError, match="No eligible.*USDC.*ETHUSDC"):
        await resolution.resolve_references(
            paths,
            source,
            external,
            Policy(target_quote="USDT", max_staleness_ns=10),
            ReferenceSelection(),
            [],
            report,
        )
    assert "internal trade precedence" in report["candidates"][-1]["reason"]
    depth = publication(source["streams"][0], mode="depth")
    future = {**pair, "observed_interval_inclusive": {"start_ns": 1795305600000000001}}
    with pytest.raises(MarketForgeError, match="No eligible automatic reference route"):
        await resolution.resolve_references(
            paths,
            depth,
            {**external, "streams": [future]},
            Policy(target_quote="USDT", max_staleness_ns=10),
            ReferenceSelection(),
            [],
            report,
        )
    assert "begins after all source events" in report["candidates"][-1]["reason"]
    chosen = await resolution.resolve_references(
        paths,
        depth,
        external,
        Policy(target_quote="USDT", max_staleness_ns=10),
        ReferenceSelection(),
        [],
        {},
    )
    assert chosen[0]["provider"] == "external"
    assert chosen[0]["merge_registry_publication_id"] == 222


@pytest.mark.asyncio
async def test_retained_priorities_require_historical_identity_and_comparable_entries(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from contextlib import asynccontextmanager
    from datetime import UTC, datetime, timedelta
    from decimal import Decimal

    from forge.markets.ranking.historical import turnover_entry_issue
    from forge.markets.ranking.policy import RankingPolicy

    class Connection:
        execute = AsyncMock()

        @asynccontextmanager
        async def transaction(self):
            yield

    @asynccontextmanager
    async def database(_dsn: str):
        yield Connection()

    cutoff = datetime(2026, 11, 19, tzinfo=UTC)
    observed = cutoff - timedelta(minutes=10)
    snapshot = uuid4()
    header = {
        "id": snapshot,
        "policy_name": "turnover",
        "policy_json": RankingPolicy().model_dump(mode="json"),
        "created_at": observed,
        "published_at": observed,
    }

    def entry(
        stream: dict[str, Any], position: int, unit: str, amount: str, **changes: Any
    ) -> dict[str, Any]:
        identity = stream["identity"]
        return {
            "instrument_reference": resolution.ranking_keys(stream)[0],
            "instrument_metadata": {
                "symbol": identity["symbol"],
                "exchange": identity["exchange"],
                "base": stream["base_currency"],
                "quote": stream["quote_currency"],
                "instrument_type": identity["instrument_kind"],
                "market": stream["market_category"],
            },
            "position": position,
            "score": Decimal(100 - position),
            "native_unit": unit,
            "native_turnover": Decimal(amount),
            "fallback_reason": None,
            "exclusion_reason": None,
            "turnover_estimated": False,
            "window_start": observed - timedelta(days=1),
            "window_end": observed,
            "source_observed_at": observed,
            "fetched_at": observed,
            "conversion_evidence": {"comparison_currency": "USDT"},
            **changes,
        }

    a = trade("ETHUSDT", "ETH", "USDT")
    b = trade("USDTETH", "USDT", "ETH", "bybit")
    c = trade("ETHUSDT", "ETH", "USDT", "okx")
    d = trade("ETHUSDT", "ETH", "USDT", "bitget")
    e = trade("ETHUSDT", "ETH", "USDT", "gateio")
    rows = [
        entry(a, 2, "USDT", "1000"),
        entry(b, 1, "ETH", "14"),
        entry(c, 3, "USDT", "50"),
        entry(d, 4, "USDT", "10", fetched_at=cutoff + timedelta(seconds=1)),
    ]
    rows[2].update(position=None, exclusion_reason="No comparable valuation")
    assert "comparable 24-hour window" in str(
        turnover_entry_issue(
            {**rows[0], "window_start": observed - timedelta(hours=21)},
            header,
            cutoff,
            ex_post=False,
        )
    )
    assert "Incomplete" in str(
        turnover_entry_issue(
            {**rows[0], "fallback_reason": "partial refresh input"},
            header,
            cutoff,
            ex_post=False,
        )
    )
    assert "Estimated turnover excluded" in str(
        turnover_entry_issue(
            {**rows[0], "turnover_estimated": True},
            header,
            cutoff,
            ex_post=False,
        )
    )
    candidates = {stream["stream_id"]: stream for stream in (a, b, c, d, e)}
    lookup = AsyncMock(return_value=(header, rows, "historical"))
    monkeypatch.setattr(resolution, "database_connection", database)

    def dsn(_root: Path) -> str:
        return "fixture"

    monkeypatch.setattr(resolution, "database_dsn", dsn)
    monkeypatch.setattr(resolution, "turnover_evidence", lookup)
    paths = ProjectPaths(tmp_path, tmp_path / "data")
    source = publication(
        observed_interval_inclusive={
            "start_ns": int(cutoff.timestamp()) * 10**9,
            "end_ns": int(cutoff.timestamp()) * 10**9,
        }
    )
    positions, evidence = await resolution.rank_candidates(
        paths,
        source,
        ReferenceSelection(ranking_snapshot=snapshot),
        candidates,
        set(candidates),
        needed=True,
    )
    assert positions == {a["stream_id"]: 2, b["stream_id"]: 1}
    assert evidence["snapshot"]["id"] == str(snapshot)
    assert len(evidence["evidence_sha256"]) == 64
    assert evidence["unusable_entries"][c["stream_id"]] == "No comparable valuation"
    assert "unavailable at the historical cutoff" in evidence["unusable_entries"][d["stream_id"]]
    assert "No unique ranking entry" in evidence["unusable_entries"][e["stream_id"]]
    assert lookup.await_args is not None and lookup.await_args.args[3] == cutoff

    lookup.return_value = (header, rows, "ex_post")
    positions, evidence = await resolution.rank_candidates(
        paths,
        source,
        ReferenceSelection(ranking_snapshot=snapshot, temporal_policy="ex_post"),
        candidates,
        set(candidates),
        needed=True,
    )
    assert positions[d["stream_id"]] == 4 and evidence["applicability"] == "ex_post"
    assert "may postdate" in evidence["temporal_note"]


def test_ranked_routes_have_stable_ties() -> None:
    first: resolution.Route = (("USDC", "ETH", "USDT"), ("common", "a"))
    second: resolution.Route = (("USDC", "ETH", "USDT"), ("common", "b"))
    assert resolution.ranked_routes([second, first], {"b": 1, "a": 1}) == [first, second]
