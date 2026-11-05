"""Human views retain units, identity, missing states, and bounded presentation."""

from datetime import UTC, datetime
from decimal import Decimal
from io import StringIO

import pytest

from forge.markets.progress import CollectionProgress, ScopeProgress
from forge.shared.terminal.console import create_console
from forge.shared.terminal.markets import (
    MarketCollectionDisplay,
    numeric,
    render_market_result,
    render_ranking_result,
)


def record(**changes: object) -> dict[str, object]:
    return {
        "reference": "bybit.linear.BTCUSDT",
        "symbol": "BTCUSDT",
        "exchange": "bybit",
        "instrument_type": "future",
        "market_category": "linear",
        "status": "active",
        "quote_asset": "USDT",
        "base_asset": "BTC",
        "settlement_asset": "USDT",
        "last_price": Decimal("0.0000000000123"),
        "turnover_24h": Decimal("1320000000"),
        "turnover_denomination": "USDT",
        "fetched_at": datetime(2026, 11, 5, tzinfo=UTC),
        "stale": False,
        **changes,
    }


@pytest.mark.parametrize("width", [48, 120])
def test_market_missing_stale_fresh_estimated_and_limit(width: int) -> None:
    output = StringIO()
    console = create_console(stream=output)
    console.width = width
    render_market_result(
        console,
        "list",
        {
            "instruments": [
                record(),
                record(stale=True, turnover_estimated=True),
                record(fetched_at=None, last_price=None),
            ],
            "truncated": True,
        },
        filters="exchange=bybit",
    )
    text = output.getvalue()
    for word in (
        "Fresh",
        "Stale",
        "Not",
        "collected",
        "USDT",
        "est.",
        "Displayed 3",
        "limit",
        "bybit",
    ):
        assert word in text
    assert "\x1b[" not in text
    assert "1.23e-11" in text.lower().replace("\n", "").replace(" ", "")


def test_futures_detail_and_empty_views() -> None:
    output = StringIO()
    console = create_console(stream=output)
    console.width = 120
    render_market_result(console, "show", {"instruments": [record(expiry_ns=1798185600000000000)]})
    text = output.getvalue()
    assert "2026-12-25" in text and "Contract value" in text and "—" in text
    assert "Observation timestamps" in text and "future / linear" in text
    render_market_result(console, "list", {"instruments": []})
    render_ranking_result(console, "list", {"rankings": []})
    text = output.getvalue()
    assert "No stored instruments" in text and "rank build NAME" in text


def test_rankings_display_stored_order_native_comparability_and_exclusions() -> None:
    output = StringIO()
    console = create_console(stream=output)
    console.width = 120
    entries: list[dict[str, object]] = [
        {
            "position": 1,
            "instrument_reference": "bybit.linear.ZZZ",
            "symbol": "ZZZ",
            "exchange": "bybit",
            "market_category": "linear",
            "instrument_type": "future",
            "score": Decimal(2),
        },
        {
            "position": 2,
            "instrument_reference": "bybit.spot.AAA",
            "symbol": "AAA",
            "exchange": "bybit",
            "market_category": "spot",
            "instrument_type": "spot",
            "score": Decimal(999),
        },
        *(
            {
                "position": None,
                "instrument_reference": f"bybit.spot.EXCLUDED{i}",
                "exclusion_reason": "no explicit conversion for USDC",
            }
            for i in range(82)
        ),
    ]
    header: dict[str, object] = {
        "id": "full-snapshot-identity",
        "policy_name": "manual",
        "created_at": datetime.now(UTC),
        "comparison_currency": "EUR",
        "policy_json": {"metric": "turnover_24h"},
        "included_count": 2,
        "universe_count": 84,
        "partial_universe": True,
    }
    render_ranking_result(console, "show", {"snapshot": header, "entries": entries}, limit=1)
    text = output.getvalue()
    assert "EUR" in text and "84" in text and "PARTIAL" in text
    assert "ZZZ" in text and "AAA" not in text
    assert "82 excluded: no explicit conversion for USDC" in text
    assert "EXCLUDED0" not in text and "--excluded" in text
    output.seek(0)
    output.truncate()
    render_ranking_result(
        console, "show", {"snapshot": header, "entries": entries}, limit=None, excluded=True
    )
    text = output.getvalue()
    assert text.index("ZZZ") < text.index("AAA") and "EXCLUDED81" in text


def test_numeric_never_rounds_small_nonzero_price_to_zero() -> None:
    assert numeric(Decimal("1e-30")) != "0"
    assert numeric(None) == "—"
    assert numeric(Decimal("1320000000")) == "1.32B"


def test_silent_and_redirected_progress_transitions_are_bounded() -> None:
    snapshot = CollectionProgress(
        True,
        (
            ScopeProgress("bybit", "linear", "complete", 5),
            ScopeProgress("okx", "inverse", "failed", 0),
        ),
    )
    output = StringIO()
    with MarketCollectionDisplay(create_console(stream=output), silent=True) as display:
        display.update(snapshot)
    assert output.getvalue() == ""
    with MarketCollectionDisplay(create_console(stream=output)) as display:
        display.update(snapshot)
        display.update(snapshot)
        assert display.live is None
    assert output.getvalue().count("bybit.linear:") == 1
    assert "5 metadata records" in output.getvalue() and "\x1b" not in output.getvalue()


def test_live_cleanup_on_failure_and_cancellation(monkeypatch: pytest.MonkeyPatch) -> None:
    from rich.live import Live

    class Terminal(StringIO):
        def isatty(self) -> bool:
            return True

    started: list[Live] = []
    stopped: list[Live] = []

    def start(self: Live, refresh: bool = False) -> None:
        started.append(self)

    def stop(self: Live) -> None:
        stopped.append(self)

    monkeypatch.setattr(Live, "start", start)
    monkeypatch.setattr(Live, "stop", stop)
    for exception in (RuntimeError, KeyboardInterrupt):
        display = MarketCollectionDisplay(create_console(stream=Terminal()))
        with pytest.raises(exception), display:
            raise exception()
        assert display.live is None
    assert len(started) == len(stopped) == 2


def test_elapsed_advances_without_new_events_and_recent_results_stay_bounded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import forge.shared.terminal.markets as views

    clock = [10.0]

    def current() -> float:
        return clock[0]

    monkeypatch.setattr(views, "monotonic", current)
    output = StringIO()
    console = create_console(stream=output)
    display = MarketCollectionDisplay(console, silent=True)
    scopes = tuple(ScopeProgress(f"exchange{i}", "linear", "complete", i) for i in range(14))
    display.update(CollectionProgress(True, scopes))
    assert len(display.recent) == 6
    clock[0] = 15.0
    console.print(display.render())
    assert "Elapsed 5s" in output.getvalue()
    output.seek(0)
    output.truncate()
    clock[0] = 20.0
    console.print(display.render())
    assert "Elapsed 10s" in output.getvalue()


@pytest.mark.parametrize("width", [48, 120])
def test_ranking_list_keeps_full_ids_copyable(width: int) -> None:
    identifier = "1ab3f908-a6af-4248-a51a-474c72284210"
    output = StringIO()
    console = create_console(stream=output)
    console.width = width
    render_ranking_result(
        console,
        "list",
        {
            "rankings": [
                {
                    "id": identifier,
                    "policy_name": "default",
                    "created_at": datetime.now(UTC),
                    "comparison_currency": "USDT",
                    "partial_universe": True,
                    "universe_count": 521,
                    "included_count": 389,
                }
            ]
        },
    )
    assert identifier in output.getvalue()
    assert "389 / 132" in output.getvalue() and "partial" in output.getvalue()


def test_conversion_summary_and_explicit_evidence_view() -> None:
    output = StringIO()
    console = create_console(stream=output)
    console.width = 120
    entry: dict[str, object] = {
        "position": 1,
        "instrument_reference": "bybit.spot.BTCEUR",
        "symbol": "BTCEUR",
        "exchange": "bybit",
        "instrument_type": "spot",
        "market_category": "spot",
        "score": Decimal(100000),
        "native_turnover": Decimal(92000),
        "native_unit": "EUR",
        "observation_id": 10,
        "conversion_evidence": {
            "method": "bridged",
            "rate": "1.0869565217391304347826086956521739130434782608696",
            "numerator": "25",
            "denominator": "23",
            "score_formula": "round80(Fraction(native_turnover) * numerator / denominator)",
            "path": ["EUR", "BTC", "USDT"],
            "legs": [
                {
                    "instrument_reference": "bybit.spot.BTCEUR",
                    "exchange": "bybit",
                    "observation_id": 11,
                    "from_currency": "EUR",
                    "to_currency": "BTC",
                    "inverted": True,
                    "original_price": "92000",
                    "effective_observed_at": "2026-11-05T00:00:00Z",
                    "fetched_at": "2026-11-05T00:00:00Z",
                },
                {
                    "instrument_reference": "bybit.spot.BTCUSDT",
                    "exchange": "bybit",
                    "observation_id": 12,
                    "from_currency": "BTC",
                    "to_currency": "USDT",
                    "inverted": False,
                    "original_price": "100000",
                    "effective_observed_at": "2026-11-05T00:00:00Z",
                    "fetched_at": "2026-11-05T00:00:00Z",
                },
            ],
        },
    }
    header: dict[str, object] = {
        "id": "pinned-id",
        "policy_name": "fx",
        "created_at": datetime.now(UTC),
        "comparison_currency": "USDT",
        "policy_json": {},
        "included_count": 1,
        "universe_count": 1,
        "partial_universe": False,
        "algorithm_version": "turnover24h-spotfx-rational80-v2",
    }
    value: dict[str, object] = {
        "snapshot": header,
        "entries": [entry],
        "conversion_summary": {"bridged": 1},
    }
    render_ranking_result(console, "show", value, conversion="bybit.spot.BTCEUR")
    text = output.getvalue()
    for expected in (
        "bridged 1",
        "Reference-price",
        "EUR -> BTC -> USDT",
        "92,000 EUR",
        "100,000 USDT",
        "inverse 1/price",
        "forward price",
        "observation 11",
        "observation 12",
    ):
        assert expected in text
    output.seek(0)
    output.truncate()
    entry["conversion_evidence"] = None
    value["conversion_summary"] = {"legacy": 1}
    render_ranking_result(console, "show", value, conversion="1")
    assert "Legacy snapshot" in output.getvalue() and "no rate is inferred" in output.getvalue()


@pytest.mark.parametrize("width", [48, 120])
def test_turnover_scope_field_and_interval_evidence_in_details(width: int) -> None:
    output = StringIO()
    console = create_console(stream=output)
    console.width = width
    render_market_result(
        console,
        "show",
        {
            "instruments": [
                record(
                    reference="bitget.spot.RSPYUSDT",
                    symbol="RSPYUSDT",
                    turnover_24h=Decimal("55046.2049"),
                    interpretation={
                        "activity_scope": "exchange_local",
                        "turnover_field": "platformTurnover24h",
                        "base_volume_scope": "broader source; omitted",
                        "interval_basis": "inferred_24h_from_source_timestamp",
                        "price_timestamp_basis": "source ticker time proxy",
                    },
                    mapping_version="current-metrics-v3-activity-scope",
                )
            ]
        },
    )
    text = output.getvalue()
    assert "exchange_local" in text
    assert "platformTurnover24h" in text.replace("\n", "").replace(" ", "")
    assert "55,046.2049" in text or "55046.2049" in text
    assert "\x1b[" not in text
