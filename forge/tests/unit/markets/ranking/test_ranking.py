"""Comparable turnover, deterministic preferences, and concrete stream identity."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from forge.markets.models import MarketDataError
from forge.markets.ranking.policy import RankingPolicy
from forge.markets.ranking.service import ConcreteStream, RankingInput, evaluate, resolve_streams


def item(index: int, ref: str, unit: str = "USDT") -> RankingInput:
    now = datetime(2026, 11, 4, tzinfo=UTC)
    return RankingInput(index, ref, index, Decimal(10), unit, False, now, now, True)


def test_ties_preferences_and_currency_exclusion_are_explicit() -> None:
    now = datetime(2026, 11, 4, tzinfo=UTC)
    inputs = [
        item(1, "bybit.spot.BTCUSDT"),
        item(2, "binance.spot.BTCUSDT"),
        item(3, "okx.inverse.BTC-USD-SWAP", "USD"),
    ]
    with pytest.raises(MarketDataError):
        evaluate(inputs, RankingPolicy(), now)
    policy = RankingPolicy(allow_partial_universe=True)
    rows = evaluate(inputs, policy, now)
    assert [r.reference for r in rows[:2]] == ["binance.spot.BTCUSDT", "bybit.spot.BTCUSDT"]
    assert rows[-1].exclusion_reason == "no explicit conversion for USD"
    preferred = policy.model_copy(update={"manual_preferences": ["bybit.spot.BTCUSDT"]})
    assert evaluate(inputs, preferred, now)[0].reference == "bybit.spot.BTCUSDT"
    assert evaluate(list(reversed(inputs)), preferred, now) == evaluate(inputs, preferred, now)


def test_staleness_and_incomplete_inputs_rejected() -> None:
    now = datetime(2026, 11, 4, tzinfo=UTC)
    with pytest.raises(MarketDataError):
        evaluate([item(1, "bybit.spot.BTCUSDT")], RankingPolicy(), now + timedelta(hours=2))


def test_stream_rank_resolution_preserves_types_and_gate_feeds() -> None:
    streams = [
        ConcreteStream(
            instrument_reference="gateio.spot.BTC_USDT", data_type="order_book_l2", feed=f
        )
        for f in ("snapshot", "incremental")
    ]
    streams.append(ConcreteStream(instrument_reference="gateio.spot.BTC_USDT", data_type="trades"))
    resolved = resolve_streams({"gateio.spot.BTC_USDT": 2}, streams)
    assert [r.stream_rank for r in resolved] == [1, 2, 3]
    assert len({r.name for r in resolved}) == 3
    with pytest.raises(ValueError):
        ConcreteStream(instrument_reference="gateio.spot.BTC_USDT", data_type="order_book_l2")


def test_explicit_conversion_and_partial_inputs_leave_provenance() -> None:
    from dataclasses import replace

    now = datetime(2026, 11, 4, tzinfo=UTC)
    policy = RankingPolicy.model_validate(
        {
            "comparison_currency": "USDT",
            "allow_partial_universe": True,
            "conversion_rates": {"USD": "0.99"},
            "conversion_as_of": now,
            "conversion_source": "explicit approximation for this snapshot",
        }
    )
    entry = evaluate(
        [replace(item(1, "bybit.inverse.BTCUSD", "USD"), refresh_complete=False)], policy, now
    )[0]
    assert entry.score == Decimal("9.90")
    assert (
        entry.fallback_reason is not None
        and "partial refresh" in entry.fallback_reason
        and "explicit conversion" in entry.fallback_reason
    )
