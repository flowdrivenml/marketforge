"""Activity scope and timestamp interpretation, using controlled public shapes."""

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from forge.markets.interpretation import turnover_issue
from forge.markets.models import Instrument
from forge.markets.providers.public import PublicProvider
from forge.markets.ranking.conversion import ConversionResolver, ConversionUnavailable, SpotPrice
from forge.markets.ranking.policy import RankingPolicy
from forge.markets.ranking.service import RankingInput, evaluate
from forge.shared.http.client import open_http_client
from forge.shared.http.rate_limit import RequestRateLimiter

NOW = datetime(2026, 11, 5, 2, 2, 25, tzinfo=UTC)
RTICKER: dict[str, object] = {
    "lastPrice": "765.76",
    "volume24h": "44668118.5557",
    "turnover24h": "34271765226.4751",
    "platformTurnover24h": "55046.2049",
}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "value,expected",
    [
        ("55046.2049", Decimal("55046.2049")),
        ("0", Decimal(0)),
        (None, None),
        ("bad", None),
        ("-1", None),
        ("NaN", None),
    ],
)
async def test_reality_metadata_selects_local_turnover_without_broader_base(
    value: object, expected: Decimal | None
) -> None:
    # Deliberately no R prefix: classification must use the documented flag.
    i = Instrument(
        exchange="bitget",
        market="spot",
        symbol="SPYUSDT",
        base="SPY",
        quote="USDT",
        raw={"isReality": "yes"},
    )
    ticker = {**RTICKER, "platformTurnover24h": value}
    async with open_http_client(limiter=RequestRateLimiter(1)) as client:
        o = PublicProvider("bitget", client).observation(i, ticker, fetched=NOW)
    assert o.turnover == expected and o.quote_volume == expected
    assert o.base_volume is None
    assert o.raw == ticker and o.raw["turnover24h"] == "34271765226.4751"
    assert o.interpretation["turnover_field"] == "platformTurnover24h"
    assert o.interpretation["activity_scope"] == "exchange_local"
    assert (
        "turnover_missing_reason" in o.interpretation
        if expected is None
        else "turnover_missing_reason" not in o.interpretation
    )


@pytest.mark.asyncio
async def test_r_prefix_does_not_classify_ordinary_instrument() -> None:
    async with open_http_client(limiter=RequestRateLimiter(1)) as client:
        provider = PublicProvider("bitget", client)
        i = Instrument(
            exchange="bitget",
            market="spot",
            symbol="RUNEUSDT",
            base="RUNE",
            quote="USDT",
            raw={"isReality": "no"},
        )
        o = provider.observation(i, RTICKER, fetched=NOW)
        unknown = provider.observation(i.model_copy(update={"raw": {}}), RTICKER, fetched=NOW)
    assert o.turnover == Decimal("34271765226.4751")
    assert o.base_volume == Decimal("44668118.5557")
    assert o.interpretation["turnover_field"] == "turnover24h"
    assert unknown.turnover is None and "unknown" in str(
        unknown.interpretation["turnover_missing_reason"]
    )


def test_legacy_compatibility_is_targeted_and_actionable() -> None:
    old = {"ticker": RTICKER, "instrument_specification": {"raw": {"isReality": "yes"}}}
    assert "legacy Bitget" in str(
        turnover_issue("bitget", "spot", "current-metrics-v2-native-money", old, {})
    )
    assert turnover_issue("bybit", "spot", "current-metrics-v1", old, {}) is None
    assert turnover_issue("bitget", "linear", "current-metrics-v1", old, {}) is None
    assert (
        turnover_issue(
            "bitget",
            "spot",
            "current-metrics-v1",
            {"ticker": {"turnover24h": "3"}},
            {"isReality": "no"},
        )
        is None
    )
    assert "legacy" in str(turnover_issue("bitget", "spot", "old", {}, {"isReality": "yes"}))
    assert (
        turnover_issue(
            "bitget",
            "spot",
            "old",
            {"ticker": {"turnover24h": "3.00", "platformTurnover24h": "3"}},
            {"isReality": "no"},
        )
        is None
    )


@pytest.mark.asyncio
async def test_binance_preserves_actual_bounds_and_old_source_despite_fresh_fetch() -> None:
    end = NOW - timedelta(seconds=376)
    start = end - timedelta(hours=23, minutes=58)
    ticker: dict[str, object] = {
        "openTime": int(start.timestamp() * 1000),
        "closeTime": int(end.timestamp() * 1000),
        "lastPrice": "1",
        "quoteVolume": "10",
    }
    i = Instrument(exchange="binance", market="spot", symbol="AMPUSDT", base="AMP", quote="USDT")
    async with open_http_client(limiter=RequestRateLimiter(1)) as client:
        o = PublicProvider("binance", client).observation(i, ticker, fetched=NOW)
    assert (o.window_start, o.window_end, o.source_observed_at, o.fetched_at) == (
        start,
        end,
        end,
        NOW,
    )
    assert o.interpretation["interval_basis"] == "source_supplied_openTime_closeTime"
    assert o.window_end - o.window_start != timedelta(hours=24)


@pytest.mark.asyncio
async def test_okx_estimate_arithmetic_and_explicit_policy() -> None:
    i = Instrument(
        exchange="okx", market="linear", symbol="AAVE-USDT-SWAP", base="AAVE", quote="USDT"
    )
    async with open_http_client(limiter=RequestRateLimiter(1)) as client:
        o = PublicProvider("okx", client).observation(
            i, {"last": "131.11", "volCcy24h": "358670.75"}, fetched=NOW
        )
    assert o.turnover == Decimal("47025322.0325") and o.turnover_estimated
    assert o.interpretation["calculation"] == "volCcy24h * last"
    a = RankingInput(1, i.reference, 1, o.turnover, "USDT", True, NOW, NOW, True)
    b = replace(a, instrument_id=2, reference="other", estimated=False)
    strict = RankingPolicy(allow_partial_universe=True)
    assert evaluate([a, b], strict, NOW)[-1].exclusion_reason == "estimated turnover excluded"
    assert (
        evaluate([a, b], strict.model_copy(update={"estimates": "allow"}), NOW)[0].score
        == o.turnover
    )


@pytest.mark.parametrize("seconds,excluded", [(899, False), (900, False), (901, True)])
def test_window_tolerance_diagnostics_and_independent_conversion_age(
    seconds: int, excluded: bool
) -> None:
    p = RankingPolicy.model_validate(
        {
            "version": 2,
            "max_window_skew_seconds": 900,
            "allow_partial_universe": True,
            "conversion": {"max_price_age_seconds": 300, "max_leg_skew_seconds": 10},
        }
    )
    a = RankingInput(1, "bybit.spot.A", 1, Decimal(10), "USDT", False, NOW, NOW, True)
    b = replace(
        a, instrument_id=2, reference="bybit.spot.B", window_end=NOW - timedelta(seconds=seconds)
    )
    result = evaluate([a, b], p, NOW)
    assert (result[-1].exclusion_reason is not None) == excluded
    if excluded:
        assert "tolerance=900s" in str(result[-1].exclusion_reason)
        assert b.window_end is not None and b.window_end.isoformat() in str(
            result[-1].exclusion_reason
        )
    old = NOW - timedelta(seconds=301)
    price = SpotPrice(
        3, "bybit.spot.EURUSDT", "bybit", "EUR", "USDT", 3, Decimal(1), old, old, old, True
    )
    with pytest.raises(ConversionUnavailable, match="stale conversion"):
        ConversionResolver(p, NOW, [price]).resolve("EUR", "bybit", NOW)
