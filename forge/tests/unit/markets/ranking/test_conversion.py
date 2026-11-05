"""Exact reference valuations, bounded paths, explicit precedence and quality rules."""

import tomllib
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal, localcontext
from fractions import Fraction

import pytest

from forge.markets.models import MarketDataError
from forge.markets.ranking.conversion import ConversionResolver, ConversionUnavailable, SpotPrice
from forge.markets.ranking.policy import TEMPLATE, RankingPolicy
from forge.markets.ranking.service import RankingInput, evaluate

NOW = datetime(2026, 11, 5, tzinfo=UTC)


def spot(
    index: int, base: str, quote: str, price: str | None, *, exchange: str = "bybit", age: int = 0
) -> SpotPrice:
    time = NOW - timedelta(seconds=age)
    return SpotPrice(
        index,
        f"{exchange}.spot.SOURCE{index}",
        exchange,
        base,
        quote,
        index,
        Decimal(price) if price is not None else None,
        time,
        time,
        time,
        True,
    )


def policy(**options: object) -> RankingPolicy:
    return RankingPolicy.model_validate({"version": 2, "conversion": options})


def test_identity_direct_inverted_and_zero_native_amount() -> None:
    p = policy()
    resolver = ConversionResolver(p, NOW, [spot(1, "EUR", "USDT", "1.08")])
    identity = resolver.resolve("USDT", "bybit", NOW)
    assert identity.method == "identity" and identity.rate == 1 and identity.score(Decimal(0)) == 0
    direct = resolver.resolve("EUR", "bybit", NOW)
    assert direct.method == "direct" and direct.rate == Decimal("1.08")
    assert direct.score(Decimal(100)) == 108
    inverted = ConversionResolver(p, NOW, [spot(2, "USDT", "EUR", "0.8")]).resolve(
        "EUR", "bybit", NOW
    )
    assert inverted.rate == Decimal("1.25") and inverted.legs[0].inverted
    item = RankingInput(5, "bybit.linear.ZERO", 5, Decimal(0), "EUR", False, NOW, NOW, True)
    assert evaluate([item], p, NOW, [spot(1, "EUR", "USDT", "1.08")])[0].score == 0


def test_eur_btc_usdt_exact_arithmetic_and_every_direction() -> None:
    a, b = spot(1, "BTC", "EUR", "92000"), spot(2, "BTC", "USDT", "100000")
    result = ConversionResolver(policy(bridge_assets=["BTC"]), NOW, [a, b]).resolve(
        "EUR", "bybit", NOW
    )
    with localcontext() as context:
        context.prec = 80
        expected = Decimal(100000) / Decimal(92000)
    assert result.rate == expected
    assert result.score(Decimal(92000)) == 100000
    assert result.method == "bridged" and result.path == ("EUR", "BTC", "USDT")
    assert [e.inverted for e in result.legs] == [True, False]
    assert Fraction(result.numerator, result.denominator) == Fraction(25, 23)
    reverse = ConversionResolver(
        RankingPolicy.model_validate(
            {"version": 2, "comparison_currency": "EUR", "conversion": {"bridge_assets": ["BTC"]}}
        ),
        NOW,
        [a, b],
    ).resolve("USDT", "bybit", NOW)
    assert reverse.score(Decimal(100000)) == 92000
    assert [e.inverted for e in reverse.legs] == [True, False]


def test_same_venue_bridge_beats_remote_direct_and_cross_permission_is_explicit() -> None:
    prices = [
        spot(1, "BTC", "EUR", "92000"),
        spot(2, "BTC", "USDT", "100000"),
        spot(3, "EUR", "USDT", "2", exchange="binance"),
    ]
    selected = ConversionResolver(policy(allow_cross_exchange=True), NOW, prices).resolve(
        "EUR", "bybit", NOW
    )
    assert selected.method == "bridged" and {e.source.exchange for e in selected.legs} == {"bybit"}
    with pytest.raises(ConversionUnavailable, match="cross-exchange conversion disallowed"):
        ConversionResolver(policy(), NOW, prices[2:]).resolve("EUR", "bybit", NOW)
    selected = ConversionResolver(policy(allow_cross_exchange=True), NOW, prices[2:]).resolve(
        "EUR", "bybit", NOW
    )
    assert selected.method == "direct" and selected.legs[0].source.exchange == "binance"
    mixed = [spot(1, "BTC", "EUR", "92000"), spot(2, "BTC", "USDT", "100000", exchange="binance")]
    with pytest.raises(ConversionUnavailable, match="no eligible"):
        ConversionResolver(policy(allow_cross_exchange=True), NOW, mixed).resolve(
            "EUR", "bybit", NOW
        )


def test_deterministic_preference_freshness_and_stable_identity() -> None:
    prices = [
        spot(2, "EUR", "USDT", "5", exchange="okx"),
        spot(1, "EUR", "USDT", "2", exchange="binance", age=5),
    ]
    p = policy(allow_cross_exchange=True, source_exchanges=["okx", "binance"])
    assert ConversionResolver(p, NOW, prices).resolve("EUR", "bybit", NOW).rate == 5
    assert ConversionResolver(p, NOW, list(reversed(prices))).resolve("EUR", "bybit", NOW).rate == 5
    prices = [spot(2, "EUR", "USDT", "5", age=5), spot(1, "EUR", "USDT", "2")]
    assert ConversionResolver(policy(), NOW, prices).resolve("EUR", "bybit", NOW).rate == 2
    prices = [spot(2, "EUR", "USDT", "5"), spot(1, "EUR", "USDT", "2")]
    assert (
        ConversionResolver(policy(), NOW, list(reversed(prices))).resolve("EUR", "bybit", NOW).rate
        == 2
    )


@pytest.mark.parametrize("price", [None, "0", "-1", "NaN", "Infinity"])
def test_invalid_prices_are_not_conversion_edges(price: str | None) -> None:
    with pytest.raises(ConversionUnavailable, match="missing or invalid conversion price"):
        ConversionResolver(policy(), NOW, [spot(1, "EUR", "USDT", price)]).resolve(
            "EUR", "bybit", NOW
        )


@pytest.mark.parametrize(
    ("change", "reason"),
    [
        ({"fetched_at": NOW - timedelta(hours=2)}, "stale conversion"),
        ({"source_observed_at": NOW + timedelta(hours=1)}, "future conversion"),
        (
            {"source_observed_at": NOW - timedelta(seconds=301)},
            "conversion/turnover timestamp skew",
        ),
        ({"complete": False}, "incomplete conversion"),
    ],
)
def test_conversion_timestamp_quality(change: dict[str, object], reason: str) -> None:
    with pytest.raises(ConversionUnavailable, match=reason):
        ConversionResolver(policy(), NOW, [replace(spot(1, "EUR", "USDT", "1"), **change)]).resolve(
            "EUR", "bybit", NOW
        )


def test_bridge_restrictions_leg_skew_and_cycle_avoidance() -> None:
    prices = [spot(1, "BTC", "EUR", "92000"), spot(2, "BTC", "USDT", "100000", age=20)]
    with pytest.raises(ConversionUnavailable, match="leg timestamp skew"):
        ConversionResolver(policy(max_leg_skew_seconds=10), NOW, prices).resolve(
            "EUR", "bybit", NOW
        )
    for options in (
        {"max_hops": 1},
        {"bridge_assets": ["ETH"]},
        {"bridge_assets": ["EUR", "USDT"]},
    ):
        with pytest.raises(ConversionUnavailable, match="no eligible"):
            ConversionResolver(policy(**options), NOW, prices).resolve("EUR", "bybit", NOW)
    assert (
        ConversionResolver(policy(), NOW, prices).resolve("USDT", "bybit", NOW).method == "identity"
    )


def test_manual_equivalence_automatic_precedence_and_no_implicit_pegs() -> None:
    prices = [spot(1, "USD", "USDT", "1.2")]
    p = policy(
        equivalent_currencies=["USD"],
        manual_rates={"USD": {"rate": "0.99", "as_of": NOW, "source": "explicit override"}},
    )
    result = ConversionResolver(p, NOW, prices).resolve("USD", "bybit", NOW)
    assert result.method == "manual" and result.rate == Decimal("0.99")
    assert (
        ConversionResolver(policy(equivalent_currencies=["USD"]), NOW, prices)
        .resolve("USD", "bybit", NOW)
        .method
        == "equivalence"
    )
    assert ConversionResolver(policy(), NOW, prices).resolve("USD", "bybit", NOW).method == "direct"
    for asset in ("USDC", "USDE", "USD1"):
        with pytest.raises(ConversionUnavailable):
            ConversionResolver(policy(equivalent_currencies=["USD"]), NOW, prices).resolve(
                asset, "bybit", NOW
            )
    stale = policy(
        manual_rates={
            "USD": {"rate": "0.99", "as_of": NOW - timedelta(hours=2), "source": "manual"}
        }
    )
    with pytest.raises(ConversionUnavailable, match="stale manual"):
        ConversionResolver(stale, NOW, prices).resolve("USD", "bybit", NOW)


def test_template_and_legacy_policy_compatibility() -> None:
    generated = RankingPolicy.model_validate(tomllib.loads(TEMPLATE))
    assert generated.version == 2 and generated.conversion.automatic
    legacy = RankingPolicy(
        conversion_rates={"EUR": Decimal("1.08")}, conversion_as_of=NOW, conversion_source="legacy"
    )
    assert not legacy.conversion.automatic
    assert ConversionResolver(legacy, NOW, []).resolve("EUR", "bybit", NOW).rate == Decimal("1.08")


@pytest.mark.parametrize(
    "options",
    [
        {"max_hops": 3},
        {"max_hops": True},
        {"bridge_assets": ["eur"]},
        {"bridge_assets": ["BTC", "BTC"]},
        {"allow_mixed_exchanges": True},
        {"prefer_same_exchange": False},
        {"unknown_field": True},
        {"source_exchanges": []},
        {"manual_rates": {"EUR": {"rate": "0", "as_of": NOW, "source": "bad"}}},
        {"manual_rates": {"EUR": {"rate": "NaN", "as_of": NOW, "source": "bad"}}},
        {"manual_rates": {"EUR": {"rate": "1", "as_of": datetime(2026, 11, 5), "source": "bad"}}},
        {"equivalent_currencies": ["USDT"]},
    ],
)
def test_invalid_conversion_policies_fail_with_diagnostics(options: dict[str, object]) -> None:
    with pytest.raises((ValueError, MarketDataError)):
        policy(**options)


def test_missing_invalid_turnover_and_fx_are_distinct_from_estimation() -> None:
    inputs = [
        RankingInput(1, "bybit.spot.VALID", 1, Decimal(100), "EUR", False, NOW, NOW, True),
        RankingInput(2, "bybit.linear.MISSING", 2, None, None, False, NOW, NOW, True),
    ]
    p = RankingPolicy(version=2, allow_partial_universe=True)
    rows = evaluate(inputs, p, NOW, [spot(1, "EUR", "USDT", "1.1")])
    assert rows[0].score == 110 and rows[0].exclusion_reason is None
    assert (
        rows[0].fallback_reason is not None and "estimated turnover" not in rows[0].fallback_reason
    )
    assert "missing native monetary" in str(rows[1].exclusion_reason)
    for invalid in (Decimal(-1), Decimal("NaN"), Decimal("Infinity")):
        rows = evaluate(
            [inputs[0], replace(inputs[1], turnover=invalid, unit="EUR")],
            p,
            NOW,
            [spot(1, "EUR", "USDT", "1.1")],
        )
        assert rows[1].exclusion_reason == "invalid native monetary turnover"


def test_full_precision_scores_sort_without_context_rounding() -> None:
    low = RankingInput(
        1,
        "bybit.spot.A",
        1,
        Decimal("1.000000000000000000000000000001"),
        "USDT",
        False,
        NOW,
        NOW,
        True,
    )
    high = replace(
        low,
        instrument_id=2,
        reference="bybit.spot.Z",
        observation_id=2,
        turnover=Decimal("1.000000000000000000000000000002"),
    )
    assert evaluate([low, high], policy(), NOW)[0].reference == high.reference


def test_invalid_future_native_window_does_not_poison_partial_universe_basis() -> None:
    valid = RankingInput(1, "bybit.spot.VALID", 1, Decimal(10), "USDT", False, NOW, NOW, True)
    future = replace(
        valid,
        instrument_id=2,
        reference="bybit.spot.FUTURE",
        observation_id=2,
        window_end=NOW + timedelta(hours=1),
    )
    rows = evaluate([valid, future], RankingPolicy(version=2, allow_partial_universe=True), NOW)
    assert rows[0].score == 10 and rows[1].exclusion_reason == "future source window"


def test_unrestricted_bridge_uses_connecting_sol_and_preserves_quality_limits() -> None:
    prices = [spot(1, "SOL", "EUR", "100"), spot(2, "SOL", "USDT", "110")]
    result = ConversionResolver(policy(bridge_assets=[]), NOW, prices).resolve("EUR", "bybit", NOW)
    assert result.path == ("EUR", "SOL", "USDT")
    assert result.score(Decimal(100)) == 110
    for options, changed, reason in (
        ({"bridge_assets": ["BTC", "ETH"]}, prices, "no eligible"),
        ({"max_hops": 1}, prices, "no eligible"),
        ({}, [prices[0], replace(prices[1], fetched_at=NOW - timedelta(hours=2))], "stale"),
        (
            {"max_leg_skew_seconds": 1},
            [prices[0], spot(2, "SOL", "USDT", "110", age=2)],
            "leg timestamp skew",
        ),
        ({}, [prices[0], replace(prices[1], exchange="binance")], "no eligible"),
    ):
        with pytest.raises(ConversionUnavailable, match=reason):
            ConversionResolver(policy(**{"bridge_assets": [], **options}), NOW, changed).resolve(
                "EUR", "bybit", NOW
            )


def test_unrestricted_bridges_are_deterministic_and_do_not_traverse_extra_legs() -> None:
    prices = [
        spot(1, "SOL", "EUR", "100"),
        spot(2, "SOL", "USDT", "110"),
        spot(3, "ADA", "EUR", "1"),
        spot(4, "ADA", "USDT", "2"),
        spot(5, "EUR", "X", "3"),
        spot(6, "X", "Y", "4"),
        spot(7, "Y", "USDT", "5"),
    ]
    p = policy(bridge_assets=[])
    assert ConversionResolver(p, NOW, prices).resolve("EUR", "bybit", NOW).path == (
        "EUR",
        "SOL",
        "USDT",
    )
    assert ConversionResolver(p, NOW, list(reversed(prices))).resolve("EUR", "bybit", NOW).path == (
        "EUR",
        "SOL",
        "USDT",
    )
    with pytest.raises(ConversionUnavailable, match="no eligible"):
        ConversionResolver(p, NOW, prices[4:]).resolve("EUR", "bybit", NOW)
