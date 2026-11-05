"""Reported and derived base quantities never cross base-asset comparison groups."""

from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest
from pydantic import ValidationError

from forge.markets.models import Instrument
from forge.markets.ranking.policy import RankingPolicy, load_policy, policy_document
from forge.markets.ranking.service import RankingInput, evaluate
from forge.markets.ranking.volume import measure

ROOT = Path(__file__).resolve().parents[5]
NOW = datetime(2026, 11, 5, tzinfo=UTC)


def test_explicit_volume_policy_and_turnover_regression() -> None:
    volume = load_policy(ROOT / "data/.config/markets/rankings/volume.toml")
    assert volume.metric == "volume" and not volume.conversion.automatic
    assert RankingPolicy.model_validate(policy_document(volume)) == volume
    assert load_policy(ROOT / "data/.config/markets/rankings/turnover.toml").conversion.automatic
    for payload in (
        {"metric": "volume"},
        {"metric": "volume", "volume_unit": "base_asset"},
        {
            "metric": "volume",
            "volume_unit": "base_asset",
            "group_by": "base_asset",
            "conversion": dict[str, object](),
        },
        {
            "metric": "volume",
            "volume_unit": "base_asset",
            "group_by": "base_asset",
            "comparison_currency": "USDT",
        },
    ):
        with pytest.raises(ValidationError):
            RankingPolicy.model_validate(payload)


def raw(*, quote_face: bool = False) -> dict[str, object]:
    i = Instrument(
        exchange="okx",
        market="inverse" if quote_face else "linear",
        symbol="ETH",
        base="ETH",
        quote="USDT",
        quantity_type="contracts",
        contract_value=Decimal("10"),
        contract_unit="USDT" if quote_face else "ETH",
    )
    return {
        "instrument_specification": i.model_dump(mode="json"),
        "interpretation": {
            "activity_scope": "exchange_local",
            "base_volume_scope": "exchange_local",
            "price_timestamp_basis": "source ticker time proxy",
        },
    }


@pytest.mark.parametrize("base", [Decimal(0), Decimal("12.34")])
def test_reported_volume_preferred_including_zero(base: Decimal) -> None:
    value = measure(
        base_volume=base,
        contract_volume=Decimal(9),
        base_unit="ETH",
        quote_unit="USDT",
        price=Decimal(100),
        raw=raw(),
        observation_id=1,
    )
    assert value.quantity == base and not value.estimated
    assert value.evidence["formula"] == "reported base_volume"


@pytest.mark.parametrize("quote_face", [False, True])
def test_verified_contract_derivation(quote_face: bool) -> None:
    value = measure(
        base_volume=None,
        contract_volume=Decimal(3),
        base_unit="ETH",
        quote_unit="USDT",
        price=Decimal(100),
        raw=raw(quote_face=quote_face),
        observation_id=2,
    )
    assert value.quantity == (Decimal("0.3") if quote_face else Decimal(30))
    assert value.estimated == quote_face
    assert value.evidence["contract_value"] == 10


@pytest.mark.parametrize("failure", ["unit", "scope", "spec", "price", "quote", "missing"])
def test_exclusion_reasons(failure: str) -> None:
    document = raw(quote_face=True)
    if failure == "scope":
        document["interpretation"] = {"activity_scope": "unknown"}
    if failure == "spec":
        document["instrument_specification"] = {
            "base": "ETH",
            "quote": "USDT",
            "instrument_type": "perpetual",
        }
    value = measure(
        base_volume=None,
        contract_volume=None if failure == "missing" else Decimal(3),
        base_unit="BTC" if failure == "unit" else "ETH",
        quote_unit="USD" if failure == "quote" else "USDT",
        price=Decimal(0) if failure == "price" else Decimal(100),
        raw=document,
        observation_id=1,
    )
    assert value.quantity is None and value.issue


def test_group_ranks_zero_estimates_freshness_and_failed_scopes() -> None:
    policy = RankingPolicy(
        metric="volume",
        volume_unit="base_asset",
        group_by="base_asset",
        allow_partial_universe=True,
    )

    def item(identity: int, asset: str, quantity: str) -> RankingInput:
        return RankingInput(
            identity,
            f"bybit.spot.{asset}{identity}",
            identity,
            Decimal(quantity),
            asset,
            False,
            NOW,
            NOW,
            True,
            group_asset=asset,
            volume_evidence={},
            base_volume=Decimal(quantity),
        )

    inputs = [
        item(1, "ETH", "4"),
        item(2, "ETH", "4"),
        item(3, "BTC", "0"),
        replace(item(4, "ETH", "99"), volume_estimated=True),
        replace(item(5, "ETH", "100"), refresh_complete=False),
    ]
    entries = evaluate(inputs, policy, NOW)
    assert [(e.base_asset, e.group_rank) for e in entries[:3]] == [
        ("BTC", 1),
        ("ETH", 1),
        ("ETH", 2),
    ]
    assert [e.position for e in entries[:3]] == [1, 2, 3]
    assert "estimated" in str(entries[3].exclusion_reason)
    assert "certified" in str(entries[4].exclusion_reason)
    allowed = evaluate(inputs, policy.model_copy(update={"estimates": "allow"}), NOW)
    assert allowed[1].instrument_id == 4 and allowed[1].group_rank == 1
