"""Historical projection is explicit, deterministic and denomination constrained."""

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest

from forge.historical.metadata.policy import (
    evidence_fingerprint,
    infer,
    native_family,
    select_evidence,
)
from forge.historical.metadata.service import covers


def instrument(**changes: Any) -> dict[str, Any]:
    return {
        "id": 17,
        "exchange": "okx",
        "instrument_type": "future",
        "market_category": "linear",
        "symbol": "BTC-USDT-261225",
        "base_asset": "BTC",
        "quote_asset": "USDT",
        "settlement_asset": "USDT",
        "quantity_type": "contracts",
        "tick_size": Decimal("0.100000000000000001"),
        "contract_value": Decimal("0.01"),
        "contract_value_asset": "BTC",
        "qty_step": Decimal("1"),
        "expiry_ns": 1798156800000000000,
        "observed_at": datetime(2026, 11, 5, tzinfo=UTC),
        "api_id": 20,
        "raw": {},
        **changes,
    }


def test_exact_projection_precision_and_provenance() -> None:
    source = instrument()
    result = infer(source, [], "2026-10-01T00:00:00Z", "2026-11-01T00:00:00Z")
    assert result["tick_size"] == "0.100000000000000001"
    assert result["inference_method"] == "catalogue_inferred"
    assert result["historically_verified"] is False
    assert result["source_api_observation_id"] == 20
    assert result["source_observed_at"] == source["observed_at"]
    assert result["instrument_facts"]["expiry_ns"] == source["expiry_ns"]
    recollected = {
        **result,
        "source_api_observation_id": 21,
        "source_observed_at": datetime(2026, 11, 6, tzinfo=UTC),
    }
    assert evidence_fingerprint(result) == evidence_fingerprint(recollected)


@pytest.mark.parametrize(
    "changes",
    [
        {"exchange": "bybit"},
        {"market_category": "inverse"},
        {"settlement_asset": "BTC"},
        {"contract_value_asset": "USD"},
        {"instrument_type": "perpetual"},
        {"base_asset": "ETH"},
    ],
)
def test_reference_isolation(changes: dict[str, str]) -> None:
    target = instrument(tick_size=None)
    assert infer(target, [instrument(**changes)], None, None)["inference_method"] == "unresolved"


def test_reference_only_missing_fields_and_expiration_preserved() -> None:
    target = instrument(
        id=18,
        symbol="BTC-USDT-260925",
        tick_size=None,
        contract_value=Decimal("0.02"),
        expiry_ns=1790323200000000000,
    )
    result = infer(target, [instrument()], None, None)
    assert result["inference_method"] == "reference_contract_inferred"
    assert result["contract_value"] == "0.02"
    assert result["instrument_facts"]["expiry_ns"] == target["expiry_ns"]
    assert result["identity"]["symbol"] == target["symbol"]
    assert "tick_size" in result["inherited_fields"]
    assert "contract_value" not in result["inherited_fields"]


def test_option_identity_never_inherited() -> None:
    source = instrument(
        instrument_type="option",
        symbol="BTC-USD-261225-100000-C",
        market_category="inverse",
        quote_asset="USD",
        settlement_asset="BTC",
        strike=Decimal("100000"),
        option_type="call",
        raw={"uly": "BTC-USD"},
    )
    target = {
        **source,
        "id": 19,
        "symbol": "BTC-USD-261127-80000-P",
        "tick_size": None,
        "strike": Decimal("80000"),
        "option_type": "put",
        "expiry_ns": 1795766400000000000,
    }
    result = infer(target, [source], None, None)
    assert result["instrument_facts"]["strike"] == Decimal("80000")
    assert result["instrument_facts"]["option_type"] == "put"
    assert result["instrument_facts"]["expiry_ns"] == target["expiry_ns"]
    assert infer({**target, "strike": None}, [source], None, None)["missing_fields"] == ["strike"]


def test_explicit_precedence_conflict_and_applicability() -> None:
    definition = {
        "tick_size": "0.1",
        "valid_from": "2026-10-01T00:00:00Z",
        "valid_to": "2026-11-01T00:00:00Z",
    }
    explicit = {"id": 1, "evidence_kind": "historically_verified", "definition": definition}
    inferred = {
        "id": 10,
        "evidence_kind": "catalogue_inferred",
        "definition": {**definition, "tick_size": "0.2"},
    }
    assert select_evidence([inferred, explicit]) == explicit
    assert covers(explicit, datetime(2026, 10, 2, tzinfo=UTC), datetime(2026, 10, 3, tzinfo=UTC))
    assert not covers(explicit, datetime(2026, 9, 2, tzinfo=UTC), datetime(2026, 10, 3, tzinfo=UTC))
    with pytest.raises(ValueError, match="Conflicting"):
        select_evidence(
            [explicit, {**explicit, "id": 2, "definition": {**definition, "tick_size": "0.2"}}]
        )


@pytest.mark.parametrize(
    "exchange,kind,symbol,expected",
    [
        ("okx", "perpetual", "BTC-USD-SWAP", "BTC-USD"),
        ("okx", "future", "BTC-USD_UM-261127", "BTC-USD_UM"),
        ("binance", "future", "BTCUSD_261225", "BTCUSD"),
        ("bybit", "future", "BTCUSD-25DEC26", "BTCUSD"),
        ("okx", "option", "BTC-USD-261225-100000-C", "BTC-USD"),
    ],
)
def test_native_family(exchange: str, kind: str, symbol: str, expected: str) -> None:
    assert (
        native_family(instrument(exchange=exchange, instrument_type=kind, symbol=symbol))
        == expected
    )


def test_missing_reference_and_non_btc_remain_unresolved() -> None:
    missing = instrument(tick_size=None)
    assert infer(missing, [], None, None)["missing_fields"] == ["tick_size"]
    target = {**missing, "base_asset": "ETH"}
    assert infer(target, [instrument()], None, None)["inference_method"] == "unresolved"


def test_exact_values_precede_reference_and_numerical_declarations_are_equivalent() -> None:
    target = instrument(tick_size=Decimal("0.5"))
    assert infer(target, [instrument()], None, None)["inference_method"] == "catalogue_inferred"
    one = {"id": 1, "evidence_kind": "historical_declaration", "definition": {"tick_size": "0.10"}}
    two = {**one, "id": 2, "definition": {"tick_size": "0.1"}}
    assert select_evidence([one, two]) == two


def test_unbounded_inference_cannot_satisfy_registered_interval() -> None:
    evidence = {
        "evidence_kind": "catalogue_inferred",
        "definition": {"assumed_applicable_from": None, "assumed_applicable_to": None},
    }
    assert not covers(
        evidence, datetime(2026, 10, 1, tzinfo=UTC), datetime(2026, 11, 1, tzinfo=UTC)
    )


def test_native_expiration_token_does_not_fabricate_time() -> None:
    target = instrument(symbol="BTC-USD_UM-261030", expiry_ns=None)
    result = infer(target, [], None, None)
    assert result["instrument_facts"]["native_expiration_token"] == "261030"
    assert result["instrument_facts"]["expiry_ns"] is None


def test_explicit_decimal_conflicts_never_round_to_context_precision() -> None:
    one = {
        "id": 1,
        "evidence_kind": "historically_verified",
        "definition": {"tick_size": "0.100000000000000000000000000001"},
    }
    two = {**one, "id": 2, "definition": {"tick_size": "0.100000000000000000000000000002"}}
    with pytest.raises(ValueError, match="Conflicting"):
        select_evidence([one, two])


def test_dated_future_rejects_xperp_and_conflicting_native_metadata() -> None:
    target = instrument(
        symbol="BTC-USD_UM-261030",
        tick_size=None,
        quantity_type=None,
        contract_value=None,
        contract_value_asset=None,
        expiry_ns=None,
        quote_asset="USD",
        settlement_asset="USD",
    )
    dated = instrument(
        symbol="BTC-USD_UM-261127",
        quote_asset="USD",
        settlement_asset="USD",
        raw={"instFamily": "BTC-USD_UM", "ruleType": "normal"},
        status="live",
    )
    xperp = {
        **dated,
        "symbol": "BTC-USD_UM_XPERP-310404",
        "raw": {"instFamily": "BTC-USD_UM_XPERP", "ruleType": "xperp"},
        "contract_value": Decimal("0.0001"),
        "observation_order": 200,
    }
    conflicting = {**dated, "raw": {"instFamily": "BTC-USD_UM", "ruleType": "xperp"}}
    assert infer(target, [xperp, conflicting], None, None)["inference_method"] == "unresolved"
    result = infer(target, [xperp, dated], None, None)
    assert result == infer(target, [dated, xperp], None, None)
    assert result["reference_instrument"].endswith(dated["symbol"])
    assert result["contract_value"] == "0.01"
    assert result["contract_value_asset"] == "BTC"
    assert result["quantity_type"] == "contracts"
    assert result["tick_size"] == str(dated["tick_size"])
    assert result["instrument_facts"]["settlement_asset"] == "USD"
    assert result["historically_verified"] is False
    old: dict[str, Any] = {
        "id": 1,
        "evidence_kind": "reference_contract_inferred",
        "policy_version": "catalogue-btc-reference-v1",
        "definition": {},
    }
    new = {**old, "id": 2, "policy_version": result["policy_version"], "definition": result}
    assert select_evidence([old]) is None
    assert select_evidence([new, old]) == new
