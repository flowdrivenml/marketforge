"""Pure inference rules. No network, filesystem, database writes or profile inference."""

from __future__ import annotations

import hashlib
import json
import re
from decimal import Decimal, InvalidOperation
from typing import Any

NAMESPACE = "marketforge:testing:historical:v1"
POLICY_VERSION = "catalogue-btc-reference-v1"
REFERENCE_POLICY_VERSION = "catalogue-btc-reference-v2"
ECONOMIC_FIELDS = (
    "base_asset",
    "quote_asset",
    "quantity_type",
    "tick_size",
    "qty_step",
    "min_qty",
    "max_qty",
    "min_notional",
    "min_notional_asset",
    "max_notional",
    "max_notional_asset",
    "contract_value",
    "contract_value_asset",
)
SPEC_FIELDS = (
    "quantity_type",
    "tick_size",
    "qty_step",
    "min_qty",
    "max_qty",
    "min_notional",
    "min_notional_asset",
    "max_notional",
    "max_notional_asset",
    "contract_value",
    "contract_value_asset",
)


def fingerprint(value: object) -> str:
    return hashlib.sha256(
        json.dumps(
            value, sort_keys=True, separators=(",", ":"), default=str, allow_nan=False
        ).encode()
    ).hexdigest()


def identity_key(row: dict[str, Any]) -> str:
    return "/".join(
        str(row[key]) for key in ("exchange", "instrument_type", "market_category", "symbol")
    )


def native_family(row: dict[str, Any]) -> str | None:
    symbol, kind, exchange = row["symbol"], row["instrument_type"], row["exchange"]
    raw: dict[str, Any] = row.get("raw") or {}
    native = raw.get("instFamily")
    if native and kind in {"perpetual", "future", "option"}:
        return str(native)
    if kind == "perpetual" and exchange == "okx" and symbol.endswith("-SWAP"):
        return symbol[:-5]
    if kind == "future":
        pattern = (
            r"(.+)_\d{6}"
            if exchange == "binance"
            else r"(.+)-\d{2}[A-Z]{3}\d{2}"
            if exchange == "bybit"
            else r"(.+)-\d{6}"
        )
        match = re.fullmatch(pattern, symbol)
        return match[1] if match else None
    if kind == "option":
        match = re.fullmatch(
            r"(.+?)[-_](?:\d{6}|\d{1,2}[A-Z]{3}\d{2})[-_]([0-9.]+)[-_]([CP])(?:[-_].+)?", symbol
        )
        return match[1] if match else None
    return None


def reference_family(row: dict[str, Any]) -> tuple[str, str] | None:
    family = native_family(row)
    if not family:
        return None
    if row["exchange"] != "okx" or row["instrument_type"] != "future":
        return family, row["instrument_type"]
    # OKX reports instFamily and ruleType on current products. Historical
    # archive identities can have only the native dated symbol.
    match = re.fullmatch(r"([A-Z0-9]+-[A-Z0-9]+(?:_[A-Z0-9]+)*)-\d{6}", row["symbol"])
    if not match or family != match[1]:
        return None
    rule = "xperp" if family.endswith("_XPERP") else "normal"
    raw: dict[str, Any] = row.get("raw") or {}
    if raw.get("ruleType") not in (None, "", rule):
        return None
    return family, rule


def decimal_value(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return result if result.is_finite() else None


def valid_spec(row: dict[str, Any]) -> list[str]:
    missing: list[str] = []
    for name in ("base_asset", "quote_asset", "quantity_type"):
        if not row.get(name):
            missing.append(name)
    tick = decimal_value(row.get("tick_size"))
    if tick is None or tick <= 0:
        missing.append("tick_size")
    if row.get("quantity_type") not in {"base", "quote", "contracts"}:
        missing.append("quantity_type")
    if row.get("quantity_type") == "contracts" or row.get("contract_value") is not None:
        contract = decimal_value(row.get("contract_value"))
        if contract is None or contract <= 0:
            missing.append("contract_value")
        if not row.get("contract_value_asset"):
            missing.append("contract_value_asset")
    for field in ("qty_step", "min_qty", "max_qty", "min_notional"):
        if row.get(field) is not None:
            value = decimal_value(row[field])
            if value is None or value < 0 or (field == "qty_step" and value == 0):
                missing.append(field)
    if row.get("min_qty") is not None and row.get("max_qty") is not None:
        if (
            decimal_value(row["min_qty"]) is not None
            and decimal_value(row["max_qty"]) is not None
            and Decimal(str(row["min_qty"])) > Decimal(str(row["max_qty"]))
        ):
            missing.append("quantity_bounds")
    return sorted(set(missing))


def compatible_reference(target: dict[str, Any], source: dict[str, Any]) -> bool:
    if (
        target.get("base_asset") != "BTC"
        or source.get("base_asset") != "BTC"
        or target.get("instrument_type") not in {"future", "option"}
    ):
        return False
    if any(
        target.get(key) != source.get(key)
        for key in (
            "exchange",
            "instrument_type",
            "market_category",
            "base_asset",
            "quote_asset",
            "settlement_asset",
        )
    ):
        return False
    family = reference_family(target)
    if family is None or family != reference_family(source) or not target.get("settlement_asset"):
        return False
    for key in ("quantity_type", "contract_value_asset"):
        if target.get(key) is not None and target[key] != source.get(key):
            return False
    return not valid_spec(source)


def option_identity(row: dict[str, Any]) -> dict[str, Any]:
    """Keep instrument properties on the target; never inherit them from a reference."""
    raw: dict[str, Any] = row.get("raw") or {}
    match = re.search(r"[-_](\d{6}|\d{1,2}[A-Z]{3}\d{2})(?:[-_]|$)", str(row.get("symbol", "")))
    return {
        "native_expiration_token": match[1]
        if match and row.get("instrument_type") in {"future", "option"}
        else None,
        "strike": row.get("strike"),
        "expiry_ns": row.get("expiry_ns"),
        "option_type": row.get("option_type") or raw.get("optionType"),
        "underlying": raw.get("uly") or raw.get("underlying") or row.get("base_asset"),
        "settlement_asset": row.get("settlement_asset"),
        "launch_time_ns": row.get("launch_time_ns"),
    }


def infer(
    target: dict[str, Any], candidates: list[dict[str, Any]], start: str | None, end: str | None
) -> dict[str, Any]:
    source = target
    method = "catalogue_inferred"
    inherited: list[str] = []
    if valid_spec(target):
        refs = [row for row in candidates if compatible_reference(target, row)]
        # Active/nonexpired source first, then newest specification observation and native identity.
        now_ns = max(
            (
                int(row["observed_at"].timestamp() * 1_000_000_000)
                for row in refs
                if hasattr(row.get("observed_at"), "timestamp")
            ),
            default=0,
        )
        refs.sort(
            key=lambda row: (
                row.get("status")
                not in {"active", "TRADING", "Trading", "live", "online", "tradable"}
                or (row.get("expiry_ns") is not None and int(row["expiry_ns"]) <= now_ns),
                -row["observed_at"].timestamp()
                if hasattr(row.get("observed_at"), "timestamp")
                else 0,
                identity_key(row),
                -int(row.get("observation_order") or 0),
            )
        )
        if not refs:
            return {
                "inference_method": "unresolved",
                "missing_fields": valid_spec(target),
                "reason": "No valid exact specification or compatible same-exchange BTC reference contract.",
            }
        source = refs[0]
        method = "reference_contract_inferred"
        inherited = [
            name for name in SPEC_FIELDS if target.get(name) is None or name in valid_spec(target)
        ]
    values = {key: target.get(key) for key in ECONOMIC_FIELDS}
    if method == "reference_contract_inferred":
        for key in inherited:
            values[key] = source.get(key)
    missing = valid_spec(values)
    if missing:
        return {
            "inference_method": "unresolved",
            "missing_fields": missing,
            "reason": "Required normalization values remain unresolved.",
        }
    if target.get("instrument_type") == "option":
        facts = option_identity(target)
        missing_option = [
            key
            for key in ("strike", "expiry_ns", "option_type", "underlying", "settlement_asset")
            if facts.get(key) is None
        ]
        if missing_option:
            return {
                "inference_method": "unresolved",
                "missing_fields": missing_option,
                "reason": "Option-specific identity cannot be inherited.",
            }
    facts = option_identity(target)
    result = {
        **values,
        "identity": {
            key: target.get(key)
            for key in ("exchange", "instrument_type", "market_category", "symbol")
        },
        "instrument_facts": facts,
        "inference_method": method,
        "historically_verified": False,
        "policy_version": REFERENCE_POLICY_VERSION
        if method == "reference_contract_inferred"
        else POLICY_VERSION,
        "assumed_applicable_from": start,
        "assumed_applicable_to": end,
        "valid_from": start,
        "valid_to": end,
        "source_catalogue_instrument_id": source.get("id"),
        "source_api_observation_id": source.get("api_id"),
        "source_observed_at": source.get("observed_at"),
        "source_snapshot_sha256": fingerprint(
            {key: source.get(key) for key in (*ECONOMIC_FIELDS, "raw")}
        ),
        "inherited_fields": inherited,
        "reference_instrument": identity_key(source)
        if method == "reference_contract_inferred"
        else None,
        "assumption": "Project policy permits projecting the selected stored specification over the operational applicability interval. This is not an observation of historical validity.",
        "provenance": "PostgreSQL retained catalogue/specification observation; no network inference.",
    }
    for key in ECONOMIC_FIELDS:
        if isinstance(result.get(key), Decimal):
            result[key] = str(result[key])
    return result


def evidence_fingerprint(definition: dict[str, Any]) -> str:
    # Identical recollections corroborate the existing economic revision. Keep its
    # original observation ID/time rather than rewriting immutable provenance.
    return fingerprint(
        {
            key: value
            for key, value in definition.items()
            if key not in {"source_api_observation_id", "source_observed_at"}
        }
    )


def select_evidence(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    rows = [
        row
        for row in rows
        if row.get("evidence_kind") != "reference_contract_inferred"
        or row.get("policy_version") == REFERENCE_POLICY_VERSION
    ]
    if not rows:
        return None
    priorities = {
        "historically_verified": 0,
        "historical_declaration": 1,
        "catalogue_inferred": 2,
        "reference_contract_inferred": 3,
    }
    best = min(
        priorities.get(row.get("evidence_kind", "historical_declaration"), 1) for row in rows
    )
    preferred = [
        row
        for row in rows
        if priorities.get(row.get("evidence_kind", "historical_declaration"), 1) == best
    ]
    if best < 2 and len({economic_signature(row["definition"]) for row in preferred}) > 1:
        raise ValueError("Conflicting explicit historical specifications cover this interval.")
    return max(preferred, key=lambda row: int(row.get("id") or 0))


def economic_signature(definition: dict[str, Any]) -> str:
    values: dict[str, Any] = {}
    for key in ECONOMIC_FIELDS:
        value = definition.get(key)
        decimal = decimal_value(value)
        if decimal is None:
            values[key] = value
        elif decimal == 0:
            values[key] = "0"
        else:
            # Decimal.normalize() uses the active context and can round long values.
            sign, digits, exponent = decimal.as_tuple()
            stripped = list(digits)
            scale = int(exponent)
            while stripped and stripped[-1] == 0:
                stripped.pop()
                scale += 1
            values[key] = (sign, stripped, scale)
    return fingerprint(values)
