"""Compatibility of retained turnover mappings, without rewriting observations."""

from typing import cast

from forge.markets.models import MarketDataError, number

ACTIVITY_MAPPING = "current-metrics-v3-activity-scope"


def object_or_empty(value: object) -> dict[str, object]:
    return cast(dict[str, object], value) if isinstance(value, dict) else {}


def turnover_issue(
    exchange: str, market: str, mapping: str, raw: object, metadata: dict[str, object]
) -> str | None:
    document = object_or_empty(raw)
    evidence = object_or_empty(document.get("interpretation"))
    reason = evidence.get("turnover_missing_reason")
    if isinstance(reason, str):
        return reason
    if exchange != "bitget" or market != "spot" or mapping == ACTIVITY_MAPPING:
        return None
    specification = object_or_empty(document.get("instrument_specification"))
    retained = object_or_empty(specification.get("raw"))
    ticker = object_or_empty(document.get("ticker"))
    known_reality = metadata.get("isReality") == "yes" or retained.get("isReality") == "yes"
    # A documented platform field differing from the old selected field is unsafe
    # even when old metadata predates isReality. Equal ordinary examples alone
    # do not invalidate unrelated observations.
    mismatch = False
    if "platformTurnover24h" in ticker:
        try:
            platform = number(ticker.get("platformTurnover24h"))
            broader = number(ticker.get("turnover24h"))
            mismatch = platform != broader
        except MarketDataError:
            mismatch = True
    if known_reality or mismatch:
        return "incompatible legacy Bitget platform-local turnover mapping; refresh Bitget metadata and statistics"
    return None
