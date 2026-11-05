"""Base-asset quantities from retained observation evidence; no monetary FX."""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, localcontext
from typing import TYPE_CHECKING

from forge.markets.interpretation import object_or_empty
from forge.markets.models import MarketDataError, number

if TYPE_CHECKING:
    from forge.markets.ranking.policy import RankingPolicy
    from forge.markets.ranking.service import RankedEntry, RankingInput

ALGORITHM = "volume24h-base-asset-v1"


@dataclass(frozen=True)
class Volume:
    asset: str | None
    quantity: Decimal | None
    estimated: bool
    issue: str | None
    evidence: dict[str, object]


def measure(
    *,
    base_volume: Decimal | None,
    contract_volume: Decimal | None,
    base_unit: str | None,
    quote_unit: str | None,
    price: Decimal | None,
    raw: object,
    observation_id: int | None,
) -> Volume:
    document = object_or_empty(raw)
    specs = object_or_empty(document.get("instrument_specification"))
    interpretation = object_or_empty(document.get("interpretation"))
    base, quote = specs.get("base"), specs.get("quote")
    asset = base if isinstance(base, str) and base else None
    evidence: dict[str, object] = {
        "observation_id": observation_id,
        "base_volume": base_volume,
        "contract_volume": contract_volume,
        "base_unit": base_unit,
        "quote_unit": quote_unit,
        "reference_price": price,
        "instrument_specification": specs,
        "activity_scope": interpretation.get("base_volume_scope"),
        "price_timestamp_basis": interpretation.get("price_timestamp_basis"),
        "formula": None,
        "estimated": False,
    }

    def reject(issue: str) -> Volume:
        return Volume(asset, None, False, issue, evidence)

    if asset is None:
        return reject("missing retained base-asset specification")
    if (
        interpretation.get("activity_scope") != "exchange_local"
        or interpretation.get("base_volume_scope") != "exchange_local"
    ):
        return reject("missing or incompatible exchange-local base-volume activity scope")
    if base_unit != asset:
        return reject("reported base-volume unit differs from retained instrument base asset")
    if base_volume is not None:
        if not base_volume.is_finite() or base_volume < 0:
            return reject("invalid reported base volume")
        evidence["formula"] = "reported base_volume"
        return Volume(asset, base_volume, False, None, evidence)
    if contract_volume is None:
        return reject("missing reported base volume and contract count")
    if not contract_volume.is_finite() or contract_volume < 0:
        return reject("invalid contract count")
    if (
        specs.get("instrument_type") not in {"perpetual", "future"}
        or specs.get("quantity_type") != "contracts"
    ):
        return reject("contract count lacks retained derivative contract specifications")
    try:
        face = number(specs.get("contract_value"))
    except MarketDataError:
        face = None
    denomination = specs.get("contract_unit")
    if face is None or face <= 0 or (denomination != asset and denomination != quote):
        return reject("missing or incompatible retained contract face value/unit")
    evidence.update(contract_value=face, contract_unit=denomination)
    with localcontext() as context:
        context.prec = 80
        quantity = contract_volume * face
        if denomination == asset:
            evidence["formula"] = "contract_count * base_units_per_contract"
        else:
            if quote_unit != quote or not isinstance(quote, str) or not quote:
                return reject("reference-price quote unit differs from retained contract quote")
            if price is None or not price.is_finite() or price <= 0:
                return reject("quote-denominated contract requires a positive reference price")
            if not interpretation.get("price_timestamp_basis"):
                return reject("reference-price timestamp evidence is missing")
            quantity /= price
            evidence["formula"] = "contract_count * quote_face_value / reference_price"
            evidence["estimated"] = True
    return Volume(asset, quantity, bool(evidence["estimated"]), None, evidence)


def evaluate_volume(
    inputs: list["RankingInput"], policy: "RankingPolicy", now: "datetime"
) -> list["RankedEntry"]:
    from dataclasses import replace
    from datetime import timedelta

    from forge.markets.ranking.service import RankedEntry

    if not inputs:
        raise MarketDataError("Ranking universe is empty.")
    if now.tzinfo is None:
        raise MarketDataError("Ranking build time must include a timezone.")
    latest: dict[str | None, datetime] = {}
    for item in inputs:
        end = item.window_end
        if (
            end is not None
            and end.tzinfo is not None
            and end <= now + timedelta(seconds=policy.max_window_skew_seconds)
        ):
            latest[item.group_asset] = max(latest.get(item.group_asset, end), end)
    included: list[RankedEntry] = []
    excluded: list[RankedEntry] = []
    for item in inputs:
        issue = item.volume_issue
        if issue is None:
            if not item.refresh_complete:
                issue = "input not certified by its statistics refresh"
            elif item.observation_id is None or item.base_volume is None:
                issue = "missing base-volume observation"
            elif not item.base_volume.is_finite() or item.base_volume < 0:
                issue = "invalid base volume"
            elif not item.group_asset:
                issue = "missing or incompatible base-asset group/unit"
            elif (
                item.fetched_at is None
                or item.window_end is None
                or item.fetched_at.tzinfo is None
                or item.window_end.tzinfo is None
            ):
                issue = "invalid volume observation timestamps"
            elif min(item.fetched_at, item.window_end) < now - timedelta(
                seconds=policy.max_age_seconds
            ):
                issue = "stale observation"
            elif item.window_end > now + timedelta(seconds=policy.max_window_skew_seconds):
                issue = "future source window"
            elif latest.get(item.group_asset, now) - item.window_end > timedelta(
                seconds=policy.max_window_skew_seconds
            ):
                issue = "incompatible source windows within base-asset group"
            elif item.volume_estimated and policy.estimates == "exclude":
                issue = "estimated base volume excluded"
        entry = RankedEntry(
            item.instrument_id,
            item.reference,
            item.observation_id,
            None,
            item.base_volume if issue is None else None,
            issue,
            "reference-price base-volume estimate" if item.volume_estimated else None,
            base_asset=item.group_asset,
            volume_unit=item.group_asset,
            volume_estimated=item.volume_estimated,
            volume_evidence=item.volume_evidence,
        )
        (included if issue is None else excluded).append(entry)
    if excluded and not policy.allow_partial_universe:
        from collections import Counter

        summary = "; ".join(
            f"{count}: {reason}"
            for reason, count in sorted(Counter(str(e.exclusion_reason) for e in excluded).items())
        )
        raise MarketDataError("Incomplete/comparability-filtered volume universe: " + summary)
    if not included:
        raise MarketDataError("No comparable fresh base-volume observations.")
    included.sort(
        key=lambda e: (
            e.base_asset or "",
            (e.score.copy_negate() if e.score is not None else Decimal(0)),
            e.reference,
        )
    )
    ranks: dict[str | None, int] = {}
    result: list[RankedEntry] = []
    for position, entry in enumerate(included, 1):
        rank = ranks.get(entry.base_asset, 0) + 1
        ranks[entry.base_asset] = rank
        result.append(replace(entry, position=position, group_rank=rank))
    return result + sorted(excluded, key=lambda e: (e.base_asset or "", e.reference))
