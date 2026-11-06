"""Immutable deterministic ranking publication and independent stream resolution."""

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Literal
from uuid import UUID, uuid4

from psycopg import AsyncConnection
from psycopg.pq import TransactionStatus
from psycopg.rows import TupleRow, dict_row
from pydantic import BaseModel, ConfigDict, model_validator

from forge.markets.capabilities import COLLECTION_VERSION, EXCHANGES, resolve_scopes
from forge.markets.filters import symbol_pattern
from forge.markets.interpretation import turnover_issue
from forge.markets.models import MarketDataError
from forge.markets.ranking.conversion import (
    ALGORITHM,
    Conversion,
    ConversionResolver,
    ConversionUnavailable,
    SpotPrice,
)
from forge.markets.ranking.inputs import spot_prices, turnover_rows
from forge.markets.ranking.policy import RankingPolicy, currency, policy_document
from forge.markets.ranking.volume import ALGORITHM as VOLUME_ALGORITHM
from forge.markets.ranking.volume import evaluate_volume, measure
from forge.markets.repository import iter_instruments, json_value


@dataclass(frozen=True)
class RankingInput:
    instrument_id: int
    reference: str
    observation_id: int | None
    turnover: Decimal | None
    unit: str | None
    estimated: bool
    fetched_at: datetime | None
    window_end: datetime | None
    refresh_complete: bool
    exchange: str = ""
    turnover_issue: str | None = None
    group_asset: str | None = None
    volume_evidence: dict[str, object] | None = None
    base_volume: Decimal | None = None
    volume_estimated: bool = False
    volume_issue: str | None = None


@dataclass(frozen=True)
class RankedEntry:
    instrument_id: int
    reference: str
    observation_id: int | None
    position: int | None
    score: Decimal | None
    exclusion_reason: str | None
    fallback_reason: str | None
    conversion: Conversion | None = None
    native_turnover: Decimal | None = None
    native_unit: str | None = None
    base_asset: str | None = None
    group_rank: int | None = None
    volume_unit: str | None = None
    volume_estimated: bool | None = None
    volume_evidence: dict[str, object] | None = None


def evaluate(
    inputs: list[RankingInput],
    policy: RankingPolicy,
    now: datetime,
    prices: list[SpotPrice] | None = None,
) -> list[RankedEntry]:
    if policy.metric == "volume":
        return evaluate_volume(inputs, policy, now)
    if not inputs:
        raise MarketDataError("Ranking universe is empty.")
    if now.tzinfo is None:
        raise MarketDataError("Ranking build time must include a timezone.")
    resolver = ConversionResolver(policy, now, prices or [])
    latest = max(
        (
            i.window_end
            for i in inputs
            if i.window_end is not None
            and i.window_end.tzinfo is not None
            and i.window_end <= now + timedelta(seconds=policy.max_window_skew_seconds)
        ),
        default=now,
    )
    included: list[RankedEntry] = []
    excluded: list[RankedEntry] = []
    for i in inputs:
        reason = None
        fallback = None
        score = None
        conversion = None
        if not i.refresh_complete and not policy.allow_partial_universe:
            reason = "incomplete source refresh"
        if i.observation_id is None or i.turnover is None or i.unit is None:
            reason = "missing native monetary turnover observation"
        elif not i.turnover.is_finite() or i.turnover < 0:
            reason = "invalid native monetary turnover"
        elif (i.fetched_at is not None and i.fetched_at.tzinfo is None) or (
            i.window_end is not None and i.window_end.tzinfo is None
        ):
            reason = "invalid turnover observation timestamps"
        elif (
            i.fetched_at is None
            or i.window_end is None
            or min(i.fetched_at, i.window_end) < now - timedelta(seconds=policy.max_age_seconds)
        ):
            reason = "stale observation"
        elif i.window_end > now + timedelta(seconds=policy.max_window_skew_seconds):
            reason = "future source window"
        elif latest - i.window_end > timedelta(seconds=policy.max_window_skew_seconds):
            reason = (
                f"incompatible source windows: end={i.window_end.isoformat()}, "
                f"latest={latest.isoformat()}, tolerance={policy.max_window_skew_seconds}s"
            )
        elif i.estimated and policy.estimates == "exclude":
            reason = "estimated turnover excluded"
        if i.turnover_issue is not None:
            reason = i.turnover_issue
        if reason is None:
            try:
                assert i.unit is not None and i.turnover is not None
                currency(i.unit)
            except ValueError:
                reason = "invalid native monetary turnover denomination"
            else:
                try:
                    conversion = resolver.resolve(
                        i.unit, i.exchange or i.reference.split(".", 1)[0], i.window_end
                    )
                except ConversionUnavailable as error:
                    reason = str(error)
                else:
                    score = conversion.score(i.turnover)
                    notes: list[str] = []
                    if i.estimated:
                        notes.append("estimated turnover")
                    if not i.refresh_complete:
                        notes.append("partial refresh input")
                    if conversion.method == "manual":
                        notes.append(
                            f"explicit conversion {i.unit} -> {policy.comparison_currency}"
                        )
                    elif conversion.method == "equivalence":
                        notes.append("explicit 1:1 equivalence assumption")
                    elif conversion.method in {"direct", "bridged"}:
                        notes.append("spot reference-price valuation (" + conversion.method + ")")
                    fallback = "; ".join(notes) or None
        entry = RankedEntry(
            i.instrument_id,
            i.reference,
            i.observation_id,
            None,
            score,
            reason,
            fallback,
            conversion,
            i.turnover
            if i.turnover is not None and i.turnover.is_finite() and i.turnover >= 0
            else None,
            i.unit,
        )
        (included if reason is None else excluded).append(entry)
    if excluded and not policy.allow_partial_universe:
        from collections import Counter

        reasons = Counter(e.exclusion_reason for e in excluded)
        summary = "; ".join(
            f"{count}: {reason}"
            for reason, count in sorted(reasons.items(), key=lambda value: str(value[0]))
        )
        raise MarketDataError(
            "Incomplete/comparability-filtered universe; explicitly allow_partial_universe to publish exclusions. "
            + summary
        )
    if not included:
        raise MarketDataError("No comparable fresh turnover observations.")
    unknown = set(policy.manual_preferences) - {i.reference for i in inputs}
    if unknown:
        raise MarketDataError("Manual preferences reference instruments outside the universe.")
    preference = {key: position for position, key in enumerate(policy.manual_preferences)}
    included.sort(
        key=lambda e: (
            preference.get(e.reference, len(preference)),
            e.score.copy_negate() if e.score is not None else Decimal(0),
            e.reference,
        )
    )
    from dataclasses import replace

    return [replace(e, position=index + 1) for index, e in enumerate(included)] + sorted(
        excluded, key=lambda e: e.reference
    )


async def build(
    c: AsyncConnection[TupleRow],
    name: str,
    policy: RankingPolicy,
    *,
    now: datetime | None = None,
    refresh_id: int | None = None,
) -> UUID:
    observed = now or datetime.now(UTC)
    owned = c.info.transaction_status == TransactionStatus.IDLE
    async with c.transaction():
        # Freeze metadata and retained observations in one read snapshot. No network.
        if owned:
            await c.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        else:
            current = await c.execute("SHOW transaction_isolation")
            if await current.fetchone() != ("repeatable read",):
                raise MarketDataError("Caller-owned ranking transactions require REPEATABLE READ.")
        # Filter in SQL before keyset pagination, never truncate a global catalog first.
        universe = [
            i
            async for i in iter_instruments(
                c,
                exchanges=list(policy.exchanges or EXCHANGES),
                markets=list(policy.markets),
                references=policy.instruments or None,
            )
        ]
        if policy.instruments and set(policy.instruments) != {i.reference for i in universe}:
            raise MarketDataError(
                "Some explicitly selected instruments are missing from the catalogue."
            )
        retained = await turnover_rows(c, [i.id for i in universe], refresh_id=refresh_id)
        inputs: list[RankingInput] = []
        for i in universe:
            row = retained.get(i.id)
            if policy.metric == "volume":
                volume = measure(
                    base_volume=row[9] if row else None,
                    contract_volume=row[10] if row else None,
                    base_unit=row[11] if row else None,
                    quote_unit=row[12] if row else None,
                    price=row[13] if row else None,
                    raw=row[8] if row else None,
                    observation_id=row[0] if row else None,
                )
                interval_issue = None
                if row and row[14] is not None and row[5] is not None:
                    duration = (row[5] - row[14]).total_seconds()
                    if abs(duration - 86400) > policy.max_window_skew_seconds:
                        interval_issue = "volume observation is not a 24-hour window"
                else:
                    interval_issue = "missing 24-hour volume window evidence"
                inputs.append(
                    RankingInput(
                        i.id,
                        i.reference,
                        row[0] if row else None,
                        row[1] if row else None,
                        row[2] if row else None,
                        bool(row[3]) if row else False,
                        row[4] if row else None,
                        row[5] if row else None,
                        row is not None and bool(row[6]),
                        i.exchange,
                        group_asset=volume.asset,
                        volume_evidence=volume.evidence,
                        base_volume=volume.quantity,
                        volume_estimated=volume.estimated,
                        volume_issue=volume.issue or interval_issue,
                    )
                )
                continue
            inputs.append(
                RankingInput(
                    i.id,
                    i.reference,
                    row[0] if row else None,
                    row[1] if row else None,
                    row[2] if row else None,
                    bool(row[3]) if row else False,
                    row[4] if row else None,
                    row[5] if row else None,
                    row is not None and bool(row[6]),
                    i.exchange,
                    turnover_issue(i.exchange, i.market, row[7], row[8], i.raw) if row else None,
                )
            )
        if refresh_id is not None:
            from dataclasses import replace

            inputs = [
                (
                    replace(i, volume_issue="scope failed in the requested statistics refresh")
                    if policy.metric == "volume"
                    else replace(
                        i, turnover_issue="scope failed in the requested statistics refresh"
                    )
                )
                if not i.refresh_complete
                else i
                for i in inputs
            ]
        missing_sources: list[str] = []
        if not policy.instruments:
            for exchange, market in resolve_scopes(policy.exchanges or None, policy.markets):
                cur = await c.execute(
                    """SELECT completed_at FROM catalog.metric_refreshes
                    WHERE scope_json->>'collection_version'=%s AND scope_json->>'symbol' IS NULL
                      AND (%s::bigint IS NULL OR id=%s)
                      AND status IN ('complete','partial','cancelled')
                      AND EXISTS(SELECT 1 FROM jsonb_array_elements(source_outcomes) s WHERE s->>'exchange'=%s AND s->>'market'=%s AND s->>'status'='complete')
                    ORDER BY completed_at DESC,id DESC LIMIT 1""",
                    (COLLECTION_VERSION, refresh_id, refresh_id, exchange, market),
                )
                source = await cur.fetchone()
                if source is None or source[0] < observed - timedelta(
                    seconds=policy.max_age_seconds
                ):
                    missing_sources.append(exchange + "." + market)
            if missing_sources and not policy.allow_partial_universe:
                raise MarketDataError(
                    "Requested ranking universe has incomplete collection scopes: "
                    + ", ".join(missing_sources)
                )
        explicit = (
            set(policy.conversion_rates)
            | set(policy.conversion.manual_rates)
            | set(policy.conversion.equivalent_currencies)
        )
        needed = {
            i.unit
            for i in inputs
            if i.unit is not None
            and i.unit != policy.comparison_currency
            and i.unit not in explicit
        }
        prices = (
            await spot_prices(c, policy, needed, refresh_id=refresh_id)
            if needed and policy.metric != "volume"
            else []
        )
        entries = evaluate(inputs, policy, observed, prices)
        snapshot = uuid4()
        document = policy_document(policy)
        algorithm = VOLUME_ALGORITHM if policy.metric == "volume" else ALGORITHM
        canonical = json.dumps(document, sort_keys=True, separators=(",", ":"))
        await c.execute(
            """INSERT INTO ranking.snapshots(id,policy_name,policy_json,policy_sha256,algorithm_version,
            comparison_currency,comparison_basis,created_at,status,partial_universe,universe_count,included_count)
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,'draft',%s,%s,%s)""",
            (
                snapshot,
                name,
                json_value(document),
                hashlib.sha256(canonical.encode()).hexdigest(),
                algorithm,
                None if policy.metric == "volume" else policy.comparison_currency,
                json_value(
                    {
                        "metric": "volume",
                        "group_by": "base_asset",
                        "volume_unit": "base_asset",
                        "meaning": "24-hour traded base quantities; comparable only within each base asset",
                        "precision": 80,
                        "algorithm_version": algorithm,
                        "missing_collection_scopes": missing_sources,
                        "refresh_id": refresh_id,
                    }
                    if policy.metric == "volume"
                    else {
                        "rates": policy.conversion_rates,
                        "as_of": policy.conversion_as_of,
                        "source": policy.conversion_source,
                        "manual_preferences": policy.manual_preferences,
                        "meaning": "current priorities, not historical backtest observations",
                        "valuation": "reference_price_turnover; aggregate monetary turnover, not execution-time FX",
                        "conversion_policy": policy.conversion.model_dump(mode="json"),
                        "precision": 80,
                        "algorithm_version": ALGORITHM,
                        "precedence": ["identity", "manual", "equivalence", "stored_spot"],
                        "path_order": [
                            "same_exchange",
                            "fewest_legs",
                            "source_exchange_preference",
                            "newest_weakest_leg",
                            "bridge_preference",
                            "stable_identity",
                        ],
                        "missing_collection_scopes": missing_sources,
                        "refresh_id": refresh_id,
                    }
                ),
                observed,
                bool(missing_sources)
                or any(e.exclusion_reason for e in entries)
                or any(not i.refresh_complete for i in inputs),
                len(entries),
                sum(e.position is not None for e in entries),
            ),
        )
        catalogue = {i.id: i for i in universe}
        conversions: dict[str, int] = {}
        for e in entries:
            conversion_id = None
            if e.conversion is not None:
                evidence = e.conversion.document()
                key = hashlib.sha256(
                    json.dumps(
                        evidence, default=str, sort_keys=True, separators=(",", ":")
                    ).encode()
                ).hexdigest()
                conversion_id = conversions.get(key)
                if conversion_id is None:
                    x = e.conversion
                    cur = await c.execute(
                        """INSERT INTO ranking.conversions(snapshot_id,evidence_sha256,method,source_currency,comparison_currency,target_exchange,rate,numerator,denominator,evidence_json)
                      VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id""",
                        (
                            snapshot,
                            key,
                            x.method,
                            x.source_currency,
                            x.comparison_currency,
                            x.target_exchange,
                            x.rate,
                            str(x.numerator),
                            str(x.denominator),
                            json_value(evidence),
                        ),
                    )
                    row = await cur.fetchone()
                    if row is None:
                        raise MarketDataError("Could not persist conversion evidence.")
                    conversion_id = row[0]
                    conversions[key] = conversion_id
                    for index, leg in enumerate(x.legs):
                        await c.execute(
                            "INSERT INTO ranking.conversion_inputs(conversion_id,observation_id,leg_index) VALUES(%s,%s,%s)",
                            (conversion_id, leg.source.observation_id, index),
                        )
            await c.execute(
                """INSERT INTO ranking.entries(snapshot_id,instrument_id,instrument_reference,observation_id,position,score,exclusion_reason,fallback_reason,conversion_id,native_turnover,native_unit,
                base_asset,group_rank,volume_unit,volume_estimated,volume_evidence,instrument_metadata)
                VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (
                    snapshot,
                    e.instrument_id,
                    e.reference,
                    e.observation_id,
                    e.position,
                    e.score,
                    e.exclusion_reason,
                    e.fallback_reason,
                    conversion_id,
                    e.native_turnover,
                    e.native_unit,
                    e.base_asset,
                    e.group_rank,
                    e.volume_unit,
                    e.volume_estimated,
                    json_value(e.volume_evidence) if e.volume_evidence is not None else None,
                    json_value(
                        {
                            "symbol": catalogue[e.instrument_id].symbol,
                            "base": e.base_asset or catalogue[e.instrument_id].base,
                            "quote": catalogue[e.instrument_id].quote,
                            "exchange": catalogue[e.instrument_id].exchange,
                            "market": catalogue[e.instrument_id].market,
                            "instrument_type": catalogue[e.instrument_id].instrument_type,
                        }
                    ),
                ),
            )
        await c.execute("UPDATE ranking.snapshots SET status='published' WHERE id=%s", (snapshot,))
        return snapshot


async def list_snapshots(c: AsyncConnection[TupleRow], limit: int = 50) -> list[dict[str, object]]:
    if not 1 <= limit <= 10001:
        raise MarketDataError("Ranking limit must be 1..10001.")
    async with c.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            "SELECT id,policy_name,created_at,comparison_currency,partial_universe,universe_count,included_count FROM ranking.snapshots WHERE status='published' ORDER BY created_at DESC,id LIMIT %s",
            (limit,),
        )
        return list(await cur.fetchall())


async def find_latest_snapshot(c: AsyncConnection[TupleRow], name: str) -> UUID | None:
    """Exact policy lookup; partial and old publications remain usable priorities."""
    cur = await c.execute(
        "SELECT id FROM ranking.snapshots WHERE policy_name=%s AND status='published' "
        "ORDER BY created_at DESC,id DESC LIMIT 1",
        (name,),
    )
    row = await cur.fetchone()
    return row[0] if row is not None else None


async def latest_snapshot(c: AsyncConnection[TupleRow], name: str) -> UUID:
    snapshot = await find_latest_snapshot(c, name)
    if snapshot is None:
        raise MarketDataError(
            f"No published snapshot for {name}. Create one with: "
            f"marketforge markets rank update {name} --testing"
        )
    return snapshot


async def show(
    c: AsyncConnection[TupleRow],
    snapshot: UUID,
    *,
    instrument_type: str | None = None,
    base_asset: str | None = None,
    quote_asset: str | None = None,
    match: str | None = None,
) -> dict[str, object]:
    if instrument_type not in {None, "spot", "perpetual", "future"}:
        raise MarketDataError("Unknown catalogue instrument type.")
    async with c.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            "SELECT * FROM ranking.snapshots WHERE id=%s AND status='published'", (snapshot,)
        )
        header = await cur.fetchone()
        if header is None:
            raise MarketDataError("Published ranking not found.")
        await cur.execute(
            """SELECT r.*,COALESCE(metadata.value->>'exchange',e.code) AS exchange,
            COALESCE(metadata.value->>'instrument_type',i.instrument_type) AS instrument_type,
            COALESCE(metadata.value->>'market',i.market_category) AS market_category,
            COALESCE(metadata.value->>'symbol',i.symbol) AS symbol,
            COALESCE(r.base_asset,metadata.value->>'base',i.base_asset) AS base_asset,
            COALESCE(metadata.value->>'quote',i.quote_asset) AS quote_asset,
            jsonb_build_object(
              'symbol',CASE WHEN r.instrument_metadata->>'symbol' IS NOT NULL THEN 'publication'
                WHEN r.volume_evidence#>>'{instrument_specification,symbol}' IS NOT NULL THEN 'retained_volume_observation'
                WHEN o.raw_json#>>'{instrument_specification,symbol}' IS NOT NULL THEN 'retained_observation'
                ELSE 'current_catalogue' END,
              'base_asset',CASE WHEN r.base_asset IS NOT NULL THEN 'publication_group'
                WHEN r.instrument_metadata->>'base' IS NOT NULL THEN 'publication'
                WHEN r.volume_evidence#>>'{instrument_specification,base}' IS NOT NULL THEN 'retained_volume_observation'
                WHEN o.raw_json#>>'{instrument_specification,base}' IS NOT NULL THEN 'retained_observation'
                ELSE 'current_catalogue' END,
              'quote_asset',CASE WHEN r.instrument_metadata->>'quote' IS NOT NULL THEN 'publication'
                WHEN r.volume_evidence#>>'{instrument_specification,quote}' IS NOT NULL THEN 'retained_volume_observation'
                WHEN o.raw_json#>>'{instrument_specification,quote}' IS NOT NULL THEN 'retained_observation'
                ELSE 'current_catalogue' END
            ) AS metadata_sources,
            COALESCE(r.native_turnover,o.turnover) AS native_turnover,COALESCE(r.native_unit,o.turnover_unit) AS native_unit,o.turnover_estimated,
            o.mapping_version,o.source_observed_at,o.fetched_at,o.window_start,o.window_end,o.window_basis,
            o.raw_json->'interpretation' AS turnover_evidence,
            x.method AS conversion_method,x.evidence_json AS conversion_evidence
            FROM ranking.entries r JOIN catalog.instruments i ON i.id=r.instrument_id JOIN catalog.exchanges e ON e.id=i.exchange_id
            LEFT JOIN catalog.metric_observations o ON o.id=r.observation_id
            LEFT JOIN ranking.conversions x ON x.id=r.conversion_id
            CROSS JOIN LATERAL (
              SELECT jsonb_strip_nulls(
                  CASE WHEN jsonb_typeof(o.raw_json->'instrument_specification')='object'
                       THEN o.raw_json->'instrument_specification' ELSE '{}'::jsonb END)
                || jsonb_strip_nulls(
                  CASE WHEN jsonb_typeof(r.volume_evidence->'instrument_specification')='object'
                       THEN r.volume_evidence->'instrument_specification' ELSE '{}'::jsonb END)
                || jsonb_strip_nulls(COALESCE(r.instrument_metadata,'{}'::jsonb)) AS value
            ) metadata
            WHERE r.snapshot_id=%s AND (%s::text IS NULL OR COALESCE(metadata.value->>'instrument_type',i.instrument_type)=%s)
              AND (%s::text IS NULL OR lower(COALESCE(r.base_asset,metadata.value->>'base',i.base_asset))=lower(%s))
              AND (%s::text IS NULL OR lower(COALESCE(metadata.value->>'quote',i.quote_asset))=lower(%s))
              AND (%s::text IS NULL OR COALESCE(metadata.value->>'symbol',i.symbol) ILIKE %s ESCAPE '!')
            ORDER BY r.position NULLS LAST,r.instrument_reference""",
            (
                snapshot,
                instrument_type,
                instrument_type,
                base_asset,
                base_asset,
                quote_asset,
                quote_asset,
                match,
                symbol_pattern(match),
            ),
        )
        entries = list(await cur.fetchall())
        from collections import Counter

        summary = Counter(
            "excluded" if r["position"] is None else str(r["conversion_method"] or "legacy")
            for r in entries
        )
        await cur.execute(
            "SELECT * FROM ranking.conversions WHERE snapshot_id=%s ORDER BY id", (snapshot,)
        )
        return {
            "snapshot": header,
            "entries": entries,
            "conversions": list(await cur.fetchall()),
            "conversion_summary": dict(summary),
            "instrument_type": instrument_type,
            "base_asset": base_asset,
            "quote_asset": quote_asset,
            "match": match,
            "metadata_semantics": "Publication metadata is preferred, then retained observation specifications; fields labeled current_catalogue use live metadata for older snapshots.",
        }


class ConcreteStream(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    instrument_reference: str
    data_type: Literal["trades", "order_book_l2"]
    feed: Literal["incremental", "snapshot"] | None = None

    @model_validator(mode="after")
    def identity(self) -> "ConcreteStream":
        parts = self.instrument_reference.split(".")
        if (
            len(parts) not in {3, 4}
            or parts[1] not in {"spot", "linear", "inverse"}
            or not all(parts)
            or (len(parts) == 4 and parts[2] not in {"spot", "perpetual", "future"})
        ):
            raise ValueError(
                "Use exchange.market.symbol or exchange.market.instrument_type.symbol."
            )
        gate = self.instrument_reference.startswith("gateio.") and self.data_type == "order_book_l2"
        if gate and self.feed is None:
            raise ValueError("Gate depth streams need an explicit feed.")
        if not gate and self.feed is not None:
            raise ValueError("Feed variant is only supported for Gate depth.")
        return self

    @property
    def name(self) -> str:
        return (
            self.instrument_reference
            + "."
            + self.data_type
            + ("." + self.feed if self.feed else "")
        )


@dataclass(frozen=True)
class ResolvedStream:
    name: str
    stream_rank: int
    ranking_position: int


def resolve_streams(
    positions: dict[str, int], streams: list[ConcreteStream]
) -> list[ResolvedStream]:
    if len({s.name for s in streams}) != len(streams):
        raise MarketDataError("Duplicate concrete stream identity.")
    if any(s.instrument_reference not in positions for s in streams):
        raise MarketDataError("Stream instrument is absent/excluded from selected ranking.")
    ordered = sorted(streams, key=lambda s: (positions[s.instrument_reference], s.name))
    return [
        ResolvedStream(s.name, index + 1, positions[s.instrument_reference])
        for index, s in enumerate(ordered)
    ]


async def resolve_snapshot(
    c: AsyncConnection[TupleRow], snapshot: UUID, streams: list[ConcreteStream]
) -> list[ResolvedStream]:
    cur = await c.execute(
        "SELECT policy_json->>'metric' FROM ranking.snapshots WHERE id=%s AND status='published'",
        (snapshot,),
    )
    header = await cur.fetchone()
    if header is None:
        raise MarketDataError("Published ranking not found.")
    if header[0] == "volume":
        raise MarketDataError(
            "Grouped base-volume snapshots cannot supply one comparable ordering across assets."
        )
    # Pinned priorities only: no price resolution, policy rebuild, or full snapshot materialization.
    cur = await c.execute(
        "SELECT instrument_reference,position FROM ranking.entries WHERE snapshot_id=%s AND position IS NOT NULL AND instrument_reference=ANY(%s::text[])",
        (snapshot, list({s.instrument_reference for s in streams})),
    )
    return resolve_streams({row[0]: row[1] for row in await cur.fetchall()}, streams)
