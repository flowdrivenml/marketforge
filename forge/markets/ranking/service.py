"""Immutable deterministic ranking publication and independent stream resolution."""

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, localcontext
from typing import Literal
from uuid import UUID, uuid4

from psycopg import AsyncConnection
from psycopg.pq import TransactionStatus
from psycopg.rows import TupleRow, dict_row
from pydantic import BaseModel, ConfigDict, model_validator

from forge.markets.models import MarketDataError
from forge.markets.ranking.policy import RankingPolicy
from forge.markets.repository import instruments, json_value
from forge.markets.service import EXCHANGES


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


@dataclass(frozen=True)
class RankedEntry:
    instrument_id: int
    reference: str
    observation_id: int | None
    position: int | None
    score: Decimal | None
    exclusion_reason: str | None
    fallback_reason: str | None


def evaluate(inputs: list[RankingInput], policy: RankingPolicy, now: datetime) -> list[RankedEntry]:
    if not inputs:
        raise MarketDataError("Ranking universe is empty.")
    if policy.conversion_as_of is not None and (
        policy.conversion_as_of > now
        or now - policy.conversion_as_of > timedelta(seconds=policy.max_age_seconds)
    ):
        raise MarketDataError("Conversion provenance is stale or in the future.")
    latest = max((i.window_end for i in inputs if i.window_end is not None), default=now)
    included: list[RankedEntry] = []
    excluded: list[RankedEntry] = []
    for i in inputs:
        reason = None
        fallback = None
        score = None
        if not i.refresh_complete and not policy.allow_partial_universe:
            reason = "incomplete source refresh"
        if i.observation_id is None or i.turnover is None or i.unit is None:
            reason = "missing turnover observation"
        elif (
            i.fetched_at is None
            or i.window_end is None
            or min(i.fetched_at, i.window_end) < now - timedelta(seconds=policy.max_age_seconds)
        ):
            reason = "stale observation"
        elif i.window_end > now + timedelta(seconds=policy.max_window_skew_seconds):
            reason = "future source window"
        elif latest - i.window_end > timedelta(seconds=policy.max_window_skew_seconds):
            reason = "incompatible source windows"
        elif i.estimated and policy.estimates == "exclude":
            reason = "estimated turnover excluded"
        if reason is None:
            if i.unit == policy.comparison_currency:
                rate = Decimal(1)
            else:
                rate = policy.conversion_rates.get(i.unit) if i.unit is not None else None
            if rate is None:
                reason = "no explicit conversion for " + str(i.unit)
            else:
                with localcontext() as ctx:
                    ctx.prec = 50
                    score = i.turnover * rate if i.turnover is not None else None
                fallback = (
                    "estimated turnover"
                    if i.estimated
                    else "partial refresh input"
                    if not i.refresh_complete
                    else None
                )
                if i.unit != policy.comparison_currency:
                    fallback = (
                        (fallback + "; " if fallback else "")
                        + "explicit conversion "
                        + str(i.unit)
                        + " -> "
                        + policy.comparison_currency
                    )
        entry = RankedEntry(
            i.instrument_id, i.reference, i.observation_id, None, score, reason, fallback
        )
        (included if reason is None else excluded).append(entry)
    if excluded and not policy.allow_partial_universe:
        raise MarketDataError(
            "Incomplete/comparability-filtered universe; explicitly allow_partial_universe to publish exclusions."
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
            -(e.score or Decimal(0)),
            e.reference,
        )
    )
    from dataclasses import replace

    return [replace(e, position=index + 1) for index, e in enumerate(included)] + sorted(
        excluded, key=lambda e: e.reference
    )


async def build(
    c: AsyncConnection[TupleRow], name: str, policy: RankingPolicy, *, now: datetime | None = None
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
        universe = [
            i
            for i in await instruments(c, limit=10000)
            if i.model().active
            and i.exchange in (policy.exchanges or list(EXCHANGES))
            and i.market in policy.markets
            and (not policy.instruments or i.model().reference in policy.instruments)
        ]
        if len(universe) >= 10000:
            raise MarketDataError("Ranking universe exceeds the supported bound.")
        if policy.instruments and set(policy.instruments) != {
            i.model().reference for i in universe
        }:
            raise MarketDataError(
                "Some explicitly selected instruments are missing from the catalogue."
            )
        inputs: list[RankingInput] = []
        for i in universe:
            cur = await c.execute(
                """SELECT o.id,o.turnover,o.turnover_unit,o.turnover_estimated,o.fetched_at,o.window_end,r.status
                FROM catalog.metric_observations o JOIN catalog.metric_refreshes r ON r.id=o.refresh_id
                WHERE o.instrument_id=%s AND r.status IN ('complete','partial') ORDER BY o.window_end DESC,o.fetched_at DESC,o.id DESC LIMIT 1""",
                (i.id,),
            )
            row = await cur.fetchone()
            inputs.append(
                RankingInput(
                    i.id,
                    i.model().reference,
                    row[0] if row else None,
                    row[1] if row else None,
                    row[2] if row else None,
                    row[3] if row else False,
                    row[4] if row else None,
                    row[5] if row else None,
                    row is not None and row[6] == "complete",
                )
            )
        missing_sources: list[str] = []
        if not policy.instruments:
            for exchange in policy.exchanges or list(EXCHANGES):
                for market in policy.markets:
                    if exchange == "bitget" and market == "inverse":
                        continue
                    cur = await c.execute(
                        """SELECT status,completed_at FROM catalog.metric_refreshes
                        WHERE scope_json->'exchanges' ? %s AND scope_json->'markets' ? %s
                          AND scope_json->>'symbol' IS NULL AND status<>'running'
                        ORDER BY started_at DESC,id DESC LIMIT 1""",
                        (exchange, market),
                    )
                    source = await cur.fetchone()
                    if (
                        source is None
                        or source[0] != "complete"
                        or source[1] < observed - timedelta(seconds=policy.max_age_seconds)
                    ):
                        missing_sources.append(exchange + "." + market)
            if missing_sources and not policy.allow_partial_universe:
                raise MarketDataError(
                    "Requested ranking universe has incomplete collection scopes: "
                    + ", ".join(missing_sources)
                )
        entries = evaluate(inputs, policy, observed)
        snapshot = uuid4()
        document = policy.model_dump(mode="json")
        canonical = json.dumps(document, sort_keys=True, separators=(",", ":"))
        await c.execute(
            """INSERT INTO ranking.snapshots(id,policy_name,policy_json,policy_sha256,algorithm_version,
            comparison_currency,comparison_basis,created_at,status,partial_universe,universe_count,included_count)
            VALUES(%s,%s,%s,%s,'turnover24h-decimal50-v1',%s,%s,%s,'draft',%s,%s,%s)""",
            (
                snapshot,
                name,
                json_value(document),
                hashlib.sha256(canonical.encode()).hexdigest(),
                policy.comparison_currency,
                json_value(
                    {
                        "rates": policy.conversion_rates,
                        "as_of": policy.conversion_as_of,
                        "source": policy.conversion_source,
                        "manual_preferences": policy.manual_preferences,
                        "meaning": "current priorities, not historical backtest observations",
                        "missing_collection_scopes": missing_sources,
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
        for e in entries:
            await c.execute(
                """INSERT INTO ranking.entries(snapshot_id,instrument_id,instrument_reference,observation_id,position,score,exclusion_reason,fallback_reason)
                VALUES(%s,%s,%s,%s,%s,%s,%s,%s)""",
                (
                    snapshot,
                    e.instrument_id,
                    e.reference,
                    e.observation_id,
                    e.position,
                    e.score,
                    e.exclusion_reason,
                    e.fallback_reason,
                ),
            )
        await c.execute("UPDATE ranking.snapshots SET status='published' WHERE id=%s", (snapshot,))
        return snapshot


async def list_snapshots(c: AsyncConnection[TupleRow], limit: int = 50) -> list[dict[str, object]]:
    if not 1 <= limit <= 1000:
        raise MarketDataError("Ranking limit must be 1..1000.")
    async with c.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            "SELECT id,policy_name,created_at,comparison_currency,partial_universe,universe_count,included_count FROM ranking.snapshots WHERE status='published' ORDER BY created_at DESC,id LIMIT %s",
            (limit,),
        )
        return list(await cur.fetchall())


async def show(c: AsyncConnection[TupleRow], snapshot: UUID) -> dict[str, object]:
    async with c.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            "SELECT * FROM ranking.snapshots WHERE id=%s AND status='published'", (snapshot,)
        )
        header = await cur.fetchone()
        if header is None:
            raise MarketDataError("Published ranking not found.")
        await cur.execute(
            "SELECT * FROM ranking.entries WHERE snapshot_id=%s ORDER BY position NULLS LAST,instrument_reference",
            (snapshot,),
        )
        return {"snapshot": header, "entries": list(await cur.fetchall())}


class ConcreteStream(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    instrument_reference: str
    data_type: Literal["trades", "order_book_l2"]
    feed: Literal["incremental", "snapshot"] | None = None

    @model_validator(mode="after")
    def identity(self) -> "ConcreteStream":
        parts = self.instrument_reference.split(".")
        if len(parts) != 3 or parts[1] not in {"spot", "linear", "inverse"} or not all(parts):
            raise ValueError("Use exchange.market.symbol.")
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
    await show(c, snapshot)
    # Query the explicit output model independently of any Rust job schema.
    cur = await c.execute(
        "SELECT instrument_reference,position FROM ranking.entries WHERE snapshot_id=%s AND position IS NOT NULL",
        (snapshot,),
    )
    return resolve_streams({row[0]: row[1] for row in await cur.fetchall()}, streams)
