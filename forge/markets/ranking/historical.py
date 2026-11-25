"""Read retained turnover priorities for historical decisions."""

import json
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID

from psycopg import AsyncConnection
from psycopg.rows import TupleRow, dict_row

from forge.markets.ranking.policy import RankingPolicy


async def turnover_evidence(
    connection: AsyncConnection[TupleRow],
    name: str,
    snapshot: UUID | None,
    cutoff: datetime,
    references: list[str],
    *,
    ex_post: bool,
) -> tuple[dict[str, Any] | None, list[dict[str, Any]], str]:
    """Read the exact snapshot or the latest published one available at the cutoff."""
    async with connection.cursor(row_factory=dict_row) as cursor:
        if snapshot is None:
            await cursor.execute(
                """SELECT id FROM ranking.snapshots
                WHERE policy_name=%s AND status='published'
                  AND policy_json->>'metric'='turnover_24h'
                  AND published_at <= %s AND created_at <= %s
                ORDER BY published_at DESC, id DESC LIMIT 1""",
                (name, cutoff, cutoff),
            )
            row = await cursor.fetchone()
            if row is None:
                await cursor.execute(
                    """SELECT * FROM ranking.snapshots WHERE policy_name=%s AND status='published'
                    ORDER BY created_at DESC,id DESC LIMIT 1""",
                    (name,),
                )
                current = await cursor.fetchone()
                if current is None:
                    return (
                        None,
                        [],
                        "No published snapshot for this policy in the selected registry.",
                    )
                if current["published_at"] is None:
                    return (
                        current,
                        [],
                        "The published snapshot lacks a verified publication timestamp.",
                    )
                if max(current["published_at"], current["created_at"]) > cutoff:
                    return (
                        current,
                        [],
                        "Current market ranking postdates the historical decision cutoff.",
                    )
                return (
                    current,
                    [],
                    "The policy has no published turnover snapshot at the historical cutoff.",
                )
            snapshot = row["id"]
        await cursor.execute(
            """SELECT * FROM ranking.snapshots
            WHERE id=%s AND policy_name=%s AND status='published'""",
            (snapshot, name),
        )
        header = await cursor.fetchone()
        if header is None:
            return (
                None,
                [],
                "The requested policy snapshot is unavailable in the selected registry.",
            )
        policy = RankingPolicy.model_validate_json(json.dumps(header["policy_json"]))
        if (
            policy.metric != "turnover_24h"
            or header["comparison_currency"] != policy.comparison_currency
        ):
            return header, [], "The snapshot does not supply comparable turnover positions."
        published, created = header["published_at"], header["created_at"]
        if not ex_post:
            if published is None or max(published, created) > cutoff:
                return (
                    header,
                    [],
                    "Snapshot publication or build time is unavailable at the historical cutoff.",
                )
            if cutoff - created > timedelta(seconds=policy.max_age_seconds):
                return header, [], "The latest historical snapshot is stale at the cutoff."
        await cursor.execute(
            """SELECT r.instrument_reference,r.instrument_id,r.instrument_metadata,r.position,
                      r.score,r.exclusion_reason,r.fallback_reason,r.observation_id,
                      r.native_turnover,r.native_unit,o.source_observed_at,o.fetched_at,
                      o.window_start,o.window_end,o.turnover_estimated,
                      x.evidence_json AS conversion_evidence
            FROM ranking.entries r
            LEFT JOIN catalog.metric_observations o ON o.id=r.observation_id
            LEFT JOIN ranking.conversions x ON x.id=r.conversion_id
            WHERE r.snapshot_id=%s AND r.instrument_reference=ANY(%s::text[])
            ORDER BY r.instrument_reference""",
            (snapshot, references),
        )
        return header, list(await cursor.fetchall()), "ex_post" if ex_post else "historical"


def turnover_entry_issue(
    row: dict[str, Any], header: dict[str, Any], cutoff: datetime, *, ex_post: bool
) -> str | None:
    if row["position"] is None:
        return str(row["exclusion_reason"])
    if "partial refresh input" in (row["fallback_reason"] or ""):
        return "Incomplete ranking observation refresh."
    policy = RankingPolicy.model_validate_json(json.dumps(header["policy_json"]))
    if row["turnover_estimated"] and policy.estimates == "exclude":
        return "Estimated turnover excluded by the retained policy."
    evidence = row["conversion_evidence"]
    if not evidence or evidence.get("comparison_currency") != policy.comparison_currency:
        return "Missing comparable retained turnover valuation evidence."
    window_start, window_end = row["window_start"], row["window_end"]
    if (
        window_start is None
        or window_end is None
        or window_start.tzinfo is None
        or window_end.tzinfo is None
    ):
        return "Missing validated turnover window."
    if abs((window_end - window_start).total_seconds() - 86400) > policy.max_window_skew_seconds:
        return "Turnover observation does not cover a comparable 24-hour window."
    if ex_post:
        return None

    times: list[datetime | None] = [
        row["fetched_at"],
        row["window_start"],
        row["window_end"],
        row["source_observed_at"] or row["window_end"],
    ]
    valuation_times: list[datetime] = []
    if evidence.get("as_of"):
        valuation_times.append(datetime.fromisoformat(evidence["as_of"]))
    for leg in evidence.get("legs", []):
        for key in ("fetched_at", "effective_observed_at", "window_end", "source_observed_at"):
            if leg.get(key):
                valuation_times.append(datetime.fromisoformat(leg[key]))
    if any(
        point is None or point.tzinfo is None or point > cutoff
        for point in [*times, *valuation_times]
    ):
        return "Turnover or valuation observations are unavailable at the historical cutoff."
    earliest = cutoff - timedelta(seconds=policy.max_age_seconds)
    if row["window_end"] < earliest or row["fetched_at"] < earliest:
        return "Turnover observation is stale at the historical cutoff."
    price_age = policy.conversion.max_price_age_seconds or policy.max_age_seconds
    if any(point < cutoff - timedelta(seconds=price_age) for point in valuation_times):
        return "Turnover valuation observations are stale at the historical cutoff."
    return None
