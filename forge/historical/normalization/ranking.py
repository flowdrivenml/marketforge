"""Reuse a pinned publication or delegate one policy update to the existing workflow."""

import json
from dataclasses import dataclass
from pathlib import Path
from uuid import UUID

from psycopg import AsyncConnection
from psycopg.rows import TupleRow, dict_row

from forge.markets.models import array, obj
from forge.markets.ranking.policy import load_policy
from forge.markets.ranking.service import find_latest_snapshot
from forge.markets.ranking.workflow import refresh_and_publish
from forge.shared.config.library import resolve_config_file
from forge.shared.config.models import HttpSettings
from forge.shared.errors import MarketForgeError


@dataclass
class RankingSelection:
    snapshot: UUID | None
    details: dict[str, object]


class RankingPreparationError(MarketForgeError):
    def __init__(self, details: dict[str, object]) -> None:
        self.details = details
        super().__init__("Ranking preparation failed: " + json.dumps(details, default=str))


async def ensure_ranking(
    c: AsyncConnection[TupleRow],
    dsn: str,
    directory: Path,
    policy: str,
    settings: HttpSettings,
    *,
    dry_run: bool,
) -> RankingSelection:
    # Caller holds database coordination and workspace ownership; recheck after locking.
    path = resolve_config_file(directory, policy)
    snapshot = await find_latest_snapshot(c, policy)
    decision = "reused"
    outcome: dict[str, object] | None = None
    if snapshot is None:
        try:
            configured = load_policy(path)
            if configured.metric != "turnover_24h":
                raise MarketForgeError(
                    "Normalization priorities require a turnover policy; grouped volume is not comparable across assets."
                )
            if dry_run:
                return RankingSelection(
                    None,
                    {
                        "decision": "would_create",
                        "policy": policy,
                        "snapshot": None,
                        "created_at": None,
                        "partial_universe": None,
                        "order_final": False,
                        "automatic_creation": {
                            "status": "not_attempted",
                            "reason": "Dry-run is read-only.",
                        },
                    },
                )
            outcome = await refresh_and_publish(dsn, settings, directory, (policy,))
            published = [
                obj(item)
                for item in array(outcome["rankings"])
                if obj(item).get("status") == "published"
            ]
            if not published:
                raise RankingPreparationError(
                    {"decision": "failed", "policy": policy, "automatic_creation": outcome}
                )
            snapshot = UUID(str(published[0]["snapshot_id"]))
            decision = "created"
        except RankingPreparationError:
            raise
        except (MarketForgeError, OSError, ValueError) as exc:
            raise RankingPreparationError(
                {
                    "decision": "failed",
                    "policy": policy,
                    "error": str(exc),
                    "automatic_creation": outcome,
                }
            ) from exc
    async with c.cursor(row_factory=dict_row) as cursor:
        await cursor.execute(
            "SELECT created_at,partial_universe,policy_json->>'metric' AS metric FROM ranking.snapshots WHERE id=%s AND status='published'",
            (snapshot,),
        )
        header = await cursor.fetchone()
    if header is None:
        raise MarketForgeError(f"Resolved ranking snapshot disappeared: {snapshot}.")
    if header["metric"] == "volume":
        raise MarketForgeError("Normalization priorities cannot use grouped volume rankings.")
    return RankingSelection(
        snapshot,
        {
            "decision": decision,
            "policy": policy,
            "snapshot": str(snapshot),
            "created_at": str(header["created_at"]),
            "partial_universe": header["partial_universe"],
            "order_final": True,
            "automatic_creation": outcome or {"status": "not_needed"},
        },
    )


async def positions_for(c: AsyncConnection[TupleRow], snapshot: UUID | None) -> dict[int, int]:
    if snapshot is None:
        return {}
    cursor = await c.execute(
        "SELECT instrument_id,position FROM ranking.entries WHERE snapshot_id=%s AND position IS NOT NULL",
        (snapshot,),
    )
    return {int(i): int(p) for i, p in await cursor.fetchall()}
