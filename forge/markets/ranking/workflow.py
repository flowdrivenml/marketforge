"""Refresh once and publish independent preset snapshots without acquiring ownership."""

import tomllib
from pathlib import Path

from pydantic import ValidationError

from forge.historical.archives.operation import failure_message
from forge.markets.models import array, obj
from forge.markets.progress import ProgressCallback
from forge.markets.ranking.policy import load_policy
from forge.markets.ranking.service import build, show
from forge.markets.service import collect
from forge.shared.config.models import HttpSettings
from forge.shared.database.testing import testing_connection
from forge.shared.errors import MarketForgeError

STANDARD_POLICIES = ("turnover", "volume")


def reason(error: Exception) -> str:
    if isinstance(error, ValidationError):
        return "; ".join(
            ".".join(str(p) for p in detail["loc"]) + ": " + detail["msg"]
            for detail in error.errors(
                include_input=False, include_context=False, include_url=False
            )
        )
    if isinstance(error, (MarketForgeError, OSError)):
        return str(error)
    if isinstance(error, tomllib.TOMLDecodeError):
        return "Invalid policy TOML."
    return failure_message(error)


async def refresh_and_publish(
    dsn: str,
    settings: HttpSettings,
    directory: Path,
    names: tuple[str, ...] = STANDARD_POLICIES,
    *,
    progress: ProgressCallback | None = None,
) -> dict[str, object]:
    try:
        refresh = await collect(dsn, settings, metadata=False, progress=progress)
    except Exception as error:
        refresh = {"status": "failed", "error": reason(error)}
    outcomes: list[dict[str, object]] = []
    refresh_id = refresh.get("refresh_id")
    for name in names:
        try:
            policy = load_policy(directory / f"{name}.toml")
            if type(refresh_id) is not int or refresh.get("status") not in {"complete", "partial"}:
                raise MarketForgeError(
                    "No completed statistics refresh is available for this update."
                )
            async with testing_connection(dsn) as connection:
                snapshot = await build(connection, name, policy, refresh_id=refresh_id)
                header = obj((await show(connection, snapshot))["snapshot"])
            outcomes.append(
                {
                    "policy": name,
                    "status": "published",
                    "snapshot_id": str(snapshot),
                    "created_at": header["created_at"],
                    "partial_universe": header["partial_universe"],
                }
            )
        except Exception as error:
            outcomes.append({"policy": name, "status": "failed", "error": reason(error)})
    failed_sources = [
        source
        for source in array(refresh.get("sources", []))
        if obj(source).get("status") != "complete"
    ]
    status = (
        "cancelled"
        if refresh.get("status") == "cancelled"
        else "complete"
        if refresh.get("status") == "complete"
        and all(outcome["status"] == "published" for outcome in outcomes)
        else "partial"
    )
    return {
        "schema_version": 1,
        "status": status,
        "refresh": refresh,
        "failed_scopes": failed_sources,
        "rankings": outcomes,
    }
