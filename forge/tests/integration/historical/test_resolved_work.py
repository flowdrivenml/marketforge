"""Verify ownership arbitration on a migrated testing registry."""

import asyncio
import os
from pathlib import Path
from uuid import uuid4

import pytest

from forge.historical.engine.work import WorkIndex, identity
from forge.shared.database.connection import (
    database_connection,
    database_environment,
    selected_registry_id,
)
from forge.shared.errors import MarketForgeError


@pytest.mark.asyncio
async def test_work_claim_excludes_a_second_materializing_owner(tmp_path: Path) -> None:
    dsn = os.environ.get("TEST_DATABASE_KEY")
    if not dsn:
        pytest.skip("Set TEST_DATABASE_KEY to a migrated disposable testing database.")
    with database_environment(testing=True):
        registry = await selected_registry_id(tmp_path)
        first = WorkIndex(tmp_path, registry, "convert", "attempt-a")
        second = WorkIndex(tmp_path, registry, "convert", "attempt-b")
        key = identity("convert", registry, "fixture-build", {"fixture": uuid4().hex})
        candidate = {key: {"report_path": "/fixture/report.json", "path": "/fixture/data"}}
        try:
            outcomes = await asyncio.gather(
                first.acquire(candidate), second.acquire(candidate), return_exceptions=True
            )
            assert outcomes.count({}) == 1
            loser_error = next(outcome for outcome in outcomes if outcome != {})
            assert isinstance(loser_error, MarketForgeError)
            assert "active or requires recovery" in str(loser_error)
            winner, loser = (first, second) if outcomes[0] == {} else (second, first)
            await loser.release([key])
            with pytest.raises(MarketForgeError, match="ownership changed"):
                await loser.complete(key, {"path": "wrong"})
            await winner.release([key])
            assert await loser.acquire(candidate) == {}
            with pytest.raises(MarketForgeError, match="ownership changed"):
                await winner.complete(key, {"path": "delayed"})
            ready = {"path": "/fixture/data"}
            await loser.complete(key, ready)
            assert (await winner.acquire(candidate))[key][2] == ready
        finally:
            async with database_connection(dsn) as connection:
                await connection.execute(
                    "DELETE FROM historical.resolved_work WHERE registry_id=%s AND work_key=%s",
                    (registry, key),
                )
