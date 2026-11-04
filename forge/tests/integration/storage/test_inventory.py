"""Cache-only planning and registry inventory do not fetch or rewrite bytes."""

import asyncio
from pathlib import Path

import pytest
from psycopg import AsyncConnection
from psycopg.rows import TupleRow

from forge.historical.archives.config import AcquisitionConfig
from forge.historical.archives.planning import plan
from forge.storage.service import inventory

pytestmark = [pytest.mark.integration, pytest.mark.database]


@pytest.mark.asyncio
async def test_cache_only_plan_is_read_only(
    database: AsyncConnection[TupleRow], tmp_path: Path
) -> None:
    from datetime import UTC, datetime

    from forge.historical.archives.config import AcquisitionInterval, ArchiveRequest, ArchiveScope

    config = AcquisitionConfig(
        interval=AcquisitionInterval(
            start=datetime(2026, 1, 1, tzinfo=UTC), end=datetime(2026, 1, 2, tzinfo=UTC)
        ),
        requests=[
            ArchiveRequest(
                exchange="bybit",
                instrument_type="spot",
                market_category="spot",
                data_type="trades",
                scope=ArchiveScope(kind="instrument", identifier="NO_NETWORK_TEST"),
            )
        ],
    )
    await database.execute("SET LOCAL transaction_read_only=on")
    result = await plan(database, config, tmp_path)
    assert result["cache_only"] is True
    assert result["sampled_archives"] == []
    assert await inventory(database, root=tmp_path) == []
    assert await asyncio.to_thread(lambda: list(tmp_path.iterdir())) == []
