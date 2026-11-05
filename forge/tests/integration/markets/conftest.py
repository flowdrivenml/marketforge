"""Committed scope fixtures for collectors which own independent connections."""

from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from pathlib import Path
from uuid import uuid4

import pytest_asyncio

from forge.shared.database.testing import testing_connection as connect_test
from forge.shared.database.testing import testing_dsn as load_test_dsn

ROOT = Path(__file__).resolve().parents[4]


@dataclass(frozen=True)
class CollectionScope:
    dsn: str = field(repr=False)
    symbol: str


@pytest_asyncio.fixture
async def collection_scope() -> AsyncIterator[CollectionScope]:
    scope = CollectionScope(load_test_dsn(ROOT), "COLLECT_" + uuid4().hex)
    try:
        yield scope
    finally:
        async with connect_test(scope.dsn) as c, c.transaction():
            cur = await c.execute(
                "SELECT id FROM catalog.metric_refreshes WHERE scope_json->>'symbol'=%s",
                (scope.symbol,),
            )
            refreshes = [r[0] for r in await cur.fetchall()]
            await c.execute(
                "DELETE FROM catalog.instrument_metrics WHERE instrument_id IN (SELECT id FROM catalog.instruments WHERE symbol=%s)",
                (scope.symbol,),
            )
            await c.execute(
                "DELETE FROM catalog.metric_observations WHERE refresh_id=ANY(%s)", (refreshes,)
            )
            await c.execute(
                "DELETE FROM catalog.instrument_api_raw WHERE instrument_id IN (SELECT id FROM catalog.instruments WHERE symbol=%s)",
                (scope.symbol,),
            )
            await c.execute(
                "DELETE FROM catalog.instrument_specs WHERE instrument_id IN (SELECT id FROM catalog.instruments WHERE symbol=%s)",
                (scope.symbol,),
            )
            await c.execute("DELETE FROM catalog.instruments WHERE symbol=%s", (scope.symbol,))
            await c.execute("DELETE FROM catalog.metric_refreshes WHERE id=ANY(%s)", (refreshes,))
            await c.execute(
                "DELETE FROM catalog.instrument_syncs WHERE scope_json->>'symbol'=%s",
                (scope.symbol,),
            )
