"""Testing-only archive registry fixtures with isolated rollback."""

import os
from collections.abc import AsyncGenerator
from datetime import UTC, datetime
from uuid import uuid4

import pytest
import pytest_asyncio
from psycopg import AsyncConnection
from psycopg.rows import TupleRow

from forge.historical.archives.config import AcquisitionInterval, ArchiveRequest
from forge.historical.archives.models import DiscoveredArchive


@pytest_asyncio.fixture
async def connection() -> AsyncGenerator[AsyncConnection[TupleRow], None]:
    dsn = os.environ.get("MARKETFORGE_DATABASE_URL_TEST")

    if not dsn:
        pytest.skip("MARKETFORGE_DATABASE_URL_TEST is not set.")

    async with await AsyncConnection.connect(
        dsn,
        autocommit=True,
        connect_timeout=10,
    ) as connection:
        cursor = await connection.execute("SELECT current_database()")
        database = await cursor.fetchone()

        if database != ("marketforge_test",):
            pytest.fail("Registry tests require the marketforge_test database.")

        cursor = await connection.execute("""
            SELECT purpose
            FROM forge_meta.database_identity
            WHERE singleton = TRUE
            """)
        identity = await cursor.fetchone()

        if identity != ("testing",):
            pytest.fail("Registry tests require a testing database identity.")

        async with connection.transaction(force_rollback=True):
            yield connection


@pytest.fixture
def archive() -> DiscoveredArchive:
    key = uuid4().hex

    return DiscoveredArchive(
        request=ArchiveRequest.model_validate(
            {
                "exchange": "bybit",
                "instrument_type": "spot",
                "market_category": "spot",
                "data_type": "trades",
                "scope": {
                    "kind": "instrument",
                    "identifier": "BTCUSDT",
                },
            }
        ),
        source_object_key=f"integration-test/{key}",
        filename=f"{key}.csv.gz",
        interval=AcquisitionInterval(
            start=datetime(2026, 9, 1, tzinfo=UTC),
            end=datetime(2026, 9, 2, tzinfo=UTC),
        ),
        download_url="https://exchange.test/archive?signature=first",
    )
