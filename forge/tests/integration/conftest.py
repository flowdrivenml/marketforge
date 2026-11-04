"""Opt-in verified testing database fixture; every test rolls back its own rows."""

from collections.abc import AsyncIterator
from pathlib import Path

import pytest_asyncio
from psycopg import AsyncConnection
from psycopg.rows import TupleRow

from forge.shared.database.testing import testing_connection, testing_dsn

ROOT = Path(__file__).resolve().parents[3]


@pytest_asyncio.fixture
async def database() -> AsyncIterator[AsyncConnection[TupleRow]]:
    async with testing_connection(testing_dsn(ROOT)) as connection:
        async with connection.transaction(force_rollback=True):
            await connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            yield connection
