"""Explicit testing-only connections and non-executable environment loading."""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path

import psycopg
from psycopg import AsyncConnection
from psycopg.conninfo import conninfo_to_dict
from psycopg.rows import TupleRow

from forge.shared.database.credentials import DatabaseCredentialsError, resolve_database_dsn
from forge.shared.errors import MarketForgeError


class TestingDatabaseError(MarketForgeError):
    """Testing database selection or access failed (without disclosing secrets)."""


def testing_dsn(root: Path | None = None) -> str:
    """Resolve exclusively testing credentials through the shared loader."""
    try:
        return resolve_database_dsn(testing=True, project_root=root)
    except DatabaseCredentialsError as error:
        raise TestingDatabaseError(str(error)) from None


@asynccontextmanager
async def testing_connection(dsn: str) -> AsyncGenerator[AsyncConnection[TupleRow], None]:
    """Verify both identity guards before yielding a mutation-capable connection."""
    try:
        if conninfo_to_dict(dsn).get("dbname") != "marketforge_test":
            raise TestingDatabaseError(
                "The testing connection must explicitly name marketforge_test."
            )
        async with await AsyncConnection.connect(
            dsn, autocommit=True, connect_timeout=10
        ) as connection:
            cursor = await connection.execute("SELECT current_database()")
            if await cursor.fetchone() != ("marketforge_test",):
                raise TestingDatabaseError("Testing operations require marketforge_test.")
            cursor = await connection.execute(
                "SELECT purpose FROM forge_meta.database_identity WHERE singleton"
            )
            if await cursor.fetchone() != ("testing",):
                raise TestingDatabaseError("Database purpose must be testing.")
            yield connection
    except psycopg.Error:
        raise TestingDatabaseError(
            "Testing PostgreSQL operation failed; verify access and migrations."
        ) from None
