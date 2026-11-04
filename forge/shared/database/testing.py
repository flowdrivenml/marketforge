"""Explicit testing-only connections and non-executable environment loading."""

import os
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path

import psycopg
from psycopg import AsyncConnection
from psycopg.conninfo import conninfo_to_dict
from psycopg.rows import TupleRow

from forge.shared.errors import MarketForgeError


class TestingDatabaseError(MarketForgeError):
    """Testing database selection or access failed (without disclosing secrets)."""


def testing_dsn(root: Path) -> str:
    """Read only the testing key as literal text; never evaluate shell syntax."""
    value = os.environ.get("MARKETFORGE_DATABASE_URL_TEST")
    if value is None:
        source = root / ".secrets" / "postgres.env"
        if source.is_file():
            for line in source.read_text(encoding="utf-8").splitlines():
                key, separator, candidate = line.strip().removeprefix("export ").partition("=")
                if separator and key.strip() == "MARKETFORGE_DATABASE_URL_TEST":
                    value = candidate.strip()
                    if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
                        value = value[1:-1]
                    break
    if not value or not value.strip():
        raise TestingDatabaseError("Set MARKETFORGE_DATABASE_URL_TEST explicitly.")
    return value


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
