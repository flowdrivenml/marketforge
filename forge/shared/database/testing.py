"""Compatibility helpers for explicitly testing-only database operations."""

from contextlib import AbstractAsyncContextManager
from pathlib import Path

from psycopg import AsyncConnection
from psycopg.rows import TupleRow

from forge.shared.database.connection import (
    DatabaseError as TestingDatabaseError,
)
from forge.shared.database.connection import (
    database_connection,
    database_dsn,
)

__all__ = ["TestingDatabaseError", "testing_connection", "testing_dsn"]


def testing_dsn(root: Path | None = None) -> str:
    return database_dsn(root, testing=True)


def testing_connection(dsn: str) -> AbstractAsyncContextManager[AsyncConnection[TupleRow]]:
    return database_connection(dsn, testing=True)
