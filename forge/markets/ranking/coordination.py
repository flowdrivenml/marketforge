"""Database-wide ranking maintenance coordination, inside existing workspace ownership."""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from psycopg import AsyncConnection
from psycopg.rows import TupleRow

# Also used by migration 0012's maintenance deletion function.
RANKING_LOCK = 506746721904


@asynccontextmanager
async def ranking_lock(
    c: AsyncConnection[TupleRow], *, shared: bool = False
) -> AsyncGenerator[None]:
    """Session lock spans collection's separate transactions and artifact publication."""
    acquire = "SELECT pg_advisory_lock_shared(%s)" if shared else "SELECT pg_advisory_lock(%s)"
    release = "SELECT pg_advisory_unlock_shared(%s)" if shared else "SELECT pg_advisory_unlock(%s)"
    await c.execute(acquire, (RANKING_LOCK,))
    try:
        yield
    finally:
        await c.execute(release, (RANKING_LOCK,))
