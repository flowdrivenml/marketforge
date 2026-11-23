"""Resolve and verify the database selected for a Forge operation."""

from collections.abc import AsyncGenerator, Generator, Mapping
from contextlib import asynccontextmanager, contextmanager
from contextvars import ContextVar
from pathlib import Path
from uuid import UUID

import psycopg
from psycopg import AsyncConnection
from psycopg.conninfo import conninfo_to_dict
from psycopg.rows import TupleRow

from forge.shared.database.credentials import DatabaseCredentialsError, resolve_database_dsn
from forge.shared.errors import MarketForgeError

TEST_DATABASE_NAME = "marketforge_test"
_testing: ContextVar[bool] = ContextVar("database_testing", default=False)
_registry: ContextVar[str | None] = ContextVar("verified_registry", default=None)


class DatabaseError(MarketForgeError):
    """Sanitized database selection or access failure."""


def is_testing() -> bool:
    return _testing.get()


@contextmanager
def database_environment(*, testing: bool) -> Generator[None]:
    """Keep the selection scoped to this operation and its async tasks."""
    token = _testing.set(testing)
    try:
        yield
    finally:
        _testing.reset(token)


@contextmanager
def pinned_registry(identity: str | None) -> Generator[None]:
    """Scope the expected UUID, or independently resolve an identity when unset."""
    token = _registry.set(identity)
    try:
        yield
    finally:
        _registry.reset(token)


def database_dsn(root: Path | None = None, *, testing: bool | None = None) -> str:
    try:
        return resolve_database_dsn(
            testing=is_testing() if testing is None else testing, project_root=root
        )
    except DatabaseCredentialsError as error:
        raise DatabaseError(str(error)) from None


def expected_database(dsn: str, *, testing: bool) -> str:
    purpose = "testing" if testing else "production"
    try:
        name = conninfo_to_dict(dsn).get("dbname")
    except (psycopg.Error, ValueError):
        raise DatabaseError(f"Invalid {purpose} PostgreSQL credentials.") from None
    if testing and name != TEST_DATABASE_NAME:
        raise DatabaseError("TEST_DATABASE_KEY must explicitly select marketforge_test.")
    if not testing and (not isinstance(name, str) or not name or name == TEST_DATABASE_NAME):
        raise DatabaseError(
            "PRODUCTION_DATABASE_KEY must explicitly select a production database "
            "distinct from marketforge_test."
        )
    assert isinstance(name, str)
    return name


def verify_identity(row: TupleRow | None, *, testing: bool) -> UUID:
    purpose = "testing" if testing else "production"
    if row is None or not isinstance(row[0], UUID) or row[1] != purpose:
        raise DatabaseError(f"Forge database identity must contain a UUID and purpose {purpose}.")
    return row[0]


@asynccontextmanager
async def database_connection(
    dsn: str, *, testing: bool | None = None
) -> AsyncGenerator[AsyncConnection[TupleRow], None]:
    """Verify the actual database and retained identity before allowing registry access."""
    testing = is_testing() if testing is None else testing
    purpose = "testing" if testing else "production"
    name = expected_database(dsn, testing=testing)
    try:
        async with await AsyncConnection.connect(
            dsn, autocommit=True, connect_timeout=10
        ) as connection:
            cursor = await connection.execute("SELECT current_database()")
            if await cursor.fetchone() != (name,):
                raise DatabaseError(
                    f"Connected database differs from the configured {purpose} database."
                )
            cursor = await connection.execute(
                "SELECT database_id, purpose FROM forge_meta.database_identity WHERE singleton"
            )
            identity = verify_identity(await cursor.fetchone(), testing=testing)
            if (pinned := _registry.get()) is not None and str(identity) != pinned:
                raise DatabaseError(
                    "This operation requires the original verified database identity."
                )
            yield connection
    except psycopg.Error:
        raise DatabaseError(
            f"{purpose.capitalize()} PostgreSQL operation failed; verify access and Forge migrations."
        ) from None


def database_purpose() -> str:
    return "testing" if is_testing() else "production"


def database_flag() -> str:
    return f"--{database_purpose()}"


async def registry_id(connection: AsyncConnection[TupleRow]) -> str:
    cursor = await connection.execute(
        "SELECT database_id, purpose FROM forge_meta.database_identity WHERE singleton"
    )
    return str(verify_identity(await cursor.fetchone(), testing=is_testing()))


async def selected_registry_id(root: Path) -> str:
    async with database_connection(database_dsn(root)) as connection:
        return await registry_id(connection)


def require_registry(evidence: Mapping[str, object], identity: str) -> None:
    """Validate retained identity without changing the original artifact."""
    purpose = evidence.get("environment")
    if (purpose is not None and purpose != database_purpose()) or evidence.get(
        "registry_id"
    ) != identity:
        raise DatabaseError("This operation requires the original verified database identity.")
