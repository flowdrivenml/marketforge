"""Read-only inspection of the selected database migration state."""

from dataclasses import dataclass, replace
from pathlib import Path
from typing import Literal

import psycopg
from psycopg.rows import class_row

from forge.shared.database.connection import (
    DatabaseError,
    database_dsn,
    expected_database,
    is_testing,
    verify_identity,
)
from forge.shared.database.migrations import (
    AppliedMigration,
    MigrationError,
    load_migrations,
    validate_history,
)


@dataclass(frozen=True, slots=True)
class DatabaseStatus:
    """Verified database identity and migration state."""

    database: str
    initialized: bool
    database_id: str | None
    purpose: str | None
    applied: tuple[int, ...]
    pending: tuple[int, ...]
    missing_schemas: tuple[str, ...] = ()


InspectionStage = Literal["credentials", "connection", "identity", "registry", "history", "schemas"]


class DatabaseInspectionError(MigrationError):
    """Sanitized failure with observations established before the failing step."""

    def __init__(
        self,
        message: str,
        stage: InspectionStage,
        *,
        connected: bool = False,
        status: DatabaseStatus | None = None,
    ) -> None:
        super().__init__(message)
        self.stage = stage
        self.connected = connected
        self.status = status


def inspect_database(
    *, project_root: Path | None = None, testing: bool | None = None
) -> DatabaseStatus:
    """Inspect the selected database in a bounded, read-only transaction."""
    testing = is_testing() if testing is None else testing
    try:
        dsn = database_dsn(project_root, testing=testing)
        name = expected_database(dsn, testing=testing)
    except DatabaseError as error:
        raise DatabaseInspectionError(str(error), "credentials") from None

    stage: InspectionStage = "connection"
    connected = False
    status: DatabaseStatus | None = None
    try:
        with psycopg.connect(dsn, autocommit=True, connect_timeout=10) as connection:
            connected = True
            stage = "identity"
            with connection.transaction():
                connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
                connection.execute("SET LOCAL statement_timeout = '5s'")
                connection.execute("SET LOCAL lock_timeout = '5s'")

                if connection.execute("SELECT current_database()").fetchone() != (name,):
                    raise MigrationError("Connected database differs from the configured database.")

                stage = "registry"
                metadata = connection.execute("""
                    SELECT
                        to_regclass('forge_meta.database_identity') IS NOT NULL,
                        to_regclass('forge_meta.schema_migrations') IS NOT NULL
                    """).fetchone()
                if metadata == (False, False):
                    status = DatabaseStatus(name, False, None, None, (), ())
                elif metadata == (True, True):
                    stage = "identity"
                    identity_row = connection.execute("""
                        SELECT database_id, purpose
                        FROM forge_meta.database_identity
                        WHERE singleton = TRUE
                        """).fetchone()
                    identity = verify_identity(identity_row, testing=testing)
                    purpose = "testing" if testing else "production"
                    status = DatabaseStatus(name, True, str(identity), purpose, (), ())
                else:
                    raise MigrationError("Forge migration metadata is incomplete.")

                stage = "schemas"
                required = ("forge_meta", "catalog", "historical", "ranking")
                rows = connection.execute(
                    "SELECT nspname FROM pg_namespace WHERE nspname = ANY(%s)",
                    (list(required),),
                ).fetchall()
                present = {row[0] for row in rows}
                status = replace(
                    status,
                    missing_schemas=tuple(schema for schema in required if schema not in present),
                )

                stage = "history"
                try:
                    migrations = load_migrations()
                except (OSError, UnicodeError):
                    raise MigrationError("Could not read local migration files.") from None
                applied: tuple[AppliedMigration, ...] = ()
                if status.initialized:
                    with connection.cursor(row_factory=class_row(AppliedMigration)) as cursor:
                        cursor.execute("""
                            SELECT version, name, checksum
                            FROM forge_meta.schema_migrations
                            WHERE stream = 'core'
                            ORDER BY version
                            """)
                        applied = tuple(cursor.fetchall())
                    validate_history(migrations, applied)
                return replace(
                    status,
                    applied=tuple(item.version for item in applied),
                    pending=tuple(item.version for item in migrations[len(applied) :]),
                )

    except (MigrationError, DatabaseError) as error:
        raise DatabaseInspectionError(
            str(error), stage, connected=connected, status=status
        ) from None
    except psycopg.Error as error:
        if isinstance(error, (psycopg.errors.ConnectionTimeout, psycopg.errors.QueryCanceled)):
            message = "PostgreSQL inspection timed out."
        elif not connected:
            message = "Could not connect to the selected PostgreSQL database."
        else:
            message = "PostgreSQL could not complete this inspection step."
        raise DatabaseInspectionError(message, stage, connected=connected, status=status) from None


def inspect_testing_database(*, project_root: Path | None = None) -> DatabaseStatus:
    return inspect_database(project_root=project_root, testing=True)


def require_initialized_production(project_root: Path) -> None:
    """Production initialization uses an explicitly provisioned current registry."""
    result = inspect_database(project_root=project_root, testing=False)
    if not result.initialized or result.pending or result.missing_schemas:
        raise MigrationError(
            "Review mf database status --production and apply approved migrations "
            "with mf database migrate --production before initialization."
        )
