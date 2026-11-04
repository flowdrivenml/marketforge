"""Read-only inspection of the testing database migration state."""

import os
from dataclasses import dataclass

import psycopg
from psycopg.rows import class_row

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


def inspect_testing_database() -> DatabaseStatus:
    """Inspect the explicitly configured testing database without modifying it."""
    dsn = os.environ.get("MARKETFORGE_DATABASE_URL_TEST")

    if not dsn or not dsn.strip():
        raise MigrationError("Set MARKETFORGE_DATABASE_URL_TEST explicitly.")

    migrations = load_migrations()

    try:
        with psycopg.connect(
            dsn,
            autocommit=True,
            connect_timeout=10,
        ) as connection:
            with connection.transaction():
                connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")

                database_row = connection.execute("SELECT current_database()").fetchone()

                if database_row != ("marketforge_test",):
                    raise MigrationError(
                        "Testing operations require the marketforge_test database."
                    )

                metadata_row = connection.execute("""
                    SELECT
                        to_regclass('forge_meta.database_identity') IS NOT NULL,
                        to_regclass('forge_meta.schema_migrations') IS NOT NULL
                    """).fetchone()

                if metadata_row == (False, False):
                    return DatabaseStatus(
                        database="marketforge_test",
                        initialized=False,
                        database_id=None,
                        purpose=None,
                        applied=(),
                        pending=tuple(migration.version for migration in migrations),
                    )

                if metadata_row != (True, True):
                    raise MigrationError("Forge migration metadata is incomplete.")

                identity_row = connection.execute("""
                    SELECT database_id::text, purpose
                    FROM forge_meta.database_identity
                    WHERE singleton = TRUE
                    """).fetchone()

                if identity_row is None:
                    raise MigrationError("Forge database identity is missing.")

                database_id, purpose = identity_row

                if not isinstance(database_id, str) or purpose != "testing":
                    raise MigrationError("Database identity does not identify a testing database.")

                with connection.cursor(row_factory=class_row(AppliedMigration)) as cursor:
                    cursor.execute("""
                        SELECT version, name, checksum
                        FROM forge_meta.schema_migrations
                        WHERE stream = 'core'
                        ORDER BY version
                        """)
                    applied = tuple(cursor.fetchall())

                validate_history(migrations, applied)

                return DatabaseStatus(
                    database="marketforge_test",
                    initialized=True,
                    database_id=database_id,
                    purpose="testing",
                    applied=tuple(item.version for item in applied),
                    pending=tuple(item.version for item in migrations[len(applied) :]),
                )

    except psycopg.Error as exc:
        raise MigrationError("PostgreSQL could not inspect the migration state.") from exc
