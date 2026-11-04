"""Transactional core migrations for the explicitly configured testing database."""

from __future__ import annotations

import hashlib
import os
import re
from dataclasses import dataclass
from importlib.resources import files
from pathlib import Path
from typing import Final

import psycopg
from psycopg.rows import class_row

from forge.shared.errors import MarketForgeError

dsn = os.environ["MARKETFORGE_DATABASE_URL_TEST"]
_TEST_DATABASE_ENV: Final[str] = "MARKETFORGE_DATABASE_URL_TEST"
_TEST_DATABASE_NAME: Final[str] = "marketforge_test"
_MIGRATION_LOCK: Final[int] = 0x4D464F5247450001


class MigrationError(MarketForgeError):
    """Raised when migration input or database state is unsuitable."""


@dataclass(frozen=True, slots=True)
class Migration:
    version: int
    name: str
    checksum: str
    sql: str


@dataclass(frozen=True, slots=True)
class AppliedMigration:
    version: int
    name: str
    checksum: str


@dataclass(frozen=True, slots=True)
class MigrationResult:
    database: str
    applied: tuple[int, ...]
    already_applied: tuple[int, ...]


def load_migrations(directory: Path | None = None) -> tuple[Migration, ...]:
    """Read ordered SQL once so the checksum matches the executed content."""
    root = (
        files("forge.shared.database").joinpath("sql", "core") if directory is None else directory
    )

    if not root.is_dir():
        raise MigrationError("The core migration directory is missing.")

    migrations: list[Migration] = []

    for entry in root.iterdir():
        if not entry.is_file() or not entry.name.endswith(".sql"):
            continue

        match = re.fullmatch(
            r"([0-9]{4})_([a-z][a-z0-9_]*)\.sql",
            entry.name,
        )
        if match is None:
            raise MigrationError(f"Invalid migration filename: {entry.name}")

        content = entry.read_bytes()

        if not content.strip():
            raise MigrationError(f"Empty migration: {entry.name}")

        migrations.append(
            Migration(
                version=int(match.group(1)),
                name=entry.name,
                checksum=hashlib.sha256(content).hexdigest(),
                sql=content.decode("utf-8"),
            )
        )

    migrations.sort(key=lambda migration: migration.version)

    if not migrations:
        raise MigrationError("No core migrations were found.")

    versions = [migration.version for migration in migrations]

    if versions != list(range(1, len(migrations) + 1)):
        raise MigrationError("Migration versions must be unique and consecutive from 1.")

    return tuple(migrations)


def validate_history(
    migrations: tuple[Migration, ...],
    applied: tuple[AppliedMigration, ...],
) -> None:
    """Require applied history to match an unchanged prefix of local files."""
    if len(applied) > len(migrations):
        raise MigrationError("Database history contains unavailable migrations.")

    for expected, recorded in zip(
        migrations[: len(applied)],
        applied,
        strict=True,
    ):
        if (
            recorded.version != expected.version
            or recorded.name != expected.name
            or recorded.checksum != expected.checksum
        ):
            raise MigrationError(
                f"Migration history mismatch at version {recorded.version}. "
                "Restore the applied SQL file; add a new migration for changes."
            )


def migrate_testing_database(
    directory: Path | None = None,
) -> MigrationResult:
    """Apply the pending core batch atomically to marketforge_test only.

    Migration SQL must not contain transaction-control statements.
    No production-connection fallback or database creation is performed.
    """
    dsn = os.environ.get(_TEST_DATABASE_ENV)

    if dsn is None or not dsn.strip():
        raise MigrationError(f"Set {_TEST_DATABASE_ENV} explicitly.")

    migrations = load_migrations(directory)
    bootstrap = (
        files("forge.shared.database").joinpath("sql", "bootstrap.sql").read_text(encoding="utf-8")
    )

    try:
        with psycopg.connect(
            dsn,
            autocommit=True,
            connect_timeout=10,
        ) as connection:
            database = connection.execute("SELECT current_database()").fetchone()

            if database != (_TEST_DATABASE_NAME,):
                raise MigrationError("Refusing to migrate a database other than marketforge_test.")

            with connection.transaction():
                locked = connection.execute(
                    "SELECT pg_try_advisory_xact_lock(%s)",
                    (_MIGRATION_LOCK,),
                ).fetchone()

                if locked != (True,):
                    raise MigrationError("Another Forge migration runner is active.")

                connection.execute("SET LOCAL lock_timeout = '5s'")
                connection.execute(
                    bootstrap.encode("utf-8"),
                    prepare=False,
                )
                connection.execute("""
                    INSERT INTO forge_meta.database_identity(singleton, purpose)
                    VALUES (TRUE, 'testing')
                    ON CONFLICT (singleton) DO NOTHING
                    """)

                purpose = connection.execute("""
                    SELECT purpose
                    FROM forge_meta.database_identity
                    WHERE singleton
                    """).fetchone()

                if purpose != ("testing",):
                    raise MigrationError("The database is not designated for testing.")

                with connection.cursor(
                    row_factory=class_row(AppliedMigration),
                ) as cursor:
                    cursor.execute("""
                        SELECT version, name, checksum
                        FROM forge_meta.schema_migrations
                        WHERE stream = 'core'
                        ORDER BY version
                        """)
                    applied = tuple(cursor.fetchall())

                validate_history(migrations, applied)
                pending = migrations[len(applied) :]

                for migration in pending:
                    connection.execute(
                        migration.sql.encode("utf-8"),
                        prepare=False,
                    )
                    connection.execute(
                        """
                        INSERT INTO forge_meta.schema_migrations
                            (stream, version, name, checksum)
                        VALUES ('core', %s, %s, %s)
                        """,
                        (
                            migration.version,
                            migration.name,
                            migration.checksum,
                        ),
                    )

        return MigrationResult(
            database=_TEST_DATABASE_NAME,
            applied=tuple(migration.version for migration in pending),
            already_applied=tuple(migration.version for migration in applied),
        )
    except psycopg.Error as exc:
        raise MigrationError("PostgreSQL could not complete the migration transaction.") from exc
