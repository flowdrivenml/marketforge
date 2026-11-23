"""Transactional core migrations for the explicitly selected database."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from importlib.resources import files
from pathlib import Path
from typing import Final

import psycopg
from psycopg.rows import class_row

from forge.shared.database.connection import (
    DatabaseError,
    database_dsn,
    expected_database,
    verify_identity,
)
from forge.shared.errors import MarketForgeError

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


def migrate_database(
    directory: Path | None = None,
    *,
    testing: bool,
    dsn: str | None = None,
    project_root: Path | None = None,
) -> MigrationResult:
    """Apply the pending core batch atomically to the selected database."""
    purpose = "testing" if testing else "production"
    try:
        dsn = database_dsn(project_root, testing=testing) if dsn is None else dsn
        name = expected_database(dsn, testing=testing)
    except DatabaseError as error:
        raise MigrationError(str(error)) from None

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

            if database != (name,):
                raise MigrationError("Connected database differs from the configured database.")

            with connection.transaction():
                locked = connection.execute(
                    "SELECT pg_try_advisory_xact_lock(%s)",
                    (_MIGRATION_LOCK,),
                ).fetchone()

                if locked != (True,):
                    raise MigrationError("Another Forge migration runner is active.")

                connection.execute("SET LOCAL lock_timeout = '5s'")
                metadata = connection.execute("""
                    SELECT to_regclass('forge_meta.database_identity') IS NOT NULL,
                           to_regclass('forge_meta.schema_migrations') IS NOT NULL
                    """).fetchone()
                if metadata == (False, False):
                    existing_schema = connection.execute("""
                        SELECT EXISTS (
                            SELECT FROM pg_namespace
                            WHERE nspname IN ('catalog', 'historical', 'ranking')
                        )
                        """).fetchone()
                    if existing_schema == (True,):
                        raise MigrationError(
                            "Existing Forge schemas have no migration metadata. "
                            "Review database provisioning before bootstrapping."
                        )
                    connection.execute(bootstrap.encode("utf-8"), prepare=False)
                    connection.execute(
                        "INSERT INTO forge_meta.database_identity(singleton, purpose) VALUES (TRUE, %s)",
                        (purpose,),
                    )
                elif metadata != (True, True):
                    raise MigrationError("Forge migration metadata is incomplete.")

                identity = connection.execute("""
                    SELECT database_id, purpose
                    FROM forge_meta.database_identity
                    WHERE singleton
                    """).fetchone()
                verify_identity(identity, testing=testing)

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
            database=name,
            applied=tuple(migration.version for migration in pending),
            already_applied=tuple(migration.version for migration in applied),
        )
    except DatabaseError as error:
        raise MigrationError(str(error)) from None
    except psycopg.Error:
        raise MigrationError(
            f"PostgreSQL could not complete the {purpose} migration transaction. "
            "Verify credentials, schema state, and migration privileges."
        ) from None


def migrate_testing_database(
    directory: Path | None = None,
    *,
    dsn: str | None = None,
    project_root: Path | None = None,
) -> MigrationResult:
    return migrate_database(directory, testing=True, dsn=dsn, project_root=project_root)
