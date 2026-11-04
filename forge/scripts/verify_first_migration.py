"""Verify batch rollback on the fresh test database, then apply core twice."""

from __future__ import annotations

import os
from pathlib import Path
from tempfile import TemporaryDirectory

import psycopg

from forge.shared.database.migrations import (
    MigrationError,
    load_migrations,
    migrate_testing_database,
)


def main() -> None:
    dsn = os.environ["MARKETFORGE_DATABASE_URL_TEST"]
    with psycopg.connect(dsn, autocommit=True) as connection:
        if connection.execute("SELECT current_database()").fetchone() != ("marketforge_test",):
            raise RuntimeError("This verification requires marketforge_test.")
        existing = connection.execute("""
            SELECT to_regnamespace('catalog') IS NOT NULL
                OR to_regnamespace('historical') IS NOT NULL
                OR to_regnamespace('forge_meta') IS NOT NULL
            """).fetchone()
        if existing != (False,):
            raise RuntimeError(
                "This first-install verification requires absent Forge schemas. "
                "It never resets an existing database."
            )

    baseline = load_migrations()
    if len(baseline) != 1 or baseline[0].version != 1:
        raise RuntimeError("This verification is for the initial version-1 baseline.")

    with TemporaryDirectory() as temporary:
        directory = Path(temporary)
        (directory / baseline[0].name).write_bytes(baseline[0].sql.encode("utf-8"))
        (directory / "0002_intentional_failure.sql").write_text(
            "SELECT 1 / 0;\n",
            encoding="utf-8",
        )
        try:
            migrate_testing_database(directory)
        except MigrationError as exc:
            if not isinstance(exc.__cause__, psycopg.errors.DivisionByZero):
                raise
        else:
            raise RuntimeError("The deliberately invalid migration unexpectedly succeeded.")

    with psycopg.connect(dsn, autocommit=True) as connection:
        remaining = connection.execute("""
            SELECT to_regnamespace('catalog') IS NOT NULL
                OR to_regnamespace('historical') IS NOT NULL
                OR to_regnamespace('forge_meta') IS NOT NULL
            """).fetchone()
        if remaining != (False,):
            raise RuntimeError("Failed migration batch left persistent schema changes.")

    print("Verified: failed batch rolled back baseline and migration metadata.")
    for number in (1, 2):
        result = migrate_testing_database()
        print(f"Run {number}: applied={result.applied}, already_applied={result.already_applied}")


if __name__ == "__main__":
    main()
