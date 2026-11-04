"""Migration file and history validation without a database."""

import hashlib
from pathlib import Path

import pytest

from forge.shared.database.migrations import (
    AppliedMigration,
    MigrationError,
    load_migrations,
    migrate_testing_database,
    validate_history,
)


def test_loads_ordered_sql_with_exact_byte_checksums(tmp_path: Path) -> None:
    second = b"SELECT 2;\n"
    first = b"SELECT 1;\n"
    (tmp_path / "0002_second.sql").write_bytes(second)
    (tmp_path / "0001_first.sql").write_bytes(first)

    migrations = load_migrations(tmp_path)

    assert tuple(item.version for item in migrations) == (1, 2)
    assert migrations[0].sql == first.decode("utf-8")
    assert migrations[0].checksum == hashlib.sha256(first).hexdigest()


@pytest.mark.parametrize(
    "names",
    [
        ("0002_gap.sql",),
        ("0001_first.sql", "0001_duplicate.sql"),
        ("invalid.sql",),
    ],
)
def test_rejects_ambiguous_migration_sequences(
    tmp_path: Path,
    names: tuple[str, ...],
) -> None:
    for name in names:
        (tmp_path / name).write_text("SELECT 1;", encoding="utf-8")

    with pytest.raises(MigrationError):
        load_migrations(tmp_path)


def test_rejects_changed_applied_sql(tmp_path: Path) -> None:
    source = tmp_path / "0001_first.sql"
    source.write_text("SELECT 1;", encoding="utf-8")
    original = load_migrations(tmp_path)[0]
    history = (AppliedMigration(original.version, original.name, original.checksum),)

    source.write_text("SELECT 2;", encoding="utf-8")

    with pytest.raises(MigrationError, match="history mismatch"):
        validate_history(load_migrations(tmp_path), history)


def test_accepts_an_unchanged_history_prefix(tmp_path: Path) -> None:
    (tmp_path / "0001_first.sql").write_text("SELECT 1;", encoding="utf-8")
    (tmp_path / "0002_second.sql").write_text("SELECT 2;", encoding="utf-8")
    migrations = load_migrations(tmp_path)
    first = migrations[0]

    validate_history(
        migrations,
        (AppliedMigration(first.version, first.name, first.checksum),),
    )


def test_rejects_history_missing_a_local_predecessor(tmp_path: Path) -> None:
    (tmp_path / "0001_first.sql").write_text("SELECT 1;", encoding="utf-8")
    (tmp_path / "0002_second.sql").write_text("SELECT 2;", encoding="utf-8")
    migrations = load_migrations(tmp_path)
    second = migrations[1]

    with pytest.raises(MigrationError, match="history mismatch"):
        validate_history(
            migrations,
            (AppliedMigration(second.version, second.name, second.checksum),),
        )


def test_requires_testing_connection_without_production_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("MARKETFORGE_DATABASE_URL_TEST", raising=False)
    monkeypatch.setenv(
        "MARKETFORGE_DATABASE_URL",
        "postgresql://unused/marketforge",
    )

    with pytest.raises(MigrationError, match="MARKETFORGE_DATABASE_URL_TEST"):
        migrate_testing_database()
