"""Testing connection selection never evaluates shell or falls back to production."""

from pathlib import Path

import pytest

from forge.shared.database.testing import (
    TestingDatabaseError as DatabaseSelectionError,
)
from forge.shared.database.testing import (
    testing_dsn as load_testing_dsn,
)


def test_testing_selection_ignores_production_secret(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("TEST_DATABASE_KEY", raising=False)
    monkeypatch.setenv("PRODUCTION_DATABASE_KEY", "postgresql://production/marketforge")
    (tmp_path / "forge").mkdir()
    (tmp_path / "forge/pyproject.toml").touch()
    (tmp_path / "engine").mkdir()
    (tmp_path / ".secrets").mkdir()
    source = tmp_path / ".secrets" / "postgres.env"
    source.write_text(
        "PRODUCTION_DATABASE_KEY=postgresql://production/marketforge\n"
    )
    source.chmod(0o600)
    with pytest.raises(DatabaseSelectionError, match="TEST_DATABASE_KEY"):
        load_testing_dsn(tmp_path)


def test_environment_file_is_literal_not_executable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("TEST_DATABASE_KEY", raising=False)
    (tmp_path / "forge").mkdir()
    (tmp_path / "forge/pyproject.toml").touch()
    (tmp_path / "engine").mkdir()
    (tmp_path / ".secrets").mkdir()
    source = tmp_path / ".secrets" / "postgres.env"
    source.write_text(
        "export TEST_DATABASE_KEY='$(touch should-not-exist)'\n"
    )
    source.chmod(0o600)
    assert load_testing_dsn(tmp_path) == "$(touch should-not-exist)"
    assert not (tmp_path / "should-not-exist").exists()
