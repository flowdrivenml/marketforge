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
    monkeypatch.delenv("MARKETFORGE_DATABASE_URL_TEST", raising=False)
    monkeypatch.setenv("MARKETFORGE_DATABASE_URL", "postgresql://production/marketforge")
    (tmp_path / ".secrets").mkdir()
    (tmp_path / ".secrets" / "postgres.env").write_text(
        "MARKETFORGE_DATABASE_URL=postgresql://production/marketforge\n"
    )
    with pytest.raises(DatabaseSelectionError, match="MARKETFORGE_DATABASE_URL_TEST"):
        load_testing_dsn(tmp_path)


def test_environment_file_is_literal_not_executable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("MARKETFORGE_DATABASE_URL_TEST", raising=False)
    (tmp_path / ".secrets").mkdir()
    (tmp_path / ".secrets" / "postgres.env").write_text(
        "export MARKETFORGE_DATABASE_URL_TEST='$(touch should-not-exist)'\n"
    )
    assert load_testing_dsn(tmp_path) == "$(touch should-not-exist)"
    assert not (tmp_path / "should-not-exist").exists()
