"""Restore destinations and subprocess arguments never inherit production selection."""

from pathlib import Path

import pytest

from forge.shared.database.backup import tool_environment, validate_destination
from forge.shared.errors import MarketForgeError


@pytest.mark.parametrize(
    "name",
    [
        "marketforge_test",
        "marketforge",
        "postgres",
        "marketforge_restore_existing;DROP",
        "marketforge_restore_",
    ],
)
def test_restore_refuses_non_disposable_destination(name: str) -> None:
    with pytest.raises(MarketForgeError):
        validate_destination(name)


def test_tool_environment_is_testing_only_and_secret_not_an_argument(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PGDATABASE", "production")
    environment = tool_environment(
        "dbname=marketforge_test user=test password=private host=127.0.0.1", "marketforge_test"
    )
    assert environment["PGDATABASE"] == "marketforge_test"
    assert environment["PGPASSWORD"] == "private"
    with pytest.raises(MarketForgeError):
        tool_environment("dbname=marketforge password=private", "marketforge")


def test_restore_checks_destination_before_reading_backup(tmp_path: Path) -> None:
    from forge.shared.database.backup import restore

    with pytest.raises(MarketForgeError, match="fresh disposable"):
        restore("dbname=marketforge_test", tmp_path, "marketforge_test")
