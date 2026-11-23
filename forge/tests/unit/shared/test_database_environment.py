"""Environment selection and retained database identity guards."""

from pathlib import Path
from uuid import uuid4

import psycopg
import pytest

from forge.cli.commands import database
from forge.cli.main import main
from forge.shared.database.connection import (
    DatabaseError,
    database_environment,
    expected_database,
    require_registry,
    verify_identity,
)
from forge.shared.database.status import DatabaseStatus


@pytest.mark.parametrize(
    ("flags", "testing"), [((), False), (("--production",), False), (("--testing",), True)]
)
def test_cli_database_status_selects_environment(
    flags: tuple[str, ...],
    testing: bool,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    choices: list[bool] = []

    def inspect(*, project_root: Path | None, testing: bool) -> DatabaseStatus:
        assert project_root is not None
        choices.append(testing)
        return DatabaseStatus(
            "marketforge_test" if testing else "marketforge",
            True,
            str(uuid4()),
            "testing" if testing else "production",
            (),
            (),
        )

    monkeypatch.setattr(database, "inspect_database", inspect)
    assert main(["database", "status", *flags]) == 0
    assert f"Purpose: {'testing' if testing else 'production'}" in capsys.readouterr().out
    assert choices == [testing]


def test_conflicting_nested_flags_are_rejected() -> None:
    with pytest.raises(SystemExit, match="2"):
        main(["storage", "--production", "list", "--testing"])


@pytest.mark.parametrize("testing", [True, False])
def test_identity_and_recovery_are_bound_to_selected_environment(testing: bool) -> None:
    purpose, other = ("testing", "production") if testing else ("production", "testing")
    identity = uuid4()
    with database_environment(testing=testing):
        assert verify_identity((identity, purpose), testing=testing) == identity
        with pytest.raises(DatabaseError, match=purpose):
            verify_identity((identity, other), testing=testing)
        with pytest.raises(DatabaseError, match=purpose):
            verify_identity((None, purpose), testing=testing)
        require_registry({"environment": purpose, "registry_id": str(identity)}, str(identity))
        for evidence in (
            {"environment": other, "registry_id": str(identity)},
            {"environment": purpose, "registry_id": str(uuid4())},
            {"environment": purpose},
        ):
            with pytest.raises(DatabaseError, match="original verified database identity"):
                require_registry(evidence, str(identity))


def test_invalid_connection_info_is_redacted(monkeypatch: pytest.MonkeyPatch) -> None:
    def invalid(_: str) -> None:
        raise psycopg.ProgrammingError("sensitive driver diagnostic")

    monkeypatch.setattr("forge.shared.database.connection.conninfo_to_dict", invalid)
    with pytest.raises(DatabaseError, match="Invalid production") as failure:
        expected_database("placeholder", testing=False)
    assert "sensitive driver diagnostic" not in str(failure.value)
    assert failure.value.__suppress_context__
