"""Credential resolution is literal, isolated by environment and independent of cwd."""

import os
from pathlib import Path
from unittest.mock import Mock

import pytest

from forge.shared.database.credentials import (
    PRODUCTION_DATABASE_KEY,
    TEST_DATABASE_KEY,
    DatabaseCredentialsError,
    resolve_database_dsn,
)
from forge.shared.database.migrations import MigrationError, migrate_testing_database
from forge.shared.database.status import inspect_testing_database
from forge.shared.database.testing import testing_dsn as load_testing_dsn


@pytest.fixture
def workspace(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    for key in (TEST_DATABASE_KEY, PRODUCTION_DATABASE_KEY, "MARKETFORGE_PROJECT_ROOT"):
        monkeypatch.delenv(key, raising=False)
    (tmp_path / "forge").mkdir()
    (tmp_path / "forge/pyproject.toml").touch()
    (tmp_path / "engine").mkdir()
    (tmp_path / ".secrets").mkdir()
    return tmp_path


def write_credentials(root: Path, content: str) -> None:
    source = root / ".secrets/postgres.env"
    source.write_text(content, encoding="utf-8")
    source.chmod(0o600)


@pytest.mark.parametrize(
    "testing,key", [(True, TEST_DATABASE_KEY), (False, PRODUCTION_DATABASE_KEY)]
)
def test_nonempty_environment_wins_without_reading_file(
    workspace: Path, monkeypatch: pytest.MonkeyPatch, testing: bool, key: str
) -> None:
    write_credentials(workspace, f'{key}="malformed\n')
    monkeypatch.setenv(key, "  dbname=marketforge_test  ")
    assert (
        resolve_database_dsn(testing=testing, project_root=workspace) == "dbname=marketforge_test"
    )


@pytest.mark.parametrize("environment", [None, "", " \t "])
@pytest.mark.parametrize(
    "testing,key", [(True, TEST_DATABASE_KEY), (False, PRODUCTION_DATABASE_KEY)]
)
def test_missing_or_blank_environment_falls_back_to_same_file_key(
    workspace: Path,
    monkeypatch: pytest.MonkeyPatch,
    environment: str | None,
    testing: bool,
    key: str,
) -> None:
    if environment is not None:
        monkeypatch.setenv(key, environment)
    write_credentials(workspace, f"{key}=file-value\n")
    assert resolve_database_dsn(testing=testing, project_root=workspace) == "file-value"
    assert __import__("os").environ.get(key) == environment


@pytest.mark.parametrize(
    "assignment,expected",
    [
        (
            "  export   TEST_DATABASE_KEY = 'literal value # included'  # comment",
            "literal value # included",
        ),
        (
            "TEST_DATABASE_KEY = \"host=localhost password='quoted'\" # comment",
            "host=localhost password='quoted'",
        ),
        (r'TEST_DATABASE_KEY = "back\\slash\"quote"', 'back\\slash"quote'),
        ("TEST_DATABASE_KEY = plain-value   # comment", "plain-value"),
        ("TEST_DATABASE_KEY = value#fragment", "value#fragment"),
        (
            "TEST_DATABASE_KEY='${HOME} $(touch never) `literal`'",
            "${HOME} $(touch never) `literal`",
        ),
    ],
)
def test_dotenv_syntax_is_data(workspace: Path, assignment: str, expected: str) -> None:
    write_credentials(workspace, "# ignored comment\n\n" + assignment + "\n")
    assert load_testing_dsn(workspace) == expected
    assert not (workspace / "never").exists()


def test_duplicate_assignments_are_rejected(workspace: Path) -> None:
    write_credentials(workspace, f"{TEST_DATABASE_KEY}=first\n{TEST_DATABASE_KEY}=last\n")
    with pytest.raises(DatabaseCredentialsError, match="Duplicate assignment"):
        resolve_database_dsn(testing=True, project_root=workspace)


@pytest.mark.parametrize(
    "selected,other", [(True, PRODUCTION_DATABASE_KEY), (False, TEST_DATABASE_KEY)]
)
def test_environments_never_substitute_for_each_other(
    workspace: Path, monkeypatch: pytest.MonkeyPatch, selected: bool, other: str
) -> None:
    monkeypatch.setenv(other, "other-environment")
    write_credentials(workspace, f"{other}=other-environment\n")
    with pytest.raises(DatabaseCredentialsError):
        resolve_database_dsn(testing=selected, project_root=workspace)


@pytest.mark.parametrize("content", [None, "", f"{TEST_DATABASE_KEY}=   # empty\n"])
def test_missing_credentials_are_explicit(workspace: Path, content: str | None) -> None:
    if content is not None:
        write_credentials(workspace, content)
    with pytest.raises(DatabaseCredentialsError, match=TEST_DATABASE_KEY):
        resolve_database_dsn(testing=True, project_root=workspace)


@pytest.mark.parametrize("suffix", ['"private-token-without-close', '"private-token" extra-data'])
def test_malformed_selected_value_does_not_leak(workspace: Path, suffix: str) -> None:
    write_credentials(workspace, f"{TEST_DATABASE_KEY}={suffix}\n")
    with pytest.raises(DatabaseCredentialsError) as error:
        resolve_database_dsn(testing=True, project_root=workspace)
    assert "private-token" not in str(error.value)


def test_unselected_malformed_assignment_is_ignored(workspace: Path) -> None:
    write_credentials(
        workspace, f'{PRODUCTION_DATABASE_KEY}="malformed\n{TEST_DATABASE_KEY}=testing\n'
    )
    assert load_testing_dsn(workspace) == "testing"


def test_discovers_enclosing_project_from_subdirectory(
    workspace: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    write_credentials(workspace, f"{TEST_DATABASE_KEY}=correct-root\n")
    nested = workspace / "forge/shared"
    nested.mkdir()
    (nested / ".secrets").mkdir()
    (nested / ".secrets/postgres.env").write_text(f"{TEST_DATABASE_KEY}=wrong-root\n")
    monkeypatch.chdir(nested)
    assert load_testing_dsn() == "correct-root"


def test_explicit_project_root_is_independent_of_cwd(
    workspace: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    write_credentials(workspace, f"{TEST_DATABASE_KEY}=correct-root\n")
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    monkeypatch.chdir(elsewhere)
    assert load_testing_dsn(workspace) == "correct-root"


def test_migrations_and_status_use_shared_resolution_without_database_access(
    workspace: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    write_credentials(workspace, f"{TEST_DATABASE_KEY}=dbname=marketforge_test\n")
    reached_connection = Mock(side_effect=RuntimeError("connection intercepted"))
    monkeypatch.setattr("psycopg.connect", reached_connection)
    for operation in (migrate_testing_database, inspect_testing_database):
        with pytest.raises(RuntimeError, match="intercepted"):
            operation(project_root=workspace)
        assert reached_connection.call_args.args == ("dbname=marketforge_test",)
    assert reached_connection.call_count == 2


def test_explicit_migration_dsn_is_pinned(
    workspace: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    write_credentials(workspace, f"{TEST_DATABASE_KEY}=file-value\n")
    reached_connection = Mock(side_effect=RuntimeError("connection intercepted"))
    monkeypatch.setattr("psycopg.connect", reached_connection)
    with pytest.raises(RuntimeError, match="intercepted"):
        migrate_testing_database(project_root=workspace, dsn="dbname=marketforge_test")
    assert reached_connection.call_args.args == ("dbname=marketforge_test",)
    with pytest.raises(MigrationError, match="must explicitly select"):
        migrate_testing_database(project_root=workspace, dsn="")
    assert reached_connection.call_count == 1


def test_unreadable_file_has_a_credential_safe_error(
    workspace: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(os, "open", Mock(side_effect=PermissionError("private-token")))
    with pytest.raises(DatabaseCredentialsError, match="Cannot read") as error:
        resolve_database_dsn(testing=True, project_root=workspace)
    assert "private-token" not in str(error.value)


def test_refuses_unsafe_or_oversized_file(workspace: Path) -> None:
    write_credentials(workspace, f"{TEST_DATABASE_KEY}=synthetic-test-credential")
    source = workspace / ".secrets/postgres.env"
    source.chmod(0o644)
    with pytest.raises(DatabaseCredentialsError, match="private regular UTF-8"):
        resolve_database_dsn(testing=True, project_root=workspace)
    source.chmod(0o600)
    source.write_text("x" * 65_537, encoding="utf-8")
    with pytest.raises(DatabaseCredentialsError, match="private regular UTF-8"):
        resolve_database_dsn(testing=True, project_root=workspace)
