"""Doctor CLI verification with a read-only PostgreSQL inspection fixture."""

from __future__ import annotations

import io
import json
import shlex
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import MagicMock
from uuid import UUID

import psycopg
import pytest
from rich.cells import cell_len

from forge.cli.main import build_parser, main
from forge.historical.engine.managed import TARGETS, checksum
from forge.shared.database import status
from forge.shared.database.credentials import PRODUCTION_DATABASE_KEY, TEST_DATABASE_KEY
from forge.shared.database.migrations import AppliedMigration, Migration
from forge.startup import diagnostics


@pytest.fixture
def ready_workspace(workspace: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    (workspace / "data").mkdir()
    (workspace / ".bin").mkdir()
    sources: dict[str, object] = {}
    for name, target in TARGETS.items():
        path = workspace / ".bin" / target.installed_filename
        path.write_text("Fixture binary. Doctor must never execute it.")
        path.chmod(0o700)
        sources[name] = {
            "sha256": checksum(path),
            "smoke": {"exit_code": 0, "output": target.smoke_expected},
        }
    (workspace / ".bin/manifest.json").write_text(
        json.dumps({"schema_version": 1, "targets": sources})
    )
    monkeypatch.setenv(PRODUCTION_DATABASE_KEY, "dbname=fixture password=must-not-appear")
    monkeypatch.setenv(TEST_DATABASE_KEY, "dbname=marketforge_test password=must-not-appear")
    monkeypatch.setattr(
        diagnostics.shutil, "which", MagicMock(return_value="/fixture/postgres-tool")
    )
    monkeypatch.setattr(
        diagnostics.os,
        "statvfs",
        MagicMock(return_value=SimpleNamespace(f_bavail=2**24, f_frsize=4096, f_flag=0)),
    )
    monkeypatch.setattr(diagnostics.os, "access", MagicMock(return_value=True))
    return workspace


def postgres_fixture(
    monkeypatch: pytest.MonkeyPatch, scenario: str = "healthy", *, testing: bool = False
) -> MagicMock:
    purpose = "testing" if testing else "production"
    database = "marketforge_test" if testing else "fixture"
    if scenario == "wrong-name":
        database = "unexpected"
    connection = MagicMock()
    connection.__enter__.return_value = connection
    connection.execute.side_effect = [
        MagicMock(),  # Read-only transaction
        MagicMock(),  # Statement timeout
        MagicMock(),  # Lock timeout
        psycopg.errors.QueryCanceled("must-not-appear")
        if scenario == "query-timeout"
        else MagicMock(fetchone=lambda: (database,)),
        MagicMock(
            fetchone=lambda: (
                (False, False)
                if scenario.startswith("empty")
                else (True, False)
                if scenario == "incomplete"
                else (True, True)
            )
        ),
        *(
            []
            if scenario.startswith("empty")
            else [
                MagicMock(
                    fetchone=lambda: (
                        UUID(int=1),
                        ("production" if testing else "testing")
                        if scenario == "identity"
                        else purpose,
                    )
                )
            ]
        ),
        MagicMock(
            fetchall=lambda: (
                []
                if scenario == "empty"
                else [
                    (schema,)
                    for schema in (
                        ("historical",)
                        if scenario == "empty-existing"
                        else ("forge_meta", "catalog", "ranking")
                        if scenario == "schema"
                        else ("forge_meta", "catalog", "historical", "ranking")
                    )
                ]
            )
        ),
    ]
    migration = Migration(1, "0001_core.sql", "fixture-checksum", "-- fixture")
    monkeypatch.setattr(status, "load_migrations", lambda: (migration,))
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchall.return_value = (
        []
        if scenario == "pending"
        else [
            AppliedMigration(
                1, migration.name, "altered" if scenario == "history" else migration.checksum
            )
        ]
    )
    if scenario in {"timeout", "offline"}:
        failure = (
            psycopg.errors.ConnectionTimeout("must-not-appear")
            if scenario == "timeout"
            else psycopg.OperationalError("must-not-appear")
        )
        connect = MagicMock(side_effect=failure)
    else:
        connect = MagicMock(return_value=connection)
    monkeypatch.setattr(status.psycopg, "connect", connect)
    return connect


def payload(
    path: Path, capsys: pytest.CaptureFixture[str], *args: str
) -> tuple[int, dict[str, Any]]:
    exit_code = main(["--project-root", str(path), "--json", "doctor", *args])
    captured = capsys.readouterr()
    assert not captured.err and "\x1b" not in captured.out
    assert "must-not-appear" not in captured.out
    report: dict[str, Any] = json.loads(captured.out)
    return exit_code, report


def statuses(report: dict[str, Any]) -> dict[str, str]:
    return {check["id"]: check["status"] for check in report["checks"]}


def assert_read_only(connect: MagicMock) -> None:
    assert connect.call_args.kwargs["connect_timeout"] == 10
    executed = [str(call.args[0]).strip() for call in connect.return_value.execute.call_args_list]
    assert executed[:3] == [
        "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY",
        "SET LOCAL statement_timeout = '5s'",
        "SET LOCAL lock_timeout = '5s'",
    ]
    assert all(sql.startswith("SELECT") for sql in executed[3:])
    cursor = connect.return_value.cursor.return_value.__enter__.return_value
    assert all(
        str(call.args[0]).strip().startswith("SELECT") for call in cursor.execute.call_args_list
    )


@pytest.mark.parametrize(
    ("selection", "testing"), [((), False), (("--production",), False), (("--testing",), True)]
)
def test_healthy_environment_and_legacy_json(
    ready_workspace: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    selection: tuple[str, ...],
    testing: bool,
) -> None:
    connect = postgres_fixture(monkeypatch, testing=testing)
    before = set(ready_workspace.rglob("*"))
    code, report = payload(ready_workspace, capsys, *selection)
    assert code == 0
    purpose = "testing" if testing else "production"
    assert report["environment"] == purpose
    assert report["database"].endswith(" / " + purpose)
    assert report["readiness"] == "ready"
    assert report["summary"]["passed"] == len(report["checks"])
    assert report["python_ready"]
    assert report["storage_writable"]
    assert report["applied_migrations"] == [1] and report["pending_migrations"] == []
    assert all(report["postgres_tools"].values())
    assert report["engine_readiness"]["targets"]
    assert before == set(ready_workspace.rglob("*"))
    assert_read_only(connect)


@pytest.mark.parametrize(
    ("scenario", "failed", "suggest_migrate"),
    [
        ("empty", "registry", True),
        ("empty-existing", "registry", False),
        ("incomplete", "registry", False),
        ("identity", "identity", False),
        ("wrong-name", "identity", False),
        ("query-timeout", "identity", False),
        ("history", "history", False),
        ("pending", "migrations", True),
        ("schema", "schemas", False),
        ("offline", "connection", False),
        ("timeout", "connection", False),
    ],
)
def test_database_states_are_isolated_and_actions_match(
    ready_workspace: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    scenario: str,
    failed: str,
    suggest_migrate: bool,
) -> None:
    connect = postgres_fixture(monkeypatch, scenario, testing=True)
    code, report = payload(ready_workspace, capsys, "--testing")
    checks = statuses(report)
    assert code == 1 and report["readiness"] == "not_ready"
    assert report["summary"]["failed"] == 1 and sum(report["summary"].values()) == len(checks)
    assert checks[f"database.{failed}"] == "failed"
    assert checks["engine.normalize"] == checks["storage.capacity"] == "passed"
    actions = [
        line
        for check in report["checks"]
        for line in (check["action"] or "").splitlines()
        if line.startswith("mf ")
    ]
    assert any(command.endswith("database status --testing") for command in actions)
    assert (
        any(command.endswith("database migrate --testing") for command in actions)
        == suggest_migrate
    )
    for command in actions:
        build_parser().parse_args(shlex.split(command)[1:])
    if scenario in {"timeout", "offline"}:
        assert connect.call_args.kwargs["connect_timeout"] == 10
        assert checks["database.identity"] == checks["database.migrations"] == "unknown"
    else:
        assert_read_only(connect)
        assert checks["database.connection"] == "passed"


def test_missing_credentials_and_invalid_configuration_are_reported(
    ready_workspace: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.delenv(PRODUCTION_DATABASE_KEY)
    config = ready_workspace / "data/.config/forge/marketforge.toml"
    config.parent.mkdir(parents=True)
    config.write_text('version = 99\nunknown = "must-not-appear"\n')
    code, report = payload(ready_workspace, capsys)
    checks = statuses(report)
    assert code == 1
    assert checks["environment.config"] == checks["database.connection"] == "failed"
    assert checks["engine.merge"] == checks["postgres_tools.pg_dump"] == "passed"
    assert checks["storage.root"] == "unknown"
    assert PRODUCTION_DATABASE_KEY in json.dumps(report)


def test_invalid_workspace_still_checks_python_and_tools(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    destination = tmp_path / "absent"
    code, report = payload(destination, capsys)
    assert code == 1
    assert statuses(report)["environment.workspace"] == "failed"
    assert "postgres_tools.pg_restore" in statuses(report)
    assert not destination.exists()


def test_missing_engines_and_recorded_smoke_failure_preserve_storage_checks(
    ready_workspace: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    postgres_fixture(monkeypatch)
    manifest = ready_workspace / ".bin/manifest.json"
    metadata = json.loads(manifest.read_text())
    metadata["targets"]["merge"]["smoke"]["exit_code"] = 1
    manifest.write_text(json.dumps(metadata))
    for target in ("normalize", "convert"):
        (ready_workspace / ".bin" / TARGETS[target].installed_filename).unlink()
    code, report = payload(ready_workspace, capsys)
    assert code == 1
    assert all(statuses(report)[f"engine.{target}"] == "failed" for target in TARGETS)
    assert statuses(report)["storage.root"] == "passed"
    assert "Recorded smoke check incompatible" in json.dumps(report)


@pytest.mark.parametrize(
    ("condition", "expected", "required_status"),
    [
        ("missing", "indeterminate", "unknown"),
        ("readonly", "not_ready", "failed"),
        ("low", "ready_with_warnings", "passed"),
        ("tools", "ready_with_warnings", "passed"),
    ],
)
def test_read_only_storage_and_optional_tools(
    ready_workspace: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    condition: str,
    expected: str,
    required_status: str,
) -> None:
    postgres_fixture(monkeypatch)
    if condition == "missing":
        (ready_workspace / "data").rmdir()
    if condition in {"low", "readonly"}:
        monkeypatch.setattr(
            diagnostics.os,
            "statvfs",
            MagicMock(
                return_value=SimpleNamespace(
                    f_bavail=1024, f_frsize=4096, f_flag=1 if condition == "readonly" else 0
                )
            ),
        )
    if condition == "tools":
        monkeypatch.setattr(diagnostics.shutil, "which", MagicMock(return_value=None))
    code, report = payload(ready_workspace, capsys)
    assert report["readiness"] == expected
    assert code == int(expected in {"indeterminate", "not_ready"})
    assert statuses(report)["storage.root"] == required_status
    if condition == "missing":
        assert report["storage_writable"] is None
        assert report["storage_parent"] == str(ready_workspace)
        assert not (ready_workspace / "data").exists()
    if condition == "low":
        assert statuses(report)["storage.capacity"] == "warning"
    if condition == "tools":
        assert not any(report["postgres_tools"].values())


def test_unsupported_python_remains_a_required_failure(
    ready_workspace: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    postgres_fixture(monkeypatch)
    monkeypatch.setattr(
        diagnostics, "sys", SimpleNamespace(version="3.11.0", version_info=(3, 11, 0))
    )
    code, report = payload(ready_workspace, capsys)
    assert code == 1 and not report["python_ready"]
    assert statuses(report)["environment.python"] == "failed"
    assert statuses(report)["database.migrations"] == "passed"


@pytest.mark.parametrize("width", [80, 120, 160])
@pytest.mark.parametrize("scenario", ["healthy", "empty", "offline"])
def test_terminal_readiness_matches_json(
    ready_workspace: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    scenario: str,
    width: int,
) -> None:
    monkeypatch.setenv("COLUMNS", str(width))
    postgres_fixture(monkeypatch, scenario)
    code, report = payload(ready_workspace, capsys)
    postgres_fixture(monkeypatch, scenario)
    assert main(["--project-root", str(ready_workspace), "doctor"]) == code
    output = capsys.readouterr().out
    assert "MarketForge · Doctor" in output and "PRODUCTION" in output
    assert "Status: " + report["readiness"].replace("_", " ").upper() in output
    assert "must-not-appear" not in output and '"checks"' not in output
    assert all(cell_len(line) <= width for line in output.splitlines())
    if scenario == "healthy":
        assert "Next steps" not in output
        assert len(output.splitlines()) <= 15
    else:
        assert "Next steps" in output and "database status --production" in " ".join(output.split())


def test_color_marks_warnings_and_failures(
    ready_workspace: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    postgres_fixture(monkeypatch, "offline")
    monkeypatch.setattr(diagnostics.shutil, "which", MagicMock(return_value=None))
    config = ready_workspace / "color.toml"
    config.write_text('[presentation]\ncolor = "always"\n')
    output = io.StringIO()
    monkeypatch.setattr("sys.stdout", output)
    assert main(["--project-root", str(ready_workspace), "--config", str(config), "doctor"]) == 1
    assert "\x1b[38;2;224;175;104m" in output.getvalue()
    assert "\x1b[38;2;247;118;142m" in output.getvalue()


def test_help_and_conflicting_database_flags_exit_before_checks(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit, match="0"):
        main(["doctor", "--help"])
    assert "MarketForge · Doctor" in capsys.readouterr().out
    with pytest.raises(SystemExit, match="2"):
        main(["doctor", "--production", "--testing"])
    assert "not allowed with argument" in capsys.readouterr().err


@pytest.mark.asyncio
async def test_tui_doctor_reuses_report_and_selected_environment(
    ready_workspace: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from forge.startup.context import build_context
    from forge.tui.services import system

    postgres_fixture(monkeypatch, "empty", testing=True)
    context = build_context(project_root=ready_workspace, testing=True)
    result = await system("doctor", {}, context, lambda _: None)
    assert isinstance(result, dict)
    assert cast(dict[str, Any], result)["environment"] == "testing"
