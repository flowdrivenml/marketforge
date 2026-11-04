"""Database command dispatch and presentation."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from forge.cli.main import main
from forge.shared.database.migrations import MigrationError, MigrationResult


@pytest.fixture
def workspace(tmp_path: Path) -> Path:
    root = tmp_path / "workspace"
    (root / "forge").mkdir(parents=True)
    (root / "engine").mkdir()
    (root / "forge" / "pyproject.toml").write_text(
        '[project]\nname = "marketforge"\n',
        encoding="utf-8",
    )
    return root


def test_requires_explicit_testing_selection(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as exc:
        main(["database", "migrate"])

    assert exc.value.code == 2
    assert "--testing" in capsys.readouterr().err


def test_reports_migration_result_as_plain_json(
    workspace: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    calls: list[str] = []

    def fake_migrate() -> MigrationResult:
        calls.append("migrate")
        return MigrationResult(
            database="marketforge_test",
            applied=(),
            already_applied=(1,),
        )

    monkeypatch.setattr(
        "forge.cli.commands.database.migrate_testing_database",
        fake_migrate,
    )

    result = main(
        [
            "--project-root",
            str(workspace),
            "--json",
            "database",
            "migrate",
            "--testing",
        ],
    )
    captured = capsys.readouterr()

    assert result == 0, captured.err
    assert captured.err == ""
    assert calls == ["migrate"]
    assert "\x1b[" not in captured.out

    payload = json.loads(captured.out)
    assert payload["database"] == "marketforge_test"
    assert payload["applied"] == []
    assert payload["already_applied"] == [1]


def test_reports_migration_failure(
    workspace: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def fake_migrate() -> MigrationResult:
        raise MigrationError("Migration history mismatch.")

    monkeypatch.setattr(
        "forge.cli.commands.database.migrate_testing_database",
        fake_migrate,
    )

    result = main(
        [
            "--project-root",
            str(workspace),
            "--json",
            "database",
            "migrate",
            "--testing",
        ],
    )
    captured = capsys.readouterr()

    assert result == 2
    assert captured.out == ""

    payload = json.loads(captured.err)
    assert payload["error"]["message"] == "Migration history mismatch."
