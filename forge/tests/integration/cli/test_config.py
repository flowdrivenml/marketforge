"""Configuration commands through the application entry point."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from forge.cli.main import main


@pytest.fixture
def workspace(tmp_path: Path) -> Path:
    root = tmp_path / "marketforge"
    (root / "forge").mkdir(parents=True)
    (root / "engine").mkdir()
    (root / "forge" / "pyproject.toml").write_text(
        '[project]\nname = "marketforge"\n',
        encoding="utf-8",
    )
    return root


def test_show_displays_defaults_without_creating_storage(
    workspace: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    result = main(
        ["--project-root", str(workspace), "config", "show"],
    )
    captured = capsys.readouterr()

    assert result == 0
    assert "Built-in defaults" in captured.out
    assert str(workspace / "data") in captured.out
    assert captured.err == ""
    assert not (workspace / "data").exists()
    assert not (workspace / ".marketforge.lock").exists()


def test_explicit_configuration_produces_plain_json(
    workspace: Path,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    source = tmp_path / "selected.toml"
    source.write_text(
        "[storage]\n"
        'data_root = "archive-storage"\n'
        "\n"
        "[presentation]\n"
        'mode = "silent"\n'
        'color = "always"\n',
        encoding="utf-8",
    )

    result = main(
        [
            "--project-root",
            str(workspace),
            "--config",
            str(source),
            "--json",
            "config",
            "show",
        ],
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert result == 0
    assert captured.err == ""
    assert "\x1b[" not in captured.out
    assert payload["settings_file"] == str(source)
    assert payload["settings"]["presentation"]["mode"] == "silent"
    assert payload["settings"]["storage"]["resolved_data_root"] == str(
        workspace / "archive-storage",
    )


def test_invalid_configuration_returns_json_error(
    workspace: Path,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    source = tmp_path / "invalid.toml"
    source.write_text(
        '[presentation]\nmode = "background"\n',
        encoding="utf-8",
    )

    result = main(
        [
            "--project-root",
            str(workspace),
            "--config",
            str(source),
            "--json",
            "config",
            "show",
        ],
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.err)

    assert result == 2
    assert captured.out == ""
    assert "\x1b[" not in captured.err
    assert "presentation.mode" in payload["error"]["message"]
    assert "input_value" not in captured.err


def test_help_does_not_require_a_workspace(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("MARKETFORGE_PROJECT_ROOT", str(tmp_path))

    with pytest.raises(SystemExit) as exc:
        main(["--help"])

    assert exc.value.code == 0
    assert "config" in capsys.readouterr().out
