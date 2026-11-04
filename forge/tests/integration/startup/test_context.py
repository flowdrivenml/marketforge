"""Startup integration across configuration, paths, and presentation."""

from pathlib import Path

import pytest
from pydantic import ValidationError

from forge.shared.config.loader import ConfigurationLoadError
from forge.startup.context import build_context


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


def _write_settings(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def test_missing_default_uses_defaults_without_creating_storage(
    workspace: Path,
) -> None:
    context = build_context(project_root=workspace)

    assert context.settings_file is None
    assert context.settings.presentation.mode == "progress"
    assert context.paths.data_root == workspace / "data"
    assert not context.paths.data_root.exists()


def test_loads_default_settings_from_outside_workspace(
    workspace: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = workspace / "data" / ".config" / "marketforge.toml"
    _write_settings(
        source,
        '[storage]\ndata_root = "archives"\n\n[presentation]\nmode = "silent"\n',
    )
    monkeypatch.chdir(tmp_path)

    context = build_context(project_root=workspace)

    assert context.settings_file == source
    assert context.settings.presentation.mode == "silent"
    assert context.paths.data_root == workspace / "archives"
    assert context.paths.config_dir == workspace / "data" / ".config"
    assert not context.paths.data_root.exists()


def test_explicit_configuration_overrides_default_selection(
    workspace: Path,
    tmp_path: Path,
) -> None:
    default = workspace / "data" / ".config" / "marketforge.toml"
    explicit = tmp_path / "selected.toml"

    _write_settings(default, '[presentation]\nmode = "progress"\n')
    _write_settings(explicit, '[presentation]\nmode = "silent"\n')

    context = build_context(
        project_root=workspace,
        config_file=explicit,
    )

    assert context.settings_file == explicit
    assert context.settings.presentation.mode == "silent"


def test_missing_explicit_configuration_fails(
    workspace: Path,
    tmp_path: Path,
) -> None:
    with pytest.raises(ConfigurationLoadError, match="Cannot read"):
        build_context(
            project_root=workspace,
            config_file=tmp_path / "missing.toml",
        )


def test_invalid_default_does_not_fall_back_to_defaults(
    workspace: Path,
) -> None:
    source = workspace / "data" / ".config" / "marketforge.toml"
    _write_settings(source, '[presentation]\nmode = "background"\n')

    with pytest.raises(ValidationError):
        build_context(project_root=workspace)


def test_consoles_use_separate_output_streams(
    workspace: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    context = build_context(project_root=workspace)

    context.console.print("Result")
    context.error_console.print("Diagnostic", style="mf.failure")

    captured = capsys.readouterr()

    assert captured.out == "Result\n"
    assert captured.err == "Diagnostic\n"
