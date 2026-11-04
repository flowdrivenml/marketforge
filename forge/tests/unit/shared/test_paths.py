"""Workspace path-resolution tests."""

from pathlib import Path

import pytest

from forge.shared.paths import (
    PROJECT_ROOT_ENV,
    PathConfigurationError,
    ProjectPaths,
    resolve_project_root,
)


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


def test_discovers_workspace_from_child_directory(
    workspace: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(PROJECT_ROOT_ENV, raising=False)
    monkeypatch.chdir(workspace / "forge")

    assert resolve_project_root() == workspace


def test_environment_root_works_outside_workspace(
    workspace: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv(PROJECT_ROOT_ENV, str(workspace))

    assert resolve_project_root() == workspace


def test_explicit_root_overrides_environment(
    workspace: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(PROJECT_ROOT_ENV, str(tmp_path))

    assert resolve_project_root(workspace) == workspace


def test_relative_data_root_is_relative_to_workspace(
    workspace: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)

    paths = ProjectPaths.resolve(
        root=workspace,
        data_root=Path("custom-data"),
    )

    assert paths.data_root == workspace / "custom-data"
    assert paths.raw_dir == workspace / "custom-data" / "raw"
    assert not paths.data_root.exists()


def test_invalid_explicit_root_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(PathConfigurationError, match="Invalid MarketForge"):
        resolve_project_root(tmp_path)
