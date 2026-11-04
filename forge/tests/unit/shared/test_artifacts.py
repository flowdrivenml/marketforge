"""Run artifact resolution and restored identities."""

from pathlib import Path

import pytest

from forge.orchestration.artifacts import RunArtifacts
from forge.shared.identity import new_run_id, parse_run_id
from forge.shared.paths import ProjectPaths


def test_artifact_resolution_is_independent_of_working_directory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths = ProjectPaths(
        root=tmp_path / "workspace",
        data_root=tmp_path / "storage",
    )
    run_id = new_run_id()

    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    monkeypatch.chdir(elsewhere)

    artifacts = RunArtifacts.for_run(paths, run_id)

    assert artifacts.root.parent == paths.runs_dir
    assert artifacts.root.name == str(run_id)
    assert artifacts.report.parent == artifacts.root
    assert not paths.data_root.exists()


def test_restored_identity_resolves_the_same_artifacts(
    tmp_path: Path,
) -> None:
    paths = ProjectPaths(
        root=tmp_path / "workspace",
        data_root=tmp_path / "storage",
    )
    original_id = new_run_id()

    original = RunArtifacts.for_run(paths, original_id)
    restored = RunArtifacts.for_run(
        paths,
        parse_run_id(str(original_id)),
    )

    assert restored == original
    assert not restored.root.exists()
