"""Verify ownership across actual processes."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

from forge.shared.ownership import OwnershipConflictError, WorkspaceOwnership
from forge.shared.paths import ProjectPaths

CONFLICT_EXIT_CODE = 23

_CHILD_SCRIPT = """
import sys
from pathlib import Path

from forge.shared.ownership import (
    OwnershipConflictError,
    WorkspaceOwnership,
)
from forge.shared.paths import ProjectPaths

paths = ProjectPaths.resolve(
    root=Path(sys.argv[1]),
    data_root=Path(sys.argv[2]),
)

try:
    with WorkspaceOwnership(paths):
        pass
except OwnershipConflictError:
    raise SystemExit(23)
"""


def _make_workspace(root: Path) -> Path:
    (root / "forge").mkdir(parents=True)
    (root / "engine").mkdir()
    (root / "forge" / "pyproject.toml").write_text(
        '[project]\nname = "marketforge"\n',
        encoding="utf-8",
    )
    return root


def _attempt(
    root: Path,
    data_root: Path,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            "-c",
            _CHILD_SCRIPT,
            str(root),
            str(data_root),
        ],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )


def test_workspace_blocks_another_process_until_release(
    tmp_path: Path,
) -> None:
    root = _make_workspace(tmp_path / "workspace")
    paths = ProjectPaths.resolve(root=root)
    other_storage = tmp_path / "other-storage"

    with WorkspaceOwnership(paths):
        result = _attempt(root, other_storage)
        assert result.returncode == CONFLICT_EXIT_CODE, result.stderr

    result = _attempt(root, other_storage)
    assert result.returncode == 0, result.stderr

    assert (root / ".marketforge.lock").exists()
    assert (paths.data_root / ".marketforge.lock").exists()


def test_shared_storage_blocks_other_workspace_and_rolls_back(
    tmp_path: Path,
) -> None:
    first_root = _make_workspace(tmp_path / "first")
    second_root = _make_workspace(tmp_path / "second")
    shared_storage = tmp_path / "shared-storage"

    first_paths = ProjectPaths.resolve(
        root=first_root,
        data_root=shared_storage,
    )
    second_paths = ProjectPaths.resolve(
        root=second_root,
        data_root=shared_storage,
    )
    contender = WorkspaceOwnership(second_paths)

    with WorkspaceOwnership(first_paths):
        result = _attempt(second_root, shared_storage)
        assert result.returncode == CONFLICT_EXIT_CODE, result.stderr

        with pytest.raises(OwnershipConflictError):
            contender.acquire()

        # Failed acquisition must release the second workspace's lock.
        result = _attempt(second_root, tmp_path / "independent-storage")
        assert result.returncode == 0, result.stderr


def test_exception_releases_ownership(tmp_path: Path) -> None:
    root = _make_workspace(tmp_path / "workspace")
    paths = ProjectPaths.resolve(root=root)

    with pytest.raises(RuntimeError, match="operation failed"):
        with WorkspaceOwnership(paths):
            raise RuntimeError("operation failed")

    result = _attempt(root, paths.data_root)
    assert result.returncode == 0, result.stderr
