"""Deletion never follows symlinks, shared inodes, replaced files or unsafe paths."""

from pathlib import Path

import pytest

from forge.storage.managed_files import FileChanged, snapshot, unlink_reviewed


def test_remove_only_reviewed_inode(tmp_path: Path) -> None:
    root = tmp_path / "raw"
    folder = root / "attempt"
    folder.mkdir(parents=True)
    target = folder / "data.part"
    target.write_bytes(b"partial")
    expected = snapshot(root, "attempt/data.part")
    assert expected.size == 7
    target.unlink()
    target.write_bytes(b"replacement")
    with pytest.raises(FileChanged):
        unlink_reviewed(root, "attempt/data.part", expected)
    assert target.read_bytes() == b"replacement"
    fresh = snapshot(root, "attempt/data.part")
    unlink_reviewed(root, "attempt/data.part", fresh)
    assert not target.exists()


@pytest.mark.parametrize(
    "variant", ["leaf_symlink", "parent_symlink", "root_symlink", "hardlink", "traversal"]
)
def test_external_files_cannot_be_removed(tmp_path: Path, variant: str) -> None:
    root = tmp_path / "raw"
    root.mkdir()
    outside = tmp_path / "original"
    outside.write_bytes(b"original bytes")
    relative = "data.part"
    if variant == "leaf_symlink":
        (root / relative).symlink_to(outside)
    elif variant == "parent_symlink":
        (root / "parent").symlink_to(tmp_path, target_is_directory=True)
        relative = "parent/original"
    elif variant == "root_symlink":
        root.rmdir()
        root.symlink_to(tmp_path, target_is_directory=True)
        relative = "original"
    elif variant == "hardlink":
        (root / relative).hardlink_to(outside)
    else:
        relative = "../original"
    with pytest.raises(FileChanged):
        snapshot(root, relative)
    assert outside.read_bytes() == b"original bytes"


@pytest.mark.parametrize("width", [65, 140])
@pytest.mark.parametrize("eligible", [False, True])
def test_cleanup_preview_renders_full_intervals_and_hidden_review_warning(
    width: int, eligible: bool
) -> None:
    from io import StringIO

    from forge.shared.terminal.console import create_console
    from forge.shared.terminal.storage_cleanup import render_cleanup

    stream = StringIO()
    console = create_console(stream=stream)
    console.width = width
    row: dict[str, object] = {
        "local_id": 536,
        "record": {
            "scope_identifier": "BTCUSDT",
            "scope_kind": "instrument",
            "exchange": "bybit",
            "origin": "remote",
            "instrument_type": "spot",
            "market_category": "spot",
            "data_type": "trades",
            "start_at": "2026-10-01T00:00:00+00:00",
            "end_at": "2026-11-01T00:00:00+00:00",
        },
        "actual_bytes": 42,
        "reclaimable_bytes": 0,
        "state": "eligible" if eligible else "blocked",
        "blockers": ["Untracked normalization consumers"],
        "path": "/managed/raw/archive.csv",
        "action": "none",
    }
    render_cleanup(
        console,
        {
            "operation": "delete",
            "mode": "preview",
            "totals": {
                "selected": 2,
                "eligible": 2 if eligible else 0,
                "blocked": 0 if eligible else 2,
                "absent": 0,
                "reclaimable_bytes": 0,
            },
            "targets": [row, {**row, "local_id": 537}],
            "pagination": {"offset": 0, "limit": 1, "total": 2},
            "review_complete": False,
            "date_mode": "overlap",
            "byte_semantics": "Observed file sizes.",
            "dependency_check": "Completed archives remain blocked.",
            "preview_path": "/managed/preview.json",
            "apply_command": "marketforge storage delete --apply --preview /managed/preview.json",
        },
    )
    text = stream.getvalue()
    assert "local:536" in text and "local:537" not in text
    assert "2026-10-01" in text and "2026-11-01" in text
    assert "\x1b" not in text
    if eligible:
        assert "hidden" in text and "--all" in text
    else:
        assert "--apply" not in text and "After reviewing" not in text
