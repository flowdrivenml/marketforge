"""Exercise managed merge CLI preparation without loading a processing workload."""

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, Mock

import pytest

from forge.cli.main import main
from forge.historical.merge import service
from forge.shared.paths import ProjectPaths

SOURCE = """version = 1

[interval]
start = 2026-10-27T00:00:00Z
end = 2026-10-30T00:00:00Z

[[requests]]
exchange = "bybit"
instrument_type = "spot"
market_category = "spot"
data_type = "trades"
scope = { kind = "instrument", identifier = "BTCUSDT" }
"""


@pytest.fixture
def workspace(tmp_path: Path) -> Path:
    (tmp_path / "forge").mkdir()
    (tmp_path / "forge/pyproject.toml").touch()
    (tmp_path / "engine").mkdir()
    folder = tmp_path / "data/.config/historical/normalization"
    folder.mkdir(parents=True)
    (folder / "engine_test.toml").write_text(SOURCE)
    (folder / "invalid.toml").write_text(SOURCE.replace('data_type = "trades"', 'data_type = "x"'))
    return tmp_path


def test_new_and_validate_use_managed_paths_and_guard_registry(
    workspace: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    args = ["--project-root", str(workspace), "--json", "merge"]
    discover = AsyncMock(
        return_value={
            "selected_dataset_ids": [],
            "candidates": [],
            "missing": [],
            "blockers": [{"reason": "No approved files."}],
            "requested_count": 1,
        }
    )
    monkeypatch.setattr(service, "discover", discover)
    assert main([*args, "new", "manual"]) == 0
    assert main([*args, "validate", "manual"]) == 0
    assert main([*args, "new", "first_merge", "--from-normalize", "engine_test"]) == 2
    for name in ("missing", "invalid"):
        assert main([*args, "new", "first_merge", "--from-normalize", name, "--testing"]) == 2
    assert discover.await_count == 0
    capsys.readouterr()

    assert main([*args, "new", "first_merge", "--from-normalize", "engine_test", "--testing"]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["source"]["path"].endswith("/normalization/engine_test.toml")
    destination = workspace / "data/.config/historical/merge/first_merge.toml"
    before = destination.read_bytes()
    assert main([*args, "validate", "first_merge"]) == 0
    assert main([*args, "new", "first_merge", "--from-normalize", "engine_test", "--testing"]) == 2
    assert discover.await_count == 1
    assert destination.read_bytes() == before

    source = workspace / "data/.config/historical/normalization/engine_test.toml"
    assert main([*args, "new", "by_path", "--from-normalize", str(source), "--testing"]) == 0
    assert discover.await_args is not None
    assert discover.await_args.args[2] == source

    destination.write_bytes(before + b"\ndataset_ids = [5]\n")
    assert main([*args, "validate", "first_merge"]) == 2
    assert discover.await_count == 2


def test_prepare_freezes_registry_inputs_and_routes_existing_references(
    workspace: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    args = ["--project-root", str(workspace), "--json", "merge"]
    historical = workspace / "data/.config/historical"
    binary = workspace / "binary"
    binary.write_bytes(b"fixture")
    (historical / "general.toml").write_text("runtime fixture")
    directory = historical / "merge"
    directory.mkdir()
    (directory / "derived.toml").write_text(
        SOURCE.split("[[requests]]")[0] + '[selection]\nsource = "engine_test"\n'
    )
    files: list[dict[str, Any]] = [
        {
            "database_id": i,
            "engine_id": 7400,
            "namespace": "ns",
            "path": "/published/7400",
            "primary_file": f"part-{i}.parquet",
            "file_sha256": str(i) * 64,
            "manifest_sha256": "b" * 64,
            "start_ns": 1793059200000000000 + i,
            "end_ns": 1793059200000000100 + i,
            "stream_id": "stream",
            "memberships": [{"stream_id": "stream"}],
            "integrity_status": "clean",
        }
        for i in (1, 2)
    ]
    chosen = [item["database_id"] for item in files]
    discovery: dict[str, Any] = {
        "selected_dataset_ids": chosen,
        "missing": [],
        "requested_count": 1,
        "blockers": [],
        "matched_count": 2,
        "selected_count": 2,
        "publication_count": 1,
        "candidates": [
            {
                **item,
                "engine_id": "7400",
                "registered_path": item["path"] + "/" + item["primary_file"],
                "memberships": [{"stream_id": "stream", "overlaps": True, "request_indexes": [1]}],
            }
            for item in files
        ],
    }
    monkeypatch.setattr(service, "discover", AsyncMock(return_value=discovery))
    monkeypatch.setattr(service, "resolve", Mock(return_value=binary))

    def inputs(_paths: ProjectPaths, ids: list[int]) -> list[dict[str, Any]]:
        return [files[i - 1] for i in ids]

    select_inputs = AsyncMock(side_effect=inputs)
    monkeypatch.setattr(service, "select_inputs", select_inputs)
    calls: list[str] = []

    def supervised(argv: list[str], **_: Any) -> Any:
        calls.append(argv[1])
        assert argv[1] in {"--runtime-settings", "--validate-job"}
        return SimpleNamespace(returncode=0, stdout="{}")

    monkeypatch.setattr(service, "supervised", supervised)
    assert main([*args, "prepare", "derived", "--testing"]) == 0
    result = json.loads(capsys.readouterr().out)
    path = Path(result["job_path"])
    job = json.loads(path.read_bytes())
    assert job["inputs"] == [7400]
    assert job["input_paths"] == {"7400": "/published/7400"}
    assert job["primary_files"]["7400"] == [
        {
            "path": item["primary_file"],
            "sha256": item["file_sha256"],
            "manifest_sha256": item["manifest_sha256"],
        }
        for item in files
    ]
    assert job["time_range"] == {"start_ns": 1793059200000000000, "end_ns": 1793318400000000000}
    assert len(result["source"]["sha256"]) == 64
    assert result["coverage"]["observed_extent_gaps"]["stream"][-1][-1] == 1793318400000000000
    assert calls == ["--runtime-settings", "--validate-job"]

    (directory / "manual.toml").write_text(
        "dataset_ids = [2, 1]\nstart_ns = 1793059200000000000\nend_ns = 1793318400000000000\n"
    )
    assert main([*args, "prepare", "manual", "--testing"]) == 0
    manual = json.loads(capsys.readouterr().out)
    assert select_inputs.await_args is not None
    assert select_inputs.await_args.kwargs["ids"] == [2, 1]
    assert (
        json.loads(Path(manual["job_path"]).read_bytes())["primary_files"]["7400"][0]["path"]
        == "part-2.parquet"
    )

    original = path.read_bytes()
    discovery["blockers"].append({"reason": "Unsupported layout"})
    execution = Mock(return_value={"status": "complete"})
    monkeypatch.setattr(service, "execute", execution)
    assert main([*args, "run", "derived", "--testing"]) == 1
    blocked = json.loads(capsys.readouterr().out)
    assert blocked["status"] == "blocked" and "job_path" not in blocked
    assert Path(blocked["report_path"]).is_file()
    execution.assert_not_called()
    assert path.read_bytes() == original
    assert main([*args, "run", str(path), "--testing"]) == 0
    execution.assert_called_once()
    assert execution.call_args.args[1] == path

    recover = AsyncMock(return_value={"status": "complete", "registration": "registered"})
    monkeypatch.setattr(service, "register", recover)
    intent = workspace / "execution-intent.json"
    assert main([*args, "register", str(intent), "--testing"]) == 0
    assert recover.await_args is not None
    assert recover.await_args.args[1] == intent
