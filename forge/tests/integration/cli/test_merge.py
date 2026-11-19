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
    directory = historical / "merge"
    directory.mkdir()
    (directory / "derived.toml").write_text(
        SOURCE.split("[[requests]]")[0] + '[selection]\nsource = "engine_test"\n'
    )
    files: list[dict[str, Any]] = [
        {
            "database_id": i,
            "engine_id": 7401 if i == 3 else 7400,
            "namespace": "ns",
            "path": "/published/7401" if i == 3 else "/published/7400",
            "primary_file": f"part-{i}.parquet",
            "file_sha256": str(i) * 64,
            "manifest_sha256": "b" * 64,
            "start_ns": 1793059200000000000 + i,
            "end_ns": 1793059200000000100 + i,
            "memberships": [{"stream_id": f"stream-{i}"}],
            "integrity_status": "clean",
        }
        for i in (1, 2, 3)
    ]
    for item in files:
        item["registered_path"] = f"{item['path']}/{item['primary_file']}"
        item["manifest_path"] = f"{item['path']}/manifest.json"
    chosen = [item["database_id"] for item in files]
    discovery: dict[str, Any] = {
        "selected_dataset_ids": chosen,
        "missing": [],
        "requested_count": 1,
        "blockers": [],
        "matched_count": 3,
        "selected_count": 3,
        "publication_count": 2,
        "candidates": [
            {
                **item,
                "engine_id": str(item["engine_id"]),
                "registered_path": item["path"] + "/" + item["primary_file"],
                "memberships": [
                    {**member, "overlaps": True, "request_indexes": [1]}
                    for member in item["memberships"]
                ],
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
        assert argv[1] == "--validate-job"
        assert Path(argv[2]).name == "merge-job.json"
        return SimpleNamespace(returncode=0, stdout="{}")

    monkeypatch.setattr(service, "supervised", supervised)
    assert main([*args, "prepare", "derived", "--testing"]) == 0
    result = json.loads(capsys.readouterr().out)
    path = Path(result["job_path"])
    job = json.loads(path.read_bytes())
    assert set(job) == {"job_id", "operation", "inputs", "time_range", "output"}
    assert job["operation"] == "merge"
    output = Path(job["output"]["dataset_path"])
    assert output == workspace / "data/datasets/merged" / str(job["job_id"])
    assert not output.exists()
    assert job["inputs"] == [
        {
            "dataset_id": item["database_id"],
            "path": item["registered_path"],
            "sha256": item["file_sha256"],
            "publication": {
                "engine_dataset_id": str(item["engine_id"]),
                "manifest_path": item["manifest_path"],
                "manifest_sha256": item["manifest_sha256"],
            },
        }
        for item in files
    ]
    assert job["time_range"] == {"start_ns": 1793059200000000000, "end_ns": 1793318400000000000}
    assert len(result["source"]["sha256"]) == 64
    assert result["coverage"]["observed_extent_gaps"]["stream-1"][-1][-1] == 1793318400000000000
    assert calls == ["--validate-job"]

    (directory / "manual.toml").write_text(
        "dataset_ids = [2, 3, 1]\nstart_ns = 1793059200000000001\nend_ns = 1793318400000000000\n"
    )
    assert main([*args, "prepare", "manual", "--testing"]) == 0
    manual = json.loads(capsys.readouterr().out)
    assert select_inputs.await_args is not None
    assert select_inputs.await_args.kwargs["ids"] == [2, 3, 1]
    manual_job = json.loads(Path(manual["job_path"]).read_bytes())
    assert [item["dataset_id"] for item in manual_job["inputs"]] == [2, 3, 1]
    assert manual_job["time_range"]["start_ns"] == 1793059200000000001

    original = path.read_bytes()
    discovery["blockers"].append({"reason": "Ambiguous registry selection"})
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


def test_validation_rejection_keeps_generated_job_and_blocks_processing(
    workspace: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    args = ["--project-root", str(workspace), "--json", "merge"]
    binary = workspace / "binary"
    binary.write_bytes(b"fixture")
    item = {
        "database_id": 19,
        "engine_id": 8100,
        "path": "/registered/normalized/8100",
        "registered_path": "/registered/normalized/8100/part-2.parquet",
        "manifest_path": "/registered/normalized/8100/manifest.json",
        "primary_file": "part-2.parquet",
        "file_sha256": "a" * 64,
        "manifest_sha256": "b" * 64,
        "start_ns": 10,
        "end_ns": 20,
        "memberships": [{"stream_id": "one"}, {"stream_id": "two"}],
    }
    monkeypatch.setattr(service, "select_inputs", AsyncMock(return_value=[item]))
    monkeypatch.setattr(service, "resolve", Mock(return_value=binary))
    validate = Mock(
        return_value=SimpleNamespace(returncode=1, stdout="invalid type: map, expected u64")
    )
    monkeypatch.setattr(service, "supervised", validate)
    processing = Mock(side_effect=AssertionError("Processing must not start"))
    monkeypatch.setattr(service, "run_process", processing)
    paths = ProjectPaths(workspace, workspace / "data")
    recipe = service.Recipe(dataset_ids=[19], start_ns=10, end_ns=20)
    folder = paths.config_dir / "historical/merge"
    folder.mkdir()
    (folder / "manual.toml").write_text("dataset_ids = [19]\nstart_ns = 10\nend_ns = 20\n")

    assert main([*args, "prepare", "manual", "--testing"]) == 0
    prepared = json.loads(capsys.readouterr().out)
    job = Path(prepared["job_path"])
    original = job.read_bytes()
    assert prepared["status"] == "generated" and prepared["compatibility"] == "not_validated"
    assert "invalid type: map, expected u64" in prepared["validation_error"]
    assert Path(prepared["report_path"]).is_file()

    assert main([*args, "run", "manual", "--testing"]) == 1
    rejected = json.loads(capsys.readouterr().out)
    assert Path(rejected["job_path"]).is_file()
    assert main([*args, "run", str(job), "--testing"]) != 0
    assert job.read_bytes() == original
    assert all(call.args[0][1] == "--validate-job" for call in validate.call_args_list)
    processing.assert_not_called()

    reports = [
        service.prepare_job(paths, recipe, "manual", 8101, workspace / name, [19], {})
        for name in ("first", "second")
    ]
    assert Path(reports[0]["job_path"]).read_bytes() == Path(reports[1]["job_path"]).read_bytes()
