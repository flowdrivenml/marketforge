"""Orchestration policy without starting a processing subprocess."""

import json
from contextlib import nullcontext
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, Mock

import pytest

from forge.historical.normalization import execution
from forge.historical.normalization.datasets import verify_inventory
from forge.shared.config.library import ConfigurationNameError, run_reference
from forge.shared.database.connection import database_environment
from forge.shared.errors import MarketForgeError
from forge.shared.paths import ProjectPaths


@pytest.mark.parametrize(
    "statuses,allow,interrupted,invalid,registration,expected,count",
    [
        (("complete", "complete"), False, False, False, "registered", "complete", 2),
        (("complete", "failed"), False, False, False, "registered", "failed", 0),
        (("complete", "failed"), True, False, False, "registered", "partial", 1),
        (("failed", "failed"), True, False, False, "registered", "failed", 0),
        (("complete", "failed"), True, False, True, "registered", "failed", 0),
        (("complete", "failed"), True, False, False, "pending", "failed", 0),
        (("complete", "complete"), True, True, False, "registered", "interrupted", 0),
    ],
)
def test_finish_requires_valid_verified_registrations(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    statuses: tuple[str, ...],
    allow: bool,
    interrupted: bool,
    invalid: bool,
    registration: str,
    expected: str,
    count: int,
) -> None:
    paths = ProjectPaths(tmp_path, tmp_path / "data")
    document: dict[str, Any] = {"run_id": "submitted", "jobs": []}
    records: list[dict[str, Any]] = []
    for job_id, status in enumerate(statuses, 10):
        output = tmp_path / str(job_id)
        document["jobs"].append(
            {"job_id": job_id, "dataset_id": job_id + 2, "output": {"dataset_path": str(output)}}
        )
        records.append(
            {
                "job_id": job_id,
                "dataset_id": job_id + 2,
                "status": status,
                "manifest_path": str(output / "manifest.json"),
                "failure": {"code": "processing_failure", "message": "isolated failure"}
                if status == "failed"
                else None,
                "files_written": 1,
                "events_written": 4,
                "start_timestamp_ns": 1727683200000000001,
                "end_timestamp_ns": 1727683200000000002,
            }
        )
    complete = all(status == "complete" for status in statuses)
    result_path = tmp_path / "engine.json"
    result_path.write_text(
        json.dumps(
            {
                "kind": "normalization_result",
                "schema_version": 1,
                "run_id": "submitted",
                "status": "complete" if complete else "failed",
                "error": None,
                "jobs": records,
            }
        )
    )
    receipt: dict[str, Any] = {
        "normalization": "interrupted" if interrupted else "complete" if complete else "failed",
        "exit_code": 0 if complete else 1,
        "engine_result_path": str(result_path),
        "allow_partial": allow,
        "execution_id": "fixture",
        "environment": "production",
        "registry_id": "registry-fixture",
        "submitted_sha256": "fixture",
        "source_sha256_by_job": {"10": {}, "11": {}},
    }
    registered: list[str] = []

    def verify(_paths: Any, _binary: Any, _document: Any, job: Any, _sha: str) -> dict[str, Any]:
        if invalid:
            raise MarketForgeError("Checksum mismatch")
        record = next(row for row in records if row["job_id"] == job["job_id"])
        return {
            "manifest": {**record, "integrity_status": "clean"},
            "path": job["output"]["dataset_path"],
            "instrument_namespace": "fixture",
            "engine_dataset_id": str(job["dataset_id"]),
            "manifest_sha256": "fixture",
            "bytes_written": 42,
        }

    async def register(_paths: Any, items: list[dict[str, Any]], **kwargs: Any) -> dict[str, Any]:
        registered.extend(item["path"] for item in items)
        mapping = {f"{path}/part.parquet": i for i, path in enumerate(registered, 1)}
        states = dict.fromkeys(mapping, "new")
        return {
            "publications": {
                path: {"files": {f"{path}/part.parquet": mapping[f"{path}/part.parquet"]}}
                for path in registered
            },
            "counts": {
                "publications": len(registered),
                "primary_files": len(mapping),
                "new": len(mapping),
                "reused": 0,
            },
            "file_states": states,
            "registration": registration,
            "catalogue_dataset_ids": mapping,
            "registration_receipt": str(tmp_path / "registration.json"),
            "error": "fixture outage",
        }

    monkeypatch.setattr(execution, "committed_job", verify)
    monkeypatch.setattr(execution, "register_verified", register)
    execution.finish(paths, document, receipt, tmp_path / "engine", tmp_path)
    assert receipt["status"] == expected
    assert receipt["counts"]["registered"] == count
    assert bool(registered) is (count > 0 or registration == "pending")
    assert ("recovery" in receipt) == (count == 0)


def test_foreground_tui_passes_result_file_and_inherits_stdio(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    process = Mock()
    process.wait.return_value = 0
    popen = Mock(return_value=process)
    monkeypatch.setattr(execution.subprocess, "Popen", popen)
    argv = ["fixture", "--result-file", str(tmp_path / "result.json")]
    assert execution.run_process(argv, tmp_path, tmp_path, tui=True) == (0, False)
    assert popen.call_args.args[0][-3:] == ["--result-file", str(tmp_path / "result.json"), "--tui"]
    assert popen.call_args.kwargs == {
        "cwd": tmp_path,
        "stdout": None,
        "stderr": None,
        "start_new_session": False,
    }


def test_prepared_file_uses_content_and_detects_ambiguity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    recipe = tmp_path / "recipes" / "name.toml"
    recipe.parent.mkdir()
    recipe.write_text("version = 1\n")
    assert run_reference("name", recipe) == (recipe, False)
    source = tmp_path / "name"
    source.write_text('{"kind": "normalization_batch"}')
    with pytest.raises(ConfigurationNameError, match="Ambiguous"):
        run_reference("name", recipe)
    assert run_reference("./name", recipe) == (source, True)


def test_inventory_rejects_same_size_mutation(tmp_path: Path) -> None:
    output = tmp_path / "part.parquet"
    output.write_bytes(b"abc")
    entries = [{"path": output.name, "size_bytes": 3}]
    hashes = verify_inventory(tmp_path, entries, (), require_commit_checksums=False)
    (tmp_path / "publication-checksums.json").write_text(json.dumps(hashes))
    assert verify_inventory(tmp_path, entries, (), require_commit_checksums=True) == hashes
    output.write_bytes(b"cba")
    with pytest.raises(MarketForgeError, match="checksums"):
        verify_inventory(tmp_path, entries, (), require_commit_checksums=True)


def test_named_run_prepares_once_and_submits_exact_artifact(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    from forge.cli.commands import normalization
    from forge.cli.main import main

    for path in ("forge", "engine", "data/.config/workflows/data_engineering/normalization"):
        (tmp_path / path).mkdir(parents=True)
    (tmp_path / "forge/pyproject.toml").touch()
    recipe = tmp_path / "data/.config/workflows/data_engineering/normalization/demo.toml"
    recipe.write_text("""
version = 1
[interval]
start = 2024-01-01T00:00:00Z
end = 2024-01-02T00:00:00Z
[[requests]]
exchange = "binance"
market_category = "spot"
instrument_type = "spot"
data_type = "trades"
scope = {kind = "instrument", identifier = "BTCUSDT"}
""")
    submission = tmp_path / "frozen-request"
    calls: list[str] = []

    async def prepare(*args: Any, **kwargs: Any) -> dict[str, str]:
        calls.append("prepare")
        submission.write_bytes(b'{"integers":[1727683200000000001]}')
        return {"status": "prepared", "batch_path": str(submission)}

    def execute(_paths: ProjectPaths, source: Path, **kwargs: Any) -> dict[str, Any]:
        calls.append("execute")
        assert source == submission
        assert source.read_bytes() == b'{"integers":[1727683200000000001]}'
        assert kwargs == {"tui": False, "allow_partial": True, "recipe": "demo"}
        return {"status": "partial", "execution_id": "fixture", "registrations": [{}]}

    monkeypatch.setattr(normalization, "cli_execution", Mock(return_value=nullcontext()))
    monkeypatch.setattr(normalization, "prepare_batch", prepare)
    monkeypatch.setattr(normalization, "execute", execute)
    assert (
        main(
            [
                "--project-root",
                str(tmp_path),
                "--json",
                "normalize",
                "run",
                "demo",
                "--testing",
                "--allow-partial",
            ]
        )
        == 0
    )
    assert calls == ["prepare", "execute"]
    assert json.loads(capsys.readouterr().out)["status"] == "partial"


@pytest.mark.asyncio
async def test_register_from_intent_never_runs_processing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from forge.historical.engine.managed import checksum

    paths = ProjectPaths(tmp_path, tmp_path / "data")
    dataset = tmp_path / "committed"
    dataset.mkdir()
    binary = tmp_path / "marketforge-normalize"
    binary.touch()
    submission = tmp_path / "submitted-batch.json"
    submission.write_text(
        json.dumps(
            {
                "jobs": [{"job_id": 7, "output": {"dataset_path": str(dataset)}}],
            }
        )
    )
    intent = tmp_path / "execution-intent.json"
    intent.write_text(
        json.dumps(
            {
                "operation": "normalize",
                "environment": "production",
                "registry_id": "registry-fixture",
                "execution_id": "power-loss",
                "source_sha256_by_job": {"7": {"fixture": "hash"}},
                "allow_partial": False,
                "submitted_sha256": checksum(submission),
                "executable_sha256": checksum(binary),
            }
        )
    )
    monkeypatch.setattr(
        execution, "run_process", Mock(side_effect=AssertionError("No processing permitted"))
    )
    monkeypatch.setattr(execution, "committed_job", Mock(return_value={"path": str(dataset)}))

    async def retry(
        _paths: ProjectPaths, outputs: list[dict[str, Any]], **kwargs: Any
    ) -> dict[str, Any]:
        assert outputs == [{"path": str(dataset), "expected_raw_sha256": {"fixture": "hash"}}]
        assert kwargs["execution"]["execution_id"] == "power-loss"
        return {"registration": "registered", "catalogue_dataset_id": 91}

    monkeypatch.setattr(execution, "register_verified", retry)
    monkeypatch.setattr(
        execution, "selected_registry_id", AsyncMock(return_value="registry-fixture")
    )
    assert (await execution.recover(paths, intent))["catalogue_dataset_id"] == 91
    with (
        database_environment(testing=True),
        pytest.raises(MarketForgeError, match="original verified database identity"),
    ):
        await execution.recover(paths, intent)
    submission.write_text("{}")
    with pytest.raises(MarketForgeError, match="fingerprint"):
        await execution.recover(paths, intent)
