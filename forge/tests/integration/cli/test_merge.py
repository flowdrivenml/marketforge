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


def nullcontext_for_run(*_: object) -> object:
    from contextlib import nullcontext

    return nullcontext()


@pytest.fixture(autouse=True)
def isolate_execution_registry(monkeypatch: pytest.MonkeyPatch) -> None:
    from forge.cli.commands import merge

    # These tests exercise command/Engine dispatch with mocked domain services.
    monkeypatch.setattr(merge, "cli_execution", nullcontext_for_run)


@pytest.fixture
def workspace(tmp_path: Path) -> Path:
    (tmp_path / "forge").mkdir()
    (tmp_path / "forge/pyproject.toml").touch()
    (tmp_path / "engine").mkdir()
    folder = tmp_path / "data/.config/workflows/data_engineering/normalization"
    folder.mkdir(parents=True)
    (folder / "engine_test.toml").write_text(SOURCE)
    runtime = tmp_path / "data/.config/engine/merge.toml"
    runtime.parent.mkdir()
    runtime.write_text("schema_version = 1\n")
    (folder / "invalid.toml").write_text(SOURCE.replace('data_type = "trades"', 'data_type = "x"'))
    return tmp_path


def test_new_and_validate_use_managed_paths_and_guard_registry(
    workspace: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    args = ["--project-root", str(workspace), "--json", "merge"]
    monkeypatch.setattr(service, "selected_registry_id", AsyncMock(return_value="verified-fixture"))
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
    for name in ("missing", "invalid"):
        assert main([*args, "new", "first_merge", "--from-normalize", name, "--testing"]) == 2
    assert discover.await_count == 0
    capsys.readouterr()

    assert main([*args, "new", "first_merge", "--from-normalize", "engine_test", "--testing"]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["source"]["path"].endswith("/normalization/engine_test.toml")
    destination = workspace / "data/.config/workflows/data_engineering/merge/first_merge.toml"
    before = destination.read_bytes()
    assert main([*args, "validate", "first_merge"]) == 0
    assert main([*args, "new", "first_merge", "--from-normalize", "engine_test", "--testing"]) == 2
    assert discover.await_count == 1
    assert destination.read_bytes() == before

    source = workspace / "data/.config/workflows/data_engineering/normalization/engine_test.toml"
    assert main([*args, "new", "by_path", "--from-normalize", str(source), "--testing"]) == 0
    assert discover.await_args is not None
    assert discover.await_args.args[2] == source

    destination.write_bytes(before + b"\ndataset_ids = [5]\n")
    assert main([*args, "validate", "first_merge"]) == 2
    assert discover.await_count == 2


@pytest.mark.parametrize("requires_causal", [False, True])
def test_prepare_freezes_registry_inputs_and_routes_existing_references(
    workspace: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    requires_causal: bool,
) -> None:
    args = ["--project-root", str(workspace), "--json", "merge"]
    monkeypatch.setattr(service, "selected_registry_id", AsyncMock(return_value="verified-fixture"))
    library = workspace / "data/.config/workflows/data_engineering"
    binary = workspace / "binary"
    binary.write_bytes(b"fixture")
    directory = library / "merge"
    directory.mkdir(parents=True)
    (directory / "derived.toml").write_text(
        SOURCE.split("[[requests]]")[0] + '[selection]\nsource = "engine_test"\n'
    )
    files: list[dict[str, Any]] = [
        {
            "database_id": i,
            "stream_id": f"stream-{i}",
            "content_type": "depth" if requires_causal and i == 3 else "trades",
            "requires_causal_timeline": requires_causal and i == 3,
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
    monkeypatch.setattr(service, "effective_runtime", Mock(return_value={}))
    monkeypatch.setattr(service.WorkIndex, "peek", AsyncMock(return_value={}))
    monkeypatch.setattr(service.WorkIndex, "acquire", AsyncMock(return_value={}))
    monkeypatch.setattr(service.WorkIndex, "release", AsyncMock())

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
    assert set(job) == {
        "job_id",
        "operation",
        "inputs",
        "time_range",
        "output",
        "timeline_ordering",
    }
    assert job["timeline_ordering"] == ("causal" if requires_causal else "chronological")
    assert result["timeline_ordering"] == job["timeline_ordering"]
    assert job["operation"] == "merge"
    output = Path(job["output"]["dataset_path"])
    assert output == workspace / "data/datasets/merged" / str(job["job_id"])
    assert not output.exists()
    assert job["inputs"] == [
        {
            "dataset_id": item["database_id"],
            "stream_id": item["stream_id"],
            "stream_rank": item["stream_rank"],
            "content_type": item["content_type"],
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
    assert result["streams"] == [
        {
            "stream_id": f"stream-{index + 1}",
            "stream_rank": index,
            "content_type": files[index]["content_type"],
            "file_count": 1,
        }
        for index in range(3)
    ]
    assert result["selected_file_count"] == 3
    assert result["publication_count"] == 2

    (directory / "manual.toml").write_text(
        "dataset_ids = [2, 3, 1]\nstart_ns = 1793059200000000001\nend_ns = 1793318400000000000\n"
        'timeline_ordering = "causal"\n'
    )
    assert main([*args, "prepare", "manual", "--testing"]) == 0
    manual = json.loads(capsys.readouterr().out)
    assert select_inputs.await_args is not None
    assert select_inputs.await_args.kwargs["ids"] == [2, 3, 1]
    manual_job = json.loads(Path(manual["job_path"]).read_bytes())
    assert [item["dataset_id"] for item in manual_job["inputs"]] == [2, 3, 1]
    assert manual_job["timeline_ordering"] == "causal"
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
    monkeypatch.setattr(service, "selected_registry_id", AsyncMock(return_value="verified-fixture"))
    binary = workspace / "binary"
    binary.write_bytes(b"fixture")
    item = {
        "database_id": 19,
        "stream_id": "one",
        "content_type": "trades",
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
    monkeypatch.setattr(service, "effective_runtime", Mock(return_value={}))
    monkeypatch.setattr(service.WorkIndex, "peek", AsyncMock(return_value={}))
    monkeypatch.setattr(service.WorkIndex, "acquire", AsyncMock(return_value={}))
    monkeypatch.setattr(service.WorkIndex, "release", AsyncMock())

    validate = Mock(
        return_value=SimpleNamespace(returncode=1, stdout="invalid type: map, expected u64")
    )
    monkeypatch.setattr(service, "supervised", validate)
    processing = Mock(side_effect=AssertionError("Processing must not start"))
    monkeypatch.setattr(service, "run_process", processing)
    paths = ProjectPaths(workspace, workspace / "data")
    recipe = service.Recipe(dataset_ids=[19], start_ns=10, end_ns=20)
    folder = paths.workflow_dir("merge")
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


def test_merge_recipe_inventory_edit_and_delete(
    workspace: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    from forge.cli.commands import merge
    from forge.startup.context import build_context

    args = ["--project-root", str(workspace), "merge"]
    assert main([*args, "new", "explicit"]) == 0
    library = workspace / "data/.config/workflows/data_engineering/merge"
    recipe = library / "explicit.toml"
    original = recipe.read_bytes()
    malformed = library / "bad.toml"
    malformed.write_text("dataset_ids = [")
    assert main([*args, "list"]) == 0
    inventory = capsys.readouterr().out
    assert "explicit" in inventory and "bad" in inventory and "Invalid" in inventory
    assert main([*args, "show", "explicit"]) == 0
    assert "Registry file IDs (precedence)" in capsys.readouterr().out

    def editor(path: Path, **_: object) -> Path:
        assert path != recipe and recipe.read_bytes() == original
        path.write_bytes(original.replace(b"[1, 2]", b"[2, 1]"))
        return path

    monkeypatch.setattr("forge.shared.config.recipe_editing.edit_file", editor)
    merge.edit_recipe(build_context(project_root=workspace), recipe)
    assert b"[2, 1]" in recipe.read_bytes()
    assert main([*args, "delete", "bad"]) == 0 and malformed.is_file()
    assert main([*args, "delete", "bad", "--apply"]) == 0 and not malformed.exists()
    assert recipe.is_file()


def test_merge_preparation_preview_leaves_no_artifacts(
    workspace: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    args = ["--project-root", str(workspace), "--json", "merge"]
    assert main([*args, "new", "explicit"]) == 0
    capsys.readouterr()
    monkeypatch.setattr(service, "selected_registry_id", AsyncMock(return_value="registry"))
    first = 1727683200000000001
    selected = [
        {
            "database_id": i,
            "engine_id": 10,
            "namespace": "fixture",
            "stream_id": "bybit.spot.BTCUSDT.trades",
            "content_type": "trades",
            "path": "/pub",
            "registered_path": f"/pub/{i}.parquet",
            "manifest_path": "/pub/manifest.json",
            "file_sha256": "a" * 64,
            "manifest_sha256": "b" * 64,
            "start_ns": first,
            "end_ns": first + 50,
            "memberships": [{"stream_id": "bybit.spot.BTCUSDT.trades"}],
        }
        for i in (1, 2)
    ]
    monkeypatch.setattr(service, "select_inputs", AsyncMock(return_value=selected))
    assert main([*args, "prepare", "explicit", "--testing", "--dry-run"]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["status"] == "planned" and result["compatibility"] == "not_validated"
    assert (
        result["selected_file_count"] == 2
        and result["publication_count"] == result["stream_count"] == 1
    )
    assert [row["database_id"] for row in result["inputs"]] == [1, 2]
    assert (
        not (workspace / "data/runs").exists() and not Path(result["planned_output_path"]).exists()
    )


def test_merge_runtime_uses_real_engine(
    workspace: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    import hashlib
    import os
    import shutil
    import subprocess

    from forge.historical.engine.runtime_config import RuntimeConfiguration
    from forge.shared.errors import MarketForgeError

    root = Path(__file__).resolve().parents[4]
    binary = Path(
        os.environ.get("MF_MERGE_TEST_ENGINE", root / "engine/target/debug/marketforge-merge")
    )
    if (
        not binary.is_file()
        or subprocess.run(
            [str(binary), "--runtime-schema"], capture_output=True, check=False
        ).returncode
    ):
        pytest.skip("Build the merge Engine with --runtime-schema or set MF_MERGE_TEST_ENGINE.")
    destination = workspace / ".bin"
    destination.mkdir()
    executable = destination / "marketforge-merge"
    if shutil.which("strip"):
        stripped = subprocess.run(
            ["strip", "-o", str(executable), str(binary)], capture_output=True, check=False
        )
        if stripped.returncode:
            shutil.copy2(binary, executable)
    else:
        shutil.copy2(binary, executable)
    executable.chmod(0o755)
    with executable.open("rb") as stream:
        digest = hashlib.file_digest(stream, "sha256").hexdigest()
    (destination / "manifest.json").write_text(
        json.dumps({"schema_version": 1, "targets": {"merge": {"sha256": digest}}})
    )
    config = workspace / "data/.config/engine/merge.toml"
    original = b"""schema_version = 1
[resources]
workers = 2 # keep this comment
memory_limit_mb = 2048
[parquet]
row_group_target_mb = 1
file_target_mb = 4
[partitioning]
partitions_per_worker = 2
"""
    args = ["--project-root", str(workspace), "--json", "merge", "config"]
    config.write_bytes(b"broken = [")
    assert main([*args, "schema"]) == 0
    schema = json.loads(capsys.readouterr().out)
    assert schema["status"] == "available" and len(schema["fields"]) == 8
    assert all(
        not row["path"].startswith(("integrity_policy", "scratch")) for row in schema["fields"]
    )

    config.write_bytes(original)
    assert main([*args, "validate"]) == 0
    assert json.loads(capsys.readouterr().out)["effective"]["resources"]["max_open_files"] == 64
    assert main([*args, "set", "resources.memory_limit_mb", "4GiB"]) == 0
    assert json.loads(capsys.readouterr().out)["changes"][0]["after"]["effective"] == 4096
    assert config.read_bytes() == original
    for field, value in (
        ("parquet.row_group_target_mb", "2GiB"),
        ("resources.memory_limit_mb", "1KiB"),
        ("ordering.primary", "event_timestamp_ns"),
    ):
        assert main([*args, "set", field, value, "--apply"]) == 1
        assert json.loads(capsys.readouterr().out)["status"] == "failed"
        assert config.read_bytes() == original

    assert main([*args, "unset", "partitioning.partitions_per_worker", "--apply"]) == 0
    assert json.loads(capsys.readouterr().out)["changes"][0]["after"]["effective"] == 4
    assert b"workers = 2 # keep this comment" in config.read_bytes()
    runtime = RuntimeConfiguration(ProjectPaths(workspace, workspace / "data"), "merge")
    snapshot = runtime.snapshot()
    change = runtime.change_field(snapshot, "resources.workers", "3")
    config.write_bytes(snapshot.data + b"# concurrent writer\n")
    with pytest.raises(MarketForgeError, match="changed"):
        runtime.apply(change)
    assert b"# concurrent writer" in config.read_bytes()

    from forge.cli.commands import normalization_runtime as runtime_cli
    from forge.shared.terminal.editor import EditorError

    before_edit = config.read_bytes()
    draft_paths: list[Path] = []

    def interactive(_streams: object) -> bool:
        return True

    monkeypatch.setattr(runtime_cli, "all", interactive, raising=False)
    monkeypatch.setattr("builtins.input", Mock(return_value="apply"))
    human_args = [item for item in args if item != "--json"]

    for replacement in (0, None, 4):

        def editor(
            path: Path, *, editor: str | None = None, replacement: int | None = replacement
        ) -> Path:
            draft_paths.append(path)
            assert path != config and config.read_bytes() == before_edit
            if replacement is None:
                raise EditorError("Fixture editor failed")
            path.write_bytes(
                path.read_bytes().replace(b"workers = 2", f"workers = {replacement}".encode())
            )
            return path

        monkeypatch.setattr(runtime_cli, "edit_file", editor)
        assert main([*human_args, "edit"]) == (0 if replacement == 4 else 1)
        result = capsys.readouterr().out
        if replacement == 4:
            assert "APPLIED" in result and not draft_paths[-1].exists()
            assert b"workers = 4 # keep this comment" in config.read_bytes()
        else:
            assert "Retained draft" in result and draft_paths[-1].exists()
            assert config.read_bytes() == before_edit
            shutil.rmtree(draft_paths[-1].parent)


@pytest.mark.parametrize("width", [80, 120, 160])
def test_merge_execution_at_terminal_widths(workspace: Path, width: int) -> None:
    from io import StringIO

    from rich.cells import cell_len

    from forge.shared.config.models import PresentationSettings
    from forge.shared.presentation.merge import MergeView
    from forge.shared.terminal.console import create_console
    from forge.shared.terminal.merge import render_merge

    capture = StringIO()
    console = create_console(PresentationSettings(color="never"), stream=capture)
    console.width = width
    output = {
        "operation": "merge",
        "execution_id": "merge-execute-1234",
        "run_id": "merge-run-9876",
        "status": "complete",
        "execution": "complete",
        "verification": "verified",
        "registration": "registered",
        "environment": "testing",
        "dataset": {
            "engine_dataset_id": "9007199254740995",
            "timeline_ordering": "causal",
            "mode": "combined",
            "events_written": 10,
            "trade_events": 4,
            "depth_events": 6,
            "reconstruction_events": 3,
            "files": [{"rows": 13, "partition_id": 7}],
            "start_timestamp_ns": 1,
            "end_timestamp_ns": 87,
            "bytes_written": 2 << 20,
            "streams": [
                {"exchange": "bybit", "symbol": "BTCUSDT", "instrument_id": 688},
                {"exchange": "okx", "symbol": "ETH-USDT", "instrument_id": 783},
            ],
        },
        "catalogue_dataset_ids": {f"/long/absolute/path/{i}.parquet": i for i in range(1, 113)},
    }
    view = MergeView.from_result(output, action="run", reference="prepared-job.json")
    render_merge(console, view, root=workspace)
    text = capture.getvalue()
    assert all(cell_len(line) <= width for line in text.splitlines())
    assert "9007199254740995" in text and "00:00:00.000000087Z" in text
    assert "Physical rows" in text and "Reconstruction events" in text
    assert "causal" in text and "ETH-USDT (783)" in text
    assert "Showing 20 of 112 entries" in text
    assert "/long/absolute/path" not in text


def test_merge_deletion_checks_actual_conversion_reference(
    workspace: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    args = ["--project-root", str(workspace), "--json", "merge"]
    assert main([*args, "new", "source"]) == 0
    original = Path(json.loads(capsys.readouterr().out)["path"])
    before = original.read_bytes()
    conversion = original.parent.parent / "conversion/consumer.toml"
    conversion.parent.mkdir()
    conversion.write_text(f"""version = 1
[source]
kind = "merge"
reference = "source"
path = "{original}"
sha256 = "{"0" * 64}"
[interval]
start_ns = 0
end_ns = 1
[conversion]
target_quote = "USD"
max_staleness_ns = 1
""")
    assert main([*args, "delete", "source"]) == 0
    assert json.loads(capsys.readouterr().out)["dependencies"]["references"] == [str(conversion)]
    assert main([*args, "delete", "source", "--apply"]) == 1
    assert original.read_bytes() == before
    assert main([*args, "delete", "source", "--apply", "--force"]) == 0
    assert not original.exists() and conversion.exists()


def test_editor_failure_restores_real_terminal(workspace: Path) -> None:
    import os
    import pty
    import select
    import shlex
    import subprocess
    import sys
    import termios

    root = Path(__file__).resolve().parents[4]
    draft = workspace / "draft.toml"
    draft.write_text("schema_version = 1\n")
    editor = shlex.join(
        [
            sys.executable,
            "-c",
            "import os, tty; tty.setraw(0); os.write(1,b'\\x1b[?1049h'); raise SystemExit(3)",
        ]
    )
    master, slave = pty.openpty()
    original = termios.tcgetattr(slave)
    try:
        child = subprocess.run(
            [
                sys.executable,
                "-c",
                "import sys; from pathlib import Path; "
                "from forge.shared.terminal.editor import edit_file; "
                "edit_file(Path(sys.argv[1]), editor=sys.argv[2])",
                str(draft),
                editor,
            ],
            cwd=workspace,
            env={**os.environ, "PYTHONPATH": str(root)},
            stdin=slave,
            stdout=slave,
            stderr=subprocess.PIPE,
            check=False,
            timeout=5,
        )
        assert child.returncode == 1 and b"exited unsuccessfully with code 3" in child.stderr
        assert termios.tcgetattr(slave) == original
        assert select.select([master], [], [], 1)[0]
        transcript = os.read(master, 4096)
        assert b"\x1b[?1049h" in transcript and b"\x1b[?1049l" in transcript
    finally:
        os.close(master)
        os.close(slave)
