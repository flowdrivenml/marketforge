"""Named configuration commands remain independent of engine/database availability."""

import json
from pathlib import Path

import pytest

from forge.cli.main import main


def nullcontext_for_run(*_: object) -> object:
    from contextlib import nullcontext

    return nullcontext()


@pytest.fixture(autouse=True)
def isolate_execution_registry(monkeypatch: pytest.MonkeyPatch) -> None:
    from forge.cli.commands import normalization

    # These tests exercise command/Engine dispatch with mocked domain services.
    monkeypatch.setattr(normalization, "cli_execution", nullcontext_for_run)


def no_editor(path: Path, *, editor: str | None = None) -> Path:
    return path


def test_template_no_overwrite_and_validation(
    workspace: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("forge.cli.commands.normalization.edit_file", no_editor)
    args = ["--project-root", str(workspace), "--json", "normalize"]
    assert main([*args, "new", "example"]) == 0
    result = json.loads(capsys.readouterr().out)
    path = Path(result["path"])
    original = path.read_bytes()
    assert b"[[requests]]" in original
    assert "workflows/data_engineering/normalization/" in str(path)
    assert main([*args, "new", "example"]) == 2
    assert path.read_bytes() == original
    capsys.readouterr()
    assert main([*args, "validate", "example"]) == 0
    capsys.readouterr()
    path.write_text("""job_id = 7
dataset_id = 8
content_type = "trades"
[[inputs]]
task_id = 1
archive_id = 2
instrument_id = 3
format_profile = "explicit-profile"
""")
    assert main([*args, "validate", "example.toml"]) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "valid"
    explicit = workspace.parent / "explicit.toml"
    explicit.write_bytes(path.read_bytes())
    monkeypatch.chdir(workspace)
    assert main([*args, "validate", "../explicit.toml"]) == 0
    assert json.loads(capsys.readouterr().out)["path"] == str(explicit)

    missing = workspace.parent / "unavailable.toml"
    assert main([*args, "validate", "../unavailable.toml"]) == 2
    error = json.loads(capsys.readouterr().err)["error"]["message"]
    assert error == f"Normalization recipe file is unavailable: {missing}"


def test_missing_recipe_is_reported_before_opening_database(workspace: Path) -> None:
    assert main(["--project-root", str(workspace), "normalize", "prepare", "example"]) == 2


def test_human_template_uses_terminal_styles(
    workspace: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("forge.cli.commands.normalization.edit_file", no_editor)
    assert main(["--project-root", str(workspace), "normalize", "new", "human"]) == 0
    captured = capsys.readouterr()
    assert captured.err == ""
    assert "Recipe structure valid" in captured.out
    assert "\x1b[" not in captured.out


def test_prepare_json_result(
    workspace: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    from forge.historical.normalization.config import PreparationRequest
    from forge.shared.config.models import HttpSettings
    from forge.shared.paths import ProjectPaths

    config = workspace / "data/.config/workflows/data_engineering/normalization/example.toml"
    config.parent.mkdir(parents=True)
    config.write_text("""job_id = 7
dataset_id = 8
content_type = "trades"
[[inputs]]
task_id = 1
archive_id = 2
instrument_id = 3
format_profile = "explicit-profile"
""")

    async def prepare(
        paths: ProjectPaths, request: PreparationRequest, *, http: HttpSettings | None = None
    ) -> dict[str, object]:
        assert paths.root == workspace
        assert request.job_id == 7
        return {"schema_version": 1, "status": "prepared", "engine_ready": False}

    monkeypatch.setattr("forge.cli.commands.normalization.prepare", prepare)
    assert (
        main(
            [
                "--project-root",
                str(workspace),
                "--json",
                "normalize",
                "prepare",
                "example",
                "--testing",
            ]
        )
        == 0
    )
    captured = capsys.readouterr()
    assert captured.err == ""
    assert json.loads(captured.out)["engine_ready"] is False
    assert "\x1b[" not in captured.out


def test_seed_edit_validate_and_prepare_shortcut(
    workspace: Path,
    request_file: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from forge.historical.archives.config import AcquisitionConfig
    from forge.shared.config.models import HttpSettings
    from forge.shared.paths import ProjectPaths

    calls: list[Path] = []
    source = (
        request_file.read_text()
        .replace('exchange = "bybit"', 'exchange = "gateio"')
        .replace(
            'data_type = "trades"',
            'data_type = "order_book_l2"\nsource_options = { feed = "snapshot" }',
        )
        .replace('kind = "instrument"', 'kind = "family"')
    )
    request_file.write_text(source)

    def editor(path: Path, *, editor: str | None = None) -> Path:
        calls.append(path)
        assert path.read_text() == source
        path.write_text(source.replace("BTCUSDT", "ETH_USDT"))
        return path

    async def batch(
        paths: ProjectPaths,
        recipe: AcquisitionConfig,
        *,
        recipe_source: Path | None = None,
        dry_run: bool = False,
        http: HttpSettings | None = None,
    ) -> dict[str, object]:
        assert recipe.requests[0].scope.identifier == "ETH_USDT"
        assert recipe.requests[0].scope.kind == "family"
        assert recipe.requests[0].source_options is not None
        assert recipe.requests[0].source_options.feed == "snapshot"
        assert dry_run
        return {"status": "partial", "prepared_job_count": 0}

    monkeypatch.setattr("forge.cli.commands.normalization.edit_file", editor)
    monkeypatch.setattr("forge.cli.commands.normalization.prepare_batch", batch)
    args = ["--project-root", str(workspace), "--json", "normalize"]
    assert (
        main([*args, "prepare", "copy", "--testing", "--from-acquisition", "request", "--dry-run"])
        == 1
    )
    assert json.loads(capsys.readouterr().out)["prepared_job_count"] == 0
    assert len(calls) == 1
    assert request_file.read_text() == source
    assert main([*args, "new", "copy", "--from-acquisition", "request"]) == 2
    assert len(calls) == 1


def test_validate_after_editor_and_editor_failure(
    workspace: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    from forge.shared.terminal.editor import EditorError

    def invalid_editor(path: Path, *, editor: str | None = None) -> Path:
        path.write_text("version = 99")
        return path

    monkeypatch.setattr("forge.cli.commands.normalization.edit_file", invalid_editor)
    args = ["--project-root", str(workspace), "--json", "normalize"]
    assert main([*args, "new", "invalid"]) == 2
    assert "interval" in capsys.readouterr().err

    def failed_editor(path: Path, *, editor: str | None = None) -> Path:
        raise EditorError("Editor exited unsuccessfully with code 1.")

    monkeypatch.setattr("forge.cli.commands.normalization.edit_file", failed_editor)
    assert main([*args, "prepare", "failed", "--testing", "--from-acquisition", "missing"]) == 2
    assert not (
        workspace / "data/.config/workflows/data_engineering/normalization/failed.toml"
    ).exists()
    capsys.readouterr()
    assert main([*args, "new", "failed"]) == 2
    assert "Editor exited unsuccessfully" in capsys.readouterr().err


def test_ranking_creation_failure_is_structured_and_publishes_nothing(
    workspace: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from forge.historical.archives.config import AcquisitionConfig
    from forge.historical.archives.templates import ACQUISITION_TEMPLATE
    from forge.historical.normalization.ranking import RankingPreparationError
    from forge.shared.config.models import HttpSettings
    from forge.shared.paths import ProjectPaths

    paths = ProjectPaths.resolve(root=workspace)
    paths.normalization_recipe_dir.mkdir(parents=True)
    paths.normalization_recipe_dir.joinpath("failure.toml").write_text(ACQUISITION_TEMPLATE)

    async def prepare(
        paths: ProjectPaths,
        recipe: AcquisitionConfig,
        *,
        recipe_source: Path | None = None,
        dry_run: bool = False,
        http: HttpSettings | None = None,
    ) -> dict[str, object]:
        raise RankingPreparationError(
            {
                "decision": "failed",
                "policy": "turnover",
                "error": "Controlled upstream collection failure",
            }
        )

    monkeypatch.setattr("forge.cli.commands.normalization.prepare_batch", prepare)
    assert (
        main(
            [
                "--project-root",
                str(workspace),
                "--json",
                "normalize",
                "prepare",
                "failure",
                "--testing",
            ]
        )
        == 1
    )
    result = json.loads(capsys.readouterr().out)
    assert result["status"] == "failed" and result["ranking_resolution"]["decision"] == "failed"
    assert "Controlled upstream collection failure" in result["error"]
    assert not paths.runs_dir.exists()


def test_local_inventory_show_and_edit(
    workspace: Path,
    request_file: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    args = ["--project-root", str(workspace)]
    assert main([*args, "normalize", "list"]) == 0
    assert "No normalization recipes found." in capsys.readouterr().out
    directory = request_file.parent.with_name("normalization")
    directory.mkdir()
    (directory / "source.toml").write_bytes(request_file.read_bytes())
    manual = directory / "manual.toml"
    manual.write_text("""job_id = 7
dataset_id = 8
content_type = "trades"
[output]
staging_path = "example/staging"
dataset_path = "example/dataset"
[[inputs]]
task_id = 1
archive_id = 2
instrument_id = 3
""")
    (directory / "bad.toml").write_text("job_id = ?")
    assert main([*args, "normalize", "list"]) == 0
    inventory = capsys.readouterr().out
    assert "Explicit inputs" in inventory and "Invalid" in inventory and "3 recipes" in inventory
    assert main([*args, "--json", "normalize", "list"]) == 0
    records = json.loads(capsys.readouterr().out)["recipes"]
    assert [row["name"] for row in records] == ["bad", "manual", "source"]
    assert [row["valid"] for row in records] == [False, True, True]
    assert main([*args, "normalize", "show", "manual"]) == 0
    assert "Registered Archive References" in (output := capsys.readouterr().out)
    assert "example/staging" in output and '"schema_version"' not in output
    assert main([*args, "--json", "normalize", "show", "source"]) == 0
    assert json.loads(capsys.readouterr().out)["configuration"]["requests"]
    assert main([*args, "normalize", "validate", "bad"]) == 2
    assert "line 1" in capsys.readouterr().err

    def edit(path: Path, *, editor: str | None = None) -> Path:
        from forge.shared.ownership import WorkspaceOwnership
        from forge.shared.paths import ProjectPaths

        with WorkspaceOwnership(ProjectPaths(workspace, workspace / "data")):
            path.write_text("edited = ?")
        return path

    monkeypatch.setattr("forge.cli.commands.normalization.edit_file", edit)
    assert main([*args, "normalize", "edit", "manual"]) == 2
    assert "Recipe structure invalid" in capsys.readouterr().err
    assert manual.read_text() == "edited = ?"
    assert main([*args, "normalize", "edit", "missing"]) == 2
    assert not (directory / "missing.toml").exists()
    assert main([*args, "--json", "normalize", "edit", "source"]) == 2


def test_execute_and_prepared_run_share_summary_and_keep_receipt_json(
    workspace: Path,
    request_file: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    recipe = request_file.parent.with_name("normalization") / "source.toml"
    recipe.parent.mkdir()
    recipe.write_bytes(request_file.read_bytes())
    batch = workspace / "batch.json"
    batch.write_text('{"kind":"normalization_batch"}')
    receipt: dict[str, object] = {
        "schema_version": 1,
        "operation": "normalize",
        "status": "partial",
        "environment": "testing",
        "logical_run_id": "normalize-logical",
        "run_id": "normalize-batch",
        "execution_id": "normalize-execute-fixture",
        "submitted_source": str(batch),
        "verification": "verified",
        "registration": "registered",
        "registrations": [],
        "counts": {"requested": 2, "successful": 1, "failed": 1},
        "jobs": [
            {
                "job_id": 1,
                "dataset_id": 9,
                "events_written": 10,
                "files_written": 1,
                "bytes_written": 50,
            },
            {"job_id": 2, "failure": {"message": "Fixture failure"}},
        ],
    }
    calls: list[tuple[Path, bool, bool, str | None]] = []

    async def prepare(*_: object, **__: object) -> dict[str, object]:
        return {"status": "prepared", "batch_path": str(batch)}

    def execute(
        _: object, source: Path, *, tui: bool, allow_partial: bool, recipe: str | None = None
    ) -> dict[str, object]:
        calls.append((source, tui, allow_partial, recipe))
        return dict(receipt)

    monkeypatch.setattr("forge.cli.commands.normalization.prepare_batch", prepare)
    monkeypatch.setattr("forge.cli.commands.normalization.execute", execute)
    args = ["--project-root", str(workspace)]
    assert (
        main([*args, "normalize", "execute", "source", "--testing", "--tui", "--allow-partial"])
        == 0
    )
    result = capsys.readouterr().out
    assert result.count("MarketForge · Normalization") == 1 and "PARTIAL" in result
    assert "normalize-logical" in result and "normalize-execute-fixture" in result
    assert "Fixture failure" in result and '"schema_version"' not in result
    assert main([*args, "normalize", "run", str(batch), "--testing", "--allow-partial"]) == 0
    assert capsys.readouterr().out == result
    assert calls == [(batch, True, True, "source"), (batch, False, True, None)]
    assert (
        main([*args, "--json", "normalize", "run", str(batch), "--testing", "--allow-partial"]) == 0
    )
    json_output = capsys.readouterr().out
    assert json.loads(json_output) == receipt and "\x1b" not in json_output


@pytest.mark.parametrize("recovering", [False, True])
def test_registration_distinguishes_publications_and_reuse(
    workspace: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    recovering: bool,
) -> None:
    receipt: dict[str, object] = {
        "registration": "registered",
        "publication": "committed",
        "environment": "testing",
        "evidence": [{"path": "fixture", "engine_dataset_id": "8192"}],
        "publications": {
            "fixture": {
                "publication_id": 43,
                "files": {"one": 123},
                "file_states": {"one": "reused"},
            }
        },
        "catalogue_dataset_ids": {"one": 123},
        "counts": {"publications": 1, "primary_files": 1, "new": 0, "reused": 1},
    }

    async def register(*_: object) -> dict[str, object]:
        return receipt

    monkeypatch.setattr(
        "forge.cli.commands.normalization.recover"
        if recovering
        else "forge.cli.commands.normalization.register",
        register,
    )
    target = "execution-intent.json" if recovering else "fixture"
    args = ["--project-root", str(workspace)]
    assert main([*args, "normalize", "register", target, "--testing"]) == 0
    rendered = capsys.readouterr().out
    assert "Normalization Registration" in rendered and "REGISTERED" in rendered
    assert "Engine Dataset ID" in rendered and "Registry file IDs" in rendered
    assert "8192" in rendered and "Reused files" in rendered
    assert ("Registration recovery" in rendered) == recovering
    assert '"registration"' not in rendered
    assert main([*args, "--json", "normalize", "register", target, "--testing"]) == 0
    assert json.loads(capsys.readouterr().out) == receipt
