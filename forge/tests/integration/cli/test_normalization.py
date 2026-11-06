"""Named configuration commands remain independent of engine/database availability."""

import json
from pathlib import Path

import pytest

from forge.cli.main import main


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
    assert "historical/normalization/" in str(path)
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
    assert main([*args, "validate", "../escape"]) == 2
    assert "Directory paths" in capsys.readouterr().err


def test_prepare_requires_testing(workspace: Path) -> None:
    with pytest.raises(SystemExit):
        main(["--project-root", str(workspace), "normalize", "prepare", "example"])


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

    config = workspace / "data/.config/historical/normalization_jobs/example.toml"
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
    assert not (workspace / "data/.config/historical/normalization/failed.toml").exists()
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
