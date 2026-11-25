"""Pipeline CLI presentation across captured and redirected streams."""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from forge.cli.commands import pipeline
from forge.cli.main import main
from forge.historical.pipelines.config import PipelineRecipe
from forge.shared.paths import ProjectPaths

FIXTURE = Path(__file__).resolve().parents[2] / "unit/historical/fixtures/bybit_pipeline.toml"


def install(workspace: Path, *, convert: bool = False) -> Path:
    directory = ProjectPaths.resolve(root=workspace).pipeline_config_dir
    directory.mkdir(parents=True, exist_ok=True)
    source = directory / "example.toml"
    body = FIXTURE.read_text()
    if convert:
        body += '\n[convert]\ntarget_quote = "USDT"\nmax_staleness_ns = 60000000000\n'
    source.write_text(body)
    return source


@pytest.mark.parametrize("width", [49, 120])
@pytest.mark.parametrize("mode", ["auto", "never", "no_color"])
def test_plan_cli_capture(
    workspace: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    width: int,
    mode: str,
) -> None:
    source = install(workspace, convert=True)

    async def planned(_: ProjectPaths, recipe: PipelineRecipe, path: Path) -> dict[str, object]:
        return {
            "schema_version": 1,
            "kind": "historical_pipeline_plan",
            "state": "planned",
            "environment": "testing",
            "recipe": recipe.model_dump(mode="json"),
            "source": str(path),
            "stages": {stage: "Deferred until execution." for stage in recipe.stage_order()},
            "blockers": [],
        }

    monkeypatch.setattr(pipeline, "plan", planned)
    monkeypatch.setenv("COLUMNS", str(width))
    if mode == "no_color":
        monkeypatch.setenv("NO_COLOR", "1")
    args = ["--project-root", str(workspace)]
    if mode == "never":
        settings = workspace / "plain.toml"
        settings.write_text('[presentation]\ncolor="never"\n')
        args.extend(("--config", str(settings)))
    command = ["pipeline", "plan", source.name, "--testing"]
    assert main([*args, *command]) == 0
    captured = capsys.readouterr()
    for expected in ("example", "PLANNED", "Conversion", "DEFERRED", "USDT"):
        assert expected in captured.out
    assert not captured.err and "\x1b" not in captured.out
    assert main([*args, "--json", *command]) == 0
    machine = capsys.readouterr().out
    assert "\x1b" not in machine
    assert json.loads(machine)["recipe"]["convert"]["max_staleness_ns"] == 60000000000


def test_pipeline_library_and_validation(
    workspace: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    args = ["--project-root", str(workspace), "pipeline"]
    assert main([*args, "list"]) == 0
    assert "No managed pipeline recipes" in capsys.readouterr().out
    assert main([*args, "new", "draft.toml"]) == 0
    assert "Created recipe draft" in capsys.readouterr().out
    source = install(workspace, convert=True)
    source.with_name("broken.toml").write_text("[malformed")
    assert main([*args, "list"]) == 0
    output = capsys.readouterr().out
    assert all(token in output for token in ("broken", "INVALID", "example", "USDT"))
    assert main([*args, "validate", source.name]) == 0
    output = capsys.readouterr().out
    assert all(token in output for token in ("VALID", "Local recipe structure", "Conversion"))
    assert main([*args, "validate", "missing"]) == 2
    assert main([*args, "status", "not-a-run"]) == 2


def test_blocked_plan_with_real_stdout_and_stderr_pipes(workspace: Path) -> None:
    source = install(workspace, convert=True)
    env = {**os.environ, "NO_COLOR": "1", "TEST_DATABASE_KEY": "dbname=invalid_test"}
    command = [
        sys.executable,
        "-m",
        "forge.cli.main",
        "--project-root",
        str(workspace),
        "pipeline",
        "plan",
        source.name,
        "--testing",
    ]
    process = subprocess.run(command, env=env, capture_output=True, text=True, check=False)
    assert process.returncode == 2, process.stderr
    assert "BLOCKED" in process.stdout and "Diagnostics" in process.stdout
    assert not process.stderr and "\x1b" not in process.stdout


@pytest.mark.parametrize(
    ("settings", "code"),
    [
        ('target_quote="USD"\nmax_staleness_ns=1\n', 0),
        ('target_quote="USD"\nmax_staleness_ns=1\nreference_stream_ids=["trade"]\n', 0),
        (
            'target_quote="USD"\nmax_staleness_ns=1\nreference_publication_id=42\nreference_stream_ids=["trade"]\n',
            0,
        ),
        ('target_quote="USD"\nmax_staleness_ns=1\nreference_publication_id=42\n', 2),
        ('target_quote="usdt"\nmax_staleness_ns=0\n', 2),
    ],
)
def test_conversion_recipe_validation(workspace: Path, settings: str, code: int) -> None:
    source = install(workspace)
    source.write_text(source.read_text() + "\n[convert]\n" + settings)
    assert main(["--project-root", str(workspace), "pipeline", "validate", source.name]) == code


def test_documented_pipeline_recipes() -> None:
    import re

    from forge.historical.pipelines.config import load_recipe

    root = Path(__file__).resolve().parents[4]
    text = (root / "docs/content/reference/configuration/pipeline.md").read_text()
    examples = re.findall(r"\x60{3}toml\n(.*?)\x60{3}", text, re.DOTALL)
    recipes = [
        load_recipe(example.encode()) for example in examples if example.startswith("version")
    ]
    assert [len(recipe.stage_order()) for recipe in recipes] == [3, 4]
    options = recipes[1].convert
    assert options is not None
    assert options.selection_policy(recipes[1].version).mode == "automatic"
    assert "reference_stream_ids" not in options.model_fields_set
    assert options.resources.workers == 1


@pytest.mark.parametrize(("state", "exit_code"), [("complete", 0), ("failed", 1)])
def test_run_summary_keeps_processing_and_indexing_separate(
    workspace: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    state: str,
    exit_code: int,
) -> None:
    from forge.historical.pipelines.service import PipelineRun
    from forge.startup.context import ApplicationContext

    source = install(workspace, convert=True)
    reference = "b" * 64

    async def run_result(
        run: PipelineRun, _: ApplicationContext, *, tui: bool, stop: bool
    ) -> dict[str, object]:
        assert not tui and not stop and run.windows is None
        return {
            **run.report,
            "state": state,
            "execution_registry_warning": "Outcome retained. Reconcile the execution index.",
            "stages": {
                "convert": {
                    "state": state,
                    "result": {
                        "jobs": [
                            {
                                "status": state,
                                "indexing": "complete" if state == "complete" else "failed",
                                "work_key": reference,
                                "publication": {"manifest": "/fixture/manifest.json"},
                            }
                        ]
                    },
                }
            },
        }

    monkeypatch.setattr(pipeline, "run_pipeline", run_result)
    assert (
        main(
            [
                "--project-root",
                str(workspace),
                "pipeline",
                "run",
                source.name,
                "--testing",
                "--stage-terminal",
                "inline",
            ]
        )
        == exit_code
    )
    output = capsys.readouterr()
    assert state.upper() in output.out
    assert "reconciliation required" in output.out
    assert "mf runs reconcile --run-id" in output.out
    assert (reference in output.out) is (state == "complete")
    assert not output.err
