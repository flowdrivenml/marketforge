"""Full workspace entry points share the visible Run evidence header."""

import asyncio
import json
import os
from pathlib import Path

import pytest
from rich.console import Console
from textual.widgets import Button

from forge.orchestration.runs import RunInspection
from forge.shared.config.models import AppSettings
from forge.shared.paths import ProjectPaths
from forge.startup.context import ApplicationContext
from forge.tui.actions import BY_KEY, Values
from forge.tui.app import ForgeApp
from forge.tui.controller import Operation
from forge.tui.inspection import Inspection
from forge.tui.operation_view import OperationView
from forge.tui.presentation import Results
from forge.tui.resources import Row
from forge.tui.run_views import RunView, project

PIPELINE = "pipeline-11111111-1111-4111-8111-111111111111"
FAILED = "normalize-execute-11111111111111111111111111111111"
SUCCESS = "acquisition-22222222-2222-4222-8222-222222222222"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("route", "size", "identity"),
    [
        ("Retained", (80, 24), FAILED),
        ("Indexed", (100, 30), SUCCESS),
        ("Owned", (120, 32), PIPELINE),
        ("Overview", (160, 48), PIPELINE),
        ("Report", (80, 24), PIPELINE),
        ("Child", (100, 30), SUCCESS),
    ],
)
async def test_errors_control_on_workspace_run_routes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    route: str,
    size: tuple[int, int],
    identity: str,
) -> None:
    paths = ProjectPaths(tmp_path, tmp_path / "data")
    context = ApplicationContext(AppSettings(), paths, None, Console(), Console(), True)
    records: dict[str, dict[str, object]] = {}
    reports: dict[str, Path] = {}
    evidence: dict[str, dict[str, object]] = {}
    for run in (FAILED, SUCCESS, PIPELINE):
        directory = paths.runs_dir / run
        directory.mkdir(parents=True)
        report = directory / ("execution-result.json" if run == FAILED else "report.json")
        payload: dict[str, object] = {
            "run_id": run,
            "status": "complete" if run == SUCCESS else "failed",
        }
        if run == PIPELINE:
            payload.update(
                kind="historical_pipeline",
                stages={
                    "acquire": {"state": "complete", "child_run_id": SUCCESS},
                    "normalize": {"state": "failed", "error": "Profile rejected"},
                },
            )
        elif run == FAILED:
            payload["error"] = "Profile rejected"
        evidence[run] = payload
        report.write_text(json.dumps(payload))
        if run == SUCCESS:
            (directory / "events.jsonl").write_text("")
        records[run] = {
            **payload,
            "operation": run.split("-")[0],
            "status": "completed" if run == SUCCESS else "failed",
            "database_purpose": "testing",
            "artifact_root": str(directory),
            "report_path": str(report),
            "attempts": tuple(),
            "children": [{"run_id": SUCCESS, "operation": "acquisition", "status": "completed"}]
            if run == PIPELINE
            else [],
            "resources": tuple(),
        }
        reports[run] = report

    async def invoke(key: str, values: Values, *_: object) -> RunView:
        name = str(values["reference"])
        assert key in {"runs show", "runs report"}
        return project(
            RunInspection(
                name,
                records[name] if key == "runs show" else evidence[name],
                records[name],
                report_files={"Execution report": reports[name]},
            ),
            context,
            key.split()[-1],
        )

    app = ForgeApp(context, invoke=invoke)
    monkeypatch.setattr(app, "check_registry", lambda: None)
    op = Operation(
        "owned-fixture", "pipeline run", state="failed", run_id=PIPELINE, environment="testing"
    )
    app.controller.history.append(op)
    indexed = Row(identity, identity, "Indexed run", "failed", records[identity], identity)
    retained = Row(identity, identity, "Retained", "failed", evidence[identity], identity)

    # Indexed inventory is the only mocked row query. Retained and Owned use their normal readers.
    load_rows = app.load_rows

    async def rows(area: str, view: str, search: str, offset: int) -> tuple[list[Row], bool]:
        if (area, view) == ("Runs", "Indexed"):
            return [indexed], False
        return await load_rows(area, view, search, offset)

    monkeypatch.setattr(app, "load_rows", rows)
    async with app.run_test(size=size) as pilot:
        if route in {"Retained", "Indexed", "Owned"}:
            await app.switch("Runs", route)
            table = app.query_one("#results", Results)
            async with asyncio.timeout(5):
                while not table.row_count:
                    await pilot.pause(0.07)
            row_id = op.id if route == "Owned" else identity
            table.move_cursor(row=table.get_row_index(row_id))
            table.focus()
            await pilot.press("enter")
        elif route == "Overview":
            app.open_overview_run(retained)
        elif route == "Report":
            await app.open_action(BY_KEY["runs report"], None, {"reference": identity}).wait()
        else:
            await app.open_indexed_run(PIPELINE).wait()
            await pilot.pause()
            parent = app.screen
            assert isinstance(parent, Inspection)
            child_tab = tuple(parent.sections).index("Child runs")
            assert await pilot.click(f"#section-{child_tab}")
            parent.query_one("#inspection-rows").focus()
            await pilot.press("enter")
        await pilot.pause(0.3)
        screen = app.screen
        assert isinstance(screen, (Inspection, OperationView))
        json_button = screen.query_one("#inspect-open-json", Button)
        errors_button = screen.query_one("#inspect-errors", Button)
        assert json_button.visible and errors_button.visible
        assert json_button.region.y == errors_button.region.y
        assert json_button.region.right == errors_button.region.x
        assert errors_button.region.right <= size[0]
        assert json_button.region.y < size[1] // 2
        if directory := os.getenv("MF_RUN_NAV_SCREENSHOTS"):
            await asyncio.to_thread(
                Path(directory, f"{route.lower()}-summary.svg").write_text, app.export_screenshot()
            )
        assert await pilot.click(errors_button)
        await pilot.pause(0.35)
        inspector = app.screen
        assert isinstance(inspector, Inspection) and inspector.section == "Errors"
        assert identity in inspector.heading
        if identity == SUCCESS:
            assert str(inspector.query_one("#inspect-errors", Button).label) == "Errors (0)"
        if directory := os.getenv("MF_RUN_NAV_SCREENSHOTS"):
            await asyncio.to_thread(
                Path(directory, f"{route.lower()}-errors.svg").write_text, app.export_screenshot()
            )
        await pilot.press("escape")
        assert app.screen is inspector
        assert inspector.section == "Summary"
        await pilot.press("f4")
        assert inspector.section == "Errors"
