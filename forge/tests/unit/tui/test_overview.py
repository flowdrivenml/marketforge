"""Bounded Overview evidence and interaction checks."""

import asyncio
import json
from pathlib import Path

from pytest import mark
from rich.console import Console
from textual.widgets import Input, Static

from forge.orchestration.inspection import run_page
from forge.shared.config.models import AppSettings
from forge.shared.paths import ProjectPaths
from forge.startup.context import ApplicationContext
from forge.tui import overview
from forge.tui.actions import BY_KEY, Values
from forge.tui.app import ForgeApp
from forge.tui.controller import Operation
from forge.tui.inspection import Inspection
from forge.tui.presentation import Results
from forge.tui.workflows import Edit, Progress

RUN = "11111111-1111-4111-8111-111111111111"
SECOND = "22222222-2222-4222-8222-222222222222"
MANAGED = "merge-execute-abcd"
RECOVERY = "merge-register-abcd"


def context(path: Path) -> ApplicationContext:
    return ApplicationContext(
        AppSettings(), ProjectPaths(path, path / "data"), None, Console(), Console()
    )


def report(root: Path, run: str, result: dict[str, object], filename: str = "report.json") -> None:
    path = root / run / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(result))


def test_requested_results_and_full_publication_scope() -> None:
    for state, label in (
        ("valid", "Validation report"),
        ("prepared", "Prepared jobs"),
        ("stopped", "Prepared jobs"),
        ("materialized", "Materialized · unpublished"),
        ("published", "Committed publication"),
    ):
        assert overview.result_label({"status": state}) == label
    assert overview.result_label({"verification": "verified", "registration": "not_available"}) == (
        "Verified output · registration unavailable"
    )
    completed: dict[str, object] = {
        "status": "complete",
        "registration": "registered",
        "publications": {"one": {"publication_id": 5}, "two": {"publication_id": 7}},
    }
    assert overview.publications(completed) == (5, 7)
    assert overview.result_label(completed) == "2 registered publications"
    assert not overview.registration_problem({"registration": "pending", "status": "prepared"})
    assert not overview.pipeline_recovery({"kind": "historical_pipeline", "state": "interrupted"})


@mark.asyncio
async def test_overview_owned_cancel_registry_failure_and_retained_selection(
    tmp_path: Path,
) -> None:
    ctx = context(tmp_path)
    ready, cancelling, cleanup = asyncio.Event(), asyncio.Event(), asyncio.Event()
    calls: list[str] = []

    async def service(
        key: str, _: Values, __: ApplicationContext, progress: Progress, ___: Edit
    ) -> object:
        calls.append(key)
        assert key == "convert run"
        progress("execute")
        ready.set()
        try:
            await asyncio.Future[None]()
        except asyncio.CancelledError:
            cancelling.set()
            await cleanup.wait()
        return {"status": "interrupted", "registration": "not_available"}

    async def status(
        key: str, _: Values, __: ApplicationContext, ___: Progress, ____: Edit
    ) -> object:
        calls.append(key)
        if key == "database status":
            raise ValueError("Fake registry failure")
        assert key == "engine status"
        return {"targets": [{"target": "convert", "status": "verified"}]}

    app = ForgeApp(ctx, invoke=service)
    app.overview_probe = status
    async with app.run_test(size=(80, 24)) as pilot:
        await pilot.pause()
        assert app.query_one(Results).row_count == 0
        assert "Idle" in str(app.query_one("#overview-current", Static).content)
        report(
            ctx.paths.runs_dir,
            RUN,
            {"kind": "historical_pipeline", "state": "running", "name": "External"},
        )
        app.action_reload()
        await pilot.pause()
        assert app.state.rows[RUN].state == "Recorded running"
        owned = app.controller.launch(BY_KEY["convert run"], {"reference": "fixture-convert"})
        await asyncio.wait_for(ready.wait(), timeout=2)
        await pilot.pause(0.3)
        assert "execute" in str(app.query_one("#overview-current", Static).content)
        assert "Testing registry unavailable" in str(
            app.query_one("#overview-attention", Static).content
        )
        await app.switch("Runs")
        assert app.controller.active()
        await app.switch("Overview")
        assert app.state.selection == RUN
        cancel = asyncio.create_task(app.controller.cancel())
        await asyncio.wait_for(cancelling.wait(), timeout=2)
        app.render_operation()
        assert "interrupting" in str(app.query_one("#overview-current", Static).content)
        cleanup.set()
        await cancel
        await owned
        await pilot.pause(0.4)
        assert app.state.selection == RUN
        assert app.controller.current is not None and app.controller.current.state == "interrupted"
        assert app.screen == app.root_screen
        assert set(calls) == {"convert run", "engine status", "database status"}


@mark.asyncio
async def test_overview_exact_recovery_and_evidence_resolution(tmp_path: Path) -> None:
    ctx = context(tmp_path)
    directory = ctx.paths.runs_dir
    intent = str(directory / MANAGED / "execution-intent.json")
    failed: dict[str, object] = {
        "status": "failed",
        "verification": "verified",
        "registration": "failed",
        "execution_id": MANAGED,
        "execution_receipt": intent,
        "submitted_sha256": "digest",
    }
    report(directory, MANAGED, failed, "execution-result.json")
    report(
        directory,
        RUN,
        {
            "kind": "historical_pipeline",
            "state": "interrupted",
            "name": "Exact recipe",
            "submitted_sha256": "fixture",
            "resolved_sha256": "fixture",
            "started_at": "2026-11-20T12:00:00+00:00",
        },
    )
    report(directory, SECOND, {"state": "failed", "name": "Unrelated historic failure"})
    app = ForgeApp(ctx)

    async def status(
        _: str, __: Values, ___: ApplicationContext, ____: Progress, _____: Edit
    ) -> object:
        return {"database": "marketforge_test", "initialized": True}

    app.overview_probe = status
    async with app.run_test(size=(120, 32)) as pilot:
        await pilot.pause()
        assert set(app.state.rows) == {RUN, SECOND, MANAGED}
        assert "Unrelated historic failure" not in str(
            app.query_one("#overview-attention", Static).content
        )
        row = app.state.rows[RUN]
        app.open_overview_run(row)
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, Inspection)
        assert "Resume retained pipeline" in screen.links
        screen.links["Resume retained pipeline"]()
        await pilot.pause()
        assert app.screen.query_one("#field-reference", Input).value == RUN
        await pilot.press("escape", "escape")
        assert app.overview_needs_registration({**failed, "submitted_sha256": "other"})
        recovered: dict[str, object] = {
            "registration": "registered",
            "execution": {"execution_id": MANAGED, "submitted_sha256": "digest"},
            "publications": {"output": {"publication_id": 7654321}},
        }
        report(directory, RECOVERY, recovered, "registration-result.json")
        app.action_reload()
        await pilot.pause()
        assert "Recover registration" not in app.overview_run_links(MANAGED, failed)
        assert app.overview_needs_registration({**failed, "submitted_sha256": "other"})
        assert "Publication 7654321" in app.overview_run_links(RECOVERY, recovered)
        assert "registration outstanding" not in str(
            app.query_one("#overview-attention", Static).content
        )
        assert len(run_page(directory, 8, 0)[0]) == 2
        assert len(run_page(directory, 8, 0, include_managed=True)[0]) == 4


def test_standalone_phase_has_no_pipeline_stages() -> None:
    op = Operation("fixture", "normalize run", reference="fixture", progress="validate")
    label = overview.operation_lines(op, True, 24, op.snapshot()).plain
    assert "fixture" in label and "validate" in label
    assert "Acquire" not in label


@mark.asyncio
async def test_status_generation_rejects_late_results(tmp_path: Path) -> None:
    ctx = context(tmp_path)
    entered, cancelled, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
    calls = 0

    async def probe(
        key: str, _: Values, __: ApplicationContext, ___: Progress, ____: Edit
    ) -> object:
        nonlocal calls
        if key == "engine status":
            return {"targets": [{"target": "normalize", "status": "missing"}]}
        calls += 1
        if calls == 1:
            entered.set()
            try:
                await asyncio.Future[None]()
            except asyncio.CancelledError:
                cancelled.set()
                await release.wait()
            return {"database": "marketforge_test", "initialized": False}
        return {"database": "marketforge_test", "initialized": True}

    app = ForgeApp(ctx)
    app.overview_probe = probe
    async with app.run_test(size=(80, 24)) as pilot:
        await asyncio.wait_for(entered.wait(), timeout=2)
        await app.switch("Runs")
        await asyncio.wait_for(cancelled.wait(), timeout=2)
        await app.switch("Overview")
        release.set()
        await pilot.pause(0.3)
        registry = app.overview_status["Registry"]
        assert registry.text == "connected"
        app.render_operation()
        assert app.overview_status["Registry"].checked == registry.checked
