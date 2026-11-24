"""Bounded Textual interactions and supported CLI parity checks."""

import argparse
import asyncio
import io
from pathlib import Path
from typing import cast

import pytest
from rich.console import Console
from textual.widgets import Input, Static

from forge.cli.main import build_parser, main
from forge.shared.config.models import AppSettings
from forge.shared.ownership import OwnershipConflictError, WorkspaceOwnership
from forge.shared.paths import ProjectPaths
from forge.startup.context import ApplicationContext
from forge.tui.actions import BY_KEY, Values
from forge.tui.app import ForgeApp, Results
from forge.tui.controller import Controller
from forge.tui.operation_view import OperationView
from forge.tui.screens import Confirm, Form
from forge.tui.workflows import Edit, Progress


def leaves(parser: argparse.ArgumentParser, path: str = "") -> set[str]:
    def choices(value: object) -> dict[str, object]:
        return cast(dict[str, object], value) if isinstance(value, dict) else {}

    descendant: set[str] = set()
    for action in parser._actions:
        for name, child in choices(action.choices).items():
            if isinstance(child, argparse.ArgumentParser):
                descendant.update(leaves(child, f"{path} {name}".strip()))
    return descendant or {path}


def test_explicit_action_inventory_and_shell_quoting() -> None:
    canonical = leaves(build_parser())
    assert set(BY_KEY) <= canonical
    assert {"pipeline run", "datasets verify", "normalize prepare", "runs report"} <= set(BY_KEY)
    assert (
        BY_KEY["convert new"].command(
            {
                "reference": "demo",
                "route": "normalize",
                "source": "source has spaces",
                "quote": "USDT",
            }
        )
        == "mf convert new demo --from-normalize 'source has spaces' --target-quote USDT"
    )


def test_convert_action_inventory() -> None:
    required = {name for name in leaves(build_parser()) if name.startswith("convert ")}
    actions = {name for name in BY_KEY if name.startswith("convert ")}
    assert actions == required - {
        "convert delete",
        "convert config edit",
        "convert config set",
        "convert config unset",
    }


@pytest.fixture
def context(tmp_path: Path) -> ApplicationContext:
    settings = AppSettings()
    console = Console(file=io.StringIO())
    return ApplicationContext(
        settings, ProjectPaths(tmp_path, tmp_path / "data"), None, console, console, testing=True
    )


def test_non_terminal_does_not_launch(
    context: ApplicationContext, capsys: pytest.CaptureFixture[str]
) -> None:
    (context.paths.root / "forge").mkdir()
    (context.paths.root / "engine").mkdir()
    (context.paths.root / "forge/pyproject.toml").write_text('[project]\nname="marketforge"\n')
    assert main(["--project-root", str(context.paths.root), "--json", "tui", "--testing"]) == 2
    assert "interactive terminal" in capsys.readouterr().err


@pytest.mark.asyncio
async def test_form_validation_remains_in_place(context: ApplicationContext) -> None:
    app = ForgeApp(context)
    async with app.run_test(size=(65, 19)) as pilot:
        app.open_action(BY_KEY["datasets lineage"])
        await pilot.pause()
        assert isinstance(app.screen, Form)
        field = app.screen.query_one("#field-id", Input)
        field.value = "wrong"
        await pilot.click("#submit")
        assert field.value == "wrong"
        assert "requires a number" in str(app.screen.query_one("#form-error", Static).content)
        await pilot.press("escape")
        assert not isinstance(app.screen, Form)


@pytest.mark.asyncio
async def test_palette_and_navigation_while_owned_operation_runs(
    context: ApplicationContext,
) -> None:
    started = asyncio.Event()
    finish = asyncio.Event()
    calls: list[tuple[str, Values]] = []

    async def invoke(
        key: str, values: Values, _: ApplicationContext, progress: Progress, __: Edit
    ) -> object:
        calls.append((key, dict(values)))
        if key == "pipeline run":
            progress("normalize prepare")
            started.set()
            await finish.wait()
            return {"state": "stopped", "kind": "historical_pipeline", "run_id": "abc"}
        return {"runs": []}

    app = ForgeApp(context, invoke=invoke)
    async with app.run_test(size=(120, 32)) as pilot:
        await pilot.press("ctrl+p")
        await pilot.press(*"pipeline run")
        await pilot.pause()
        await pilot.press("enter")
        await pilot.pause()
        if not isinstance(app.screen, Form):
            await pilot.press("enter")
            await pilot.pause()
        assert isinstance(app.screen, Form)
        app.screen.query_one("#field-reference", Input).value = "tiny"
        await pilot.click("#submit")
        await pilot.pause()
        assert isinstance(app.screen, Confirm)
        await pilot.click("#confirm")
        await asyncio.wait_for(started.wait(), timeout=2)
        await pilot.pause()
        assert isinstance(app.screen, OperationView)
        await pilot.click("#op-back")
        await pilot.click("#area-5")
        await pilot.pause()
        assert app.area == "Runs"
        assert app.controller.active()
        assert calls[0] == (
            "pipeline run",
            {"reference": "tiny", "stop_after": "", "stage_terminal": ""},
        )
        await pilot.press("ctrl+o")
        await pilot.pause()
        live = app.screen
        assert isinstance(live, OperationView)
        assert live.query_one("#op-actions").disabled
        finish.set()
        await pilot.pause(0.4)
        assert app.screen is live
        assert app.controller.current is not None
        assert app.controller.current.state == "stopped"
        assert not live.query_one("#op-actions").disabled
        assert "Resume retained pipeline" in live.choices()


@pytest.mark.asyncio
async def test_query_versions_and_page_selection(
    context: ApplicationContext, monkeypatch: pytest.MonkeyPatch
) -> None:
    from typing import Any

    from forge.datasets import workspace_inventory

    requests: list[Values] = []
    slow_started = asyncio.Event()
    release_stale = asyncio.Event()

    async def page(
        paths: object, stage: str, offset: int, search: str = ""
    ) -> tuple[list[dict[str, Any]], bool]:
        if paths != context.paths or stage != "Raw":
            return [], False
        requests.append({"offset": offset, "search": search})
        if search == "obsolete":
            slow_started.set()
            try:
                await release_stale.wait()
            except asyncio.CancelledError:
                await release_stale.wait()
        return [
            {
                "archives": ["local:99" if search == "obsolete" else "local:12"],
                "state": "complete",
                "symbol": search,
                "data_type": "trade",
            }
        ], True

    monkeypatch.setattr(workspace_inventory, "stage_page", page)
    app = ForgeApp(context)
    async with app.run_test(size=(100, 25)) as pilot:
        await app.switch("Data", "Raw")
        await pilot.pause(0.3)
        assert app.query_one("#results", Results).row_count == 1
        field = app.query_one("#filter", Input)
        field.value = "A"
        field.value = "BTCUSDT"
        await pilot.pause(0.3)
        assert not any(request.get("search") == "A" for request in requests)
        assert requests[-1]["search"] == "BTCUSDT"
        field.value = "obsolete"
        await asyncio.wait_for(slow_started.wait(), timeout=2)
        field.value = "BTCUSDT"
        await pilot.pause(0.4)
        release_stale.set()
        await pilot.pause()
        assert list(app.state.rows) == ['["local:12"]']
        app.action_next()
        await pilot.pause(0.3)
        assert requests[-1]["offset"] == workspace_inventory.PAGE_SIZE
        assert list(app.state.rows) == ['["local:12"]']
        assert app.state.filter == "BTCUSDT"


@pytest.mark.asyncio
async def test_cancel_joins_service_before_releasing_workspace(context: ApplicationContext) -> None:
    started, cleaned = asyncio.Event(), asyncio.Event()

    async def invoke(
        key: str, values: Values, _: ApplicationContext, progress: Progress, editor: Edit
    ) -> object:
        started.set()
        try:
            await asyncio.Future[None]()
        finally:
            await asyncio.sleep(0)
            cleaned.set()
        return {}

    async def editor(path: Path) -> None:
        raise AssertionError(path)

    controller = Controller(context, editor, invoke)
    with WorkspaceOwnership(context.paths):
        pass
    task = controller.launch(BY_KEY["normalize run"], {"reference": "sample"})
    await asyncio.wait_for(started.wait(), timeout=2)
    with pytest.raises(OwnershipConflictError), WorkspaceOwnership(context.paths):
        pytest.fail("A second owner must conflict during processing.")
    await controller.cancel()
    assert cleaned.is_set()
    assert (await task).state == "interrupted"
    with WorkspaceOwnership(context.paths):
        pass


@pytest.mark.asyncio
async def test_quit_confirmation_and_copy_fallback(context: ApplicationContext) -> None:
    started, cleaned = asyncio.Event(), asyncio.Event()

    async def invoke(
        key: str, values: Values, _: ApplicationContext, progress: Progress, editor: Edit
    ) -> object:
        started.set()
        try:
            await asyncio.Future[None]()
        finally:
            cleaned.set()
        return {}

    app = ForgeApp(context, invoke=invoke)
    async with app.run_test(size=(65, 19)) as pilot:
        app.copy_value("a path with spaces")
        await pilot.pause()
        await pilot.click("#copy")
        assert app.clipboard == "a path with spaces"
        await pilot.press("escape")
        task = app.controller.launch(BY_KEY["pipeline run"], {"reference": "fixture"})
        await asyncio.wait_for(started.wait(), timeout=2)
        await pilot.press("ctrl+q")
        await pilot.pause()
        assert isinstance(app.screen, Confirm)
        await pilot.click("#back")
        assert app.controller.active()
        await pilot.press("ctrl+q")
        await pilot.pause()
        await pilot.click("#confirm")
        assert (await task).state == "interrupted"
        assert cleaned.is_set()


@pytest.mark.asyncio
async def test_publication_actions_require_explicit_file_scope(
    context: ApplicationContext, monkeypatch: pytest.MonkeyPatch
) -> None:
    from typing import Any

    from forge.datasets import workspace_inventory
    from forge.tui import app as shell
    from forge.tui.inspection import Inspection, RecordTable

    calls: list[tuple[str, Values]] = []
    fixture: dict[str, object] = {
        "registry_publication_id": 7,
        "engine_dataset_id": "700",
        "operation": "merge",
        "representative_file_id": 11,
        "primary_file_count": 2,
        "streams": [{"symbol": "BTCUSDT", "kind": "trade"}, {"symbol": "ETHBTC", "kind": "depth"}],
        "primary_files": [
            {"registry_primary_file_id": 11, "primary_file": "trades.parquet"},
            {"registry_primary_file_id": 12, "primary_file": "depth.parquet"},
        ],
    }

    async def publications(*args: object, **kwargs: object) -> dict[str, object]:
        return {"publications": [fixture], "has_more": False}

    async def invoke(
        key: str, values: Values, _: ApplicationContext, progress: Progress, editor: Edit
    ) -> object:
        calls.append((key, values))
        if key == "datasets verify":
            return {"status": "verified", "entire_publication_verified": False}
        return {"file": {"id": values["id"]}}

    monkeypatch.setattr(shell, "publications", publications)

    async def stage_page(*args: object) -> tuple[list[dict[str, Any]], bool]:
        return [{"publication": 7, "state": "Registered"}], False

    monkeypatch.setattr(workspace_inventory, "stage_page", stage_page)
    app = ForgeApp(context, invoke=invoke)
    async with app.run_test(size=(80, 24)) as pilot:
        await app.switch("Data", "Merged")
        await pilot.pause(0.3)
        assert app.query_one("#results", Results).size.height >= 14
        assert app.query_one("#area-2").has_class("active")
        app.query_one("#results", Results).focus()
        await pilot.press("enter")
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, Inspection)
        assert "Verify primary file" not in screen.links
        await pilot.click("#section-1")
        assert screen.query_one(RecordTable).row_count == 2
        await pilot.click("#section-2")
        screen.query_one(RecordTable).move_cursor(row=1, column=0)
        await pilot.press("enter")
        await pilot.pause()
        assert calls == [("datasets show", {"id": 12})]
        child = app.screen
        assert isinstance(child, Inspection)
        child.links["Verify primary file"]()
        await pilot.pause()
        assert calls[-1] == ("datasets verify", {"id": 12})
        assert isinstance(app.screen, Inspection)
        evidence = cast(dict[str, object], app.screen.evidence)
        assert evidence["entire_publication_verified"] is False
        await pilot.press("escape", "escape", "escape")
        assert app.state.selection == "7"


@pytest.mark.asyncio
async def test_instrument_binding_run_selector_and_market_next_page(
    context: ApplicationContext,
) -> None:
    import json
    from uuid import uuid4

    from forge.tui.resources import Row

    saved = str(uuid4())
    directory = context.paths.runs_dir / saved
    directory.mkdir(parents=True)
    (directory / "report.json").write_text(
        json.dumps({"kind": "historical_pipeline", "name": "old recipe"})
    )
    pages: list[int] = []

    async def invoke(
        key: str, values: Values, _: ApplicationContext, progress: Progress, editor: Edit
    ) -> object:
        if key != "markets list":
            return {}
        offset = int(values.get("offset", 0))
        pages.append(offset)
        return {
            "instruments": [
                {
                    "id": offset + 2,
                    "symbol": "ETHBTC",
                    "reference": "bybit.spot.ETHBTC",
                    "exchange": "bybit",
                    "market_category": "spot",
                    "instrument_type": "spot",
                }
            ],
            "has_more": offset == 0,
        }

    app = ForgeApp(context, invoke=invoke)
    async with app.run_test(size=(120, 32)) as pilot:
        app.open_action(BY_KEY["pipeline resume"])
        await pilot.pause()
        form = app.screen
        assert isinstance(form, Form)
        assert form.choices == (saved,)
        await pilot.press("escape")
        await app.switch("Markets", "Instruments")
        await pilot.pause(0.3)
        row = app.state.rows["bybit.spot.ETHBTC"]
        assert isinstance(row, Row)
        assert app.preset(BY_KEY["markets refresh"], row) == {
            "exchanges": "bybit",
            "markets": "spot",
            "instrument": "ETHBTC",
        }
        await pilot.press("pagedown")
        await pilot.pause(0.3)
        assert pages[-2:] == [0, 50]
        item = app.state.rows["bybit.spot.ETHBTC"].value
        assert isinstance(item, dict) and item["id"] == 52


def test_retained_search_precedes_page_limit(context: ApplicationContext) -> None:
    import json
    import os
    from uuid import UUID

    from forge.orchestration.inspection import run_page

    for index in range(71):
        directory = context.paths.runs_dir / str(UUID(int=index + 1))
        directory.mkdir(parents=True)
        (directory / "report.json").write_text(
            json.dumps({"name": "Rare target" if index == 0 else "Other"})
        )
        os.utime(directory, (100 + index, 100 + index))
    first, more = run_page(context.paths.runs_dir, 50, 0)
    second, rest = run_page(context.paths.runs_dir, 50, 50)
    match, _ = run_page(context.paths.runs_dir, 50, 0, "rare")
    assert more and not rest
    assert len(first) == 50 and len(second) == 21
    assert match[0]["name"] == "Rare target"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("view", "family", "middle", "execute_verb"),
    [
        ("Acquisition", "acquisition", "plan", "download"),
        ("Normalization", "normalize", "prepare", "run"),
        ("Merge", "merge", "prepare", "run"),
        ("Conversion", "convert", "prepare", "run"),
        ("Pipeline", "pipeline", "plan", "run"),
    ],
)
async def test_workflow_toolbar_dispatches_exact_selection(
    context: ApplicationContext,
    view: str,
    family: str,
    middle: str,
    execute_verb: str,
) -> None:
    from textual.widgets import Button

    from forge.tui.inspection import Inspection
    from forge.tui.resources import library

    source = library(context, family) / "exact.toml"
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_text("version = 1\n")
    finish = asyncio.Event()
    calls: list[str] = []

    async def invoke(
        key: str,
        values: Values,
        ctx: ApplicationContext,
        progress: Progress,
        _: Edit,
    ) -> object:
        calls.append(key)
        assert ctx.testing and values["reference"] == "exact"
        if key.endswith((" download", " run")):
            progress("Verified fixture phase")
            await finish.wait()
            return {"status": "complete"}
        if key.endswith((" plan", " prepare")):
            return {"status": "prepared" if middle == "prepare" else "planned"}
        assert key == f"{family} validate"
        return {"status": "valid"}

    app = ForgeApp(context, invoke=invoke)
    app.initial_area, app.initial_view = "Workflows", view
    try:
        async with app.run_test(size=(110, 33)) as pilot:
            await pilot.pause(0.4)
            run = app.query_one(f"#workflow-{execute_verb}", Button)
            assert run.display and not run.disabled
            assert "exact" in str(run.tooltip)
            await pilot.click("#workflow-validate")
            await pilot.pause()
            assert isinstance(app.screen, Inspection)
            await pilot.press("escape")
            await pilot.click(f"#workflow-{middle}")
            async with asyncio.timeout(3):
                while not isinstance(app.screen, Form):
                    await pilot.pause(0.05)
            await pilot.click("#submit")
            if middle == "prepare":
                async with asyncio.timeout(3):
                    while not isinstance(app.screen, Confirm):
                        await pilot.pause(0.05)
                await pilot.click("#confirm")
            async with asyncio.timeout(3):
                while f"{family} {middle}" not in calls or app.screen is app.root_screen:
                    await pilot.pause(0.05)
            await pilot.press("escape")
            await pilot.click(run)
            await pilot.pause()
            assert isinstance(app.screen, Form)
            assert app.screen.query_one("#field-reference", Input).disabled
            await pilot.click("#submit")
            await pilot.pause()
            assert isinstance(app.screen, Confirm)
            await pilot.click("#confirm")
            await pilot.pause()
            assert isinstance(app.screen, OperationView)
            assert app.screen.operation.snapshot() == "Verified fixture phase"
            await pilot.click("#op-back")
            assert app.controller.active()
            await pilot.press("ctrl+o")
            await pilot.pause()
            assert isinstance(app.screen, OperationView)
            finish.set()
            await pilot.pause()
            assert app.screen.operation.state == "complete"
            assert calls == [
                f"{family} validate",
                f"{family} {middle}",
                f"{family} validate",
                f"{family} {execute_verb}",
            ]
    finally:
        finish.set()
