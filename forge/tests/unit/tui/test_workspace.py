"""Bounded Textual interactions and supported CLI parity checks."""

import argparse
import asyncio
import io
from pathlib import Path
from typing import cast

import pytest
from rich.console import Console
from textual.widgets import Input, Static, TextArea

from forge.cli.main import build_parser, main
from forge.shared.config.models import AppSettings
from forge.shared.ownership import OwnershipConflictError, WorkspaceOwnership
from forge.shared.paths import ProjectPaths
from forge.startup.context import ApplicationContext
from forge.tui.actions import BY_KEY, Values
from forge.tui.app import ForgeApp, Results
from forge.tui.controller import Controller
from forge.tui.screens import Confirm, Detail, Form
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
    assert set(BY_KEY) == leaves(build_parser()) - {"tui", "acquisition import format new"}
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


@pytest.fixture
def context(tmp_path: Path) -> ApplicationContext:
    settings = AppSettings()
    console = Console(file=io.StringIO())
    return ApplicationContext(
        settings, ProjectPaths(tmp_path, tmp_path / "data"), None, console, console
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
            return {"state": "stopped"}
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
        await pilot.click("#area-6")
        await pilot.pause()
        assert app.area == "Runs"
        assert app.controller.active()
        assert calls[0] == ("pipeline run", {"reference": "tiny", "stop_after": ""})
        finish.set()
        await pilot.pause()
        assert app.controller.current is not None
        assert app.controller.current.state == "stopped"
        assert isinstance(app.screen, Detail)
        assert '"stopped"' in app.screen.query_one(TextArea).text


@pytest.mark.asyncio
async def test_query_versions_and_page_selection(context: ApplicationContext) -> None:
    requests: list[Values] = []

    async def invoke(
        key: str, values: Values, _: ApplicationContext, progress: Progress, editor: Edit
    ) -> object:
        if key == "datasets list":
            requests.append(dict(values))
            symbol = values.get("instrument", "all")
            return {
                "datasets": [
                    {"id": 12, "status": "complete", "symbol": symbol, "data_type": "trade"}
                ],
                "has_more": True,
            }
        return {}

    app = ForgeApp(context, invoke=invoke)
    async with app.run_test(size=(100, 25)) as pilot:
        await app.switch("Data", "Primary files")
        await pilot.pause(0.3)
        assert app.query_one(Results).row_count == 1
        field = app.query_one("#filter", Input)
        field.value = "A"
        field.value = "BTCUSDT"
        await pilot.pause(0.3)
        assert not any(request.get("instrument") == "A" for request in requests)
        assert requests[-1]["instrument"] == "BTCUSDT"
        app.action_next()
        await pilot.pause(0.3)
        assert requests[-1]["offset"] == 50
        assert list(app.state.rows) == ["12"]
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
