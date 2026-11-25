"""Bounded recipe mutation and interactive table regressions."""

import tomllib
from pathlib import Path
from typing import cast

import pytest
from rich.console import Console
from textual.widgets import Static

from forge.historical.archives.config import ArchiveRequest
from forge.historical.archives.templates import ACQUISITION_TEMPLATE
from forge.historical.authoring import (
    add_requests,
    create_pipeline,
    create_with_requests,
    recipe_writer,
)
from forge.shared.config.models import AppSettings
from forge.shared.errors import MarketForgeError
from forge.shared.paths import ProjectPaths
from forge.startup.context import ApplicationContext
from forge.tui.app import ForgeApp
from forge.tui.inspection import ActionMenu, Inspection
from forge.tui.presentation import Results
from forge.tui.resources import Row, local_page


def request(symbol: str, kind: str = "trades") -> ArchiveRequest:
    return ArchiveRequest.model_validate(
        {
            "exchange": "bybit",
            "instrument_type": "spot",
            "market_category": "spot",
            "scope": {"kind": "instrument", "identifier": symbol},
            "data_type": kind,
        }
    )


def test_authoring_preserves_comments_interval_and_external_edits(tmp_path: Path) -> None:
    path = create_pipeline(tmp_path, "pipeline")
    before = path.read_bytes()
    with pytest.raises(MarketForgeError, match="already exists"):
        create_pipeline(tmp_path, "pipeline")
    intent = (request("ETHUSDT"), request("ETHUSDT", "order_book_l2"))
    with recipe_writer(path), pytest.raises(MarketForgeError, match="being edited"):
        add_requests(tmp_path, "pipeline", intent, pipeline=True, expected=before)
    change = add_requests(tmp_path, "pipeline", intent, pipeline=True, expected=before)
    assert change.added == 2
    after = path.read_bytes()
    assert b"# Half-open UTC" in after
    assert tomllib.loads(after.decode())["interval"] == tomllib.loads(before.decode())["interval"]
    assert len(tomllib.loads(after.decode())["requests"]) == 3
    assert (
        add_requests(tmp_path, "pipeline", intent, pipeline=True, expected=after).already_present
        == 2
    )
    assert path.read_bytes() == after
    with pytest.raises(MarketForgeError, match="changed"):
        add_requests(tmp_path, "pipeline", intent, pipeline=True, expected=before)
    assert path.read_bytes() == after
    created = create_with_requests(
        tmp_path,
        "from-market",
        intent,
        pipeline=False,
        start="2026-09-01T00:00:00Z",
        end="2026-09-02T00:00:00Z",
    )
    assert len(tomllib.loads(created.path.read_text())["requests"]) == 2


def test_local_sort_happens_before_page() -> None:
    rows = [Row(str(i), f"recipe-{i:03}", "pipeline", "Saved", {}) for i in range(75)]
    first, more = local_page(rows, "", 0, "Recipe", True)
    assert more and first[0].id == "74" and first[-1].id == "25"
    second, more = local_page(rows, "", 50, "Recipe", True)
    assert not more and second[0].id == "24"
    filtered, _ = local_page(rows, "074", 0, "", False)
    assert [row.id for row in filtered] == ["74"]


@pytest.mark.asyncio
async def test_recipe_mouse_activation_sort_and_exact_menu(tmp_path: Path) -> None:
    paths = ProjectPaths(tmp_path, tmp_path / "data")
    directory = paths.pipeline_config_dir
    directory.mkdir(parents=True)
    for name in ("a", "b"):
        (directory / f"{name}.toml").write_text(ACQUISITION_TEMPLATE)
    opened: list[Path] = []

    class Workspace(ForgeApp):
        CSS_PATH = Path(__file__).resolve().parents[3] / "tui/workspace.tcss"

        async def edit_recipe(self, path: Path) -> None:
            opened.append(path)

    app = Workspace(ApplicationContext(AppSettings(), paths, None, Console(), Console()))
    async with app.run_test(size=(120, 32)) as pilot:
        await app.switch("Workflows", "Pipeline")
        await pilot.pause(0.3)
        assert app.query_one("#recipe-new").display
        assert len(app.query("#inspector")) == 0
        table = app.query_one("#results", Results)
        table.focus()
        await pilot.press("down")
        assert not opened
        await pilot.press("enter")
        await pilot.pause(0.35)
        assert isinstance(app.screen, Inspection)
        assert app.screen.heading == "b"
        assert not opened
        await pilot.press("escape")
        first_row = table.styles.gutter.top + table.header_height
        await pilot.click("#results", offset=(10, first_row))
        await pilot.pause(0.35)
        assert isinstance(app.screen, Inspection)
        assert app.screen.heading == "a"
        await pilot.press("escape")
        await pilot.click("#results", offset=(10, first_row - 1))
        await pilot.pause(0.3)
        assert app.state.sort == "Recipe"
        assert "↑" in str(app.query_one("#query-state", Static).content)
        app.state.selection = "a"
        await pilot.click("#results", offset=(10, first_row + 1), button=3)
        await pilot.pause()
        assert isinstance(app.screen, ActionMenu)
        assert app.screen.heading == "PIPELINE · b"
        assert {"Inspect", "Edit", "Validate", "Plan", "Run", "Add Acquisition Request"} <= set(
            app.screen.actions
        )
        assert "Pipeline list" not in app.screen.actions and "List" not in app.screen.actions
        await pilot.press("escape")
        assert app.state.selection == "b"


def test_runs_filter_and_sort_retained_duration_before_page(tmp_path: Path) -> None:
    import json

    from forge.orchestration.inspection import run_page

    base = {
        "kind": "historical_pipeline",
        "state": "complete",
        "name": "same",
        "started_at": "2026-11-20T00:00:00Z",
    }
    for suffix, end, exchange in (
        (1, "2026-11-20T02:00:00Z", "okx"),
        (2, "2026-11-20T00:01:00Z", "bybit"),
        (3, None, "bybit"),
        (4, "2026-11-20T00:20:00Z", "bybit"),
    ):
        directory = tmp_path / f"11111111-1111-4111-8111-{suffix:012}"
        directory.mkdir()
        (directory / "report.json").write_text(json.dumps({**base, "finished_at": end}))
        (directory / "resolved-plan.json").write_text(
            json.dumps(
                {
                    "recipe": {
                        "requests": [
                            {"exchange": exchange, "scope": {"identifier": "BTCUSDT"}},
                        ]
                    }
                }
            )
        )
    rows, more = run_page(
        tmp_path, 1, 0, filters={"exchange": "bybit"}, sort="Duration", descending=True
    )
    assert more and str(rows[0]["run_id"]).endswith("4")
    rows, more = run_page(
        tmp_path, 1, 1, filters={"exchange": "bybit"}, sort="Duration", descending=True
    )
    assert more and str(rows[0]["run_id"]).endswith("2")
    rows, more = run_page(
        tmp_path, 1, 2, filters={"exchange": "bybit"}, sort="Duration", descending=True
    )
    assert not more and str(rows[0]["run_id"]).endswith("3")


@pytest.mark.asyncio
async def test_workspace_configuration_actions_use_shared_reports(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from forge.tui import services
    from forge.tui.actions import BY_KEY

    paths = ProjectPaths(tmp_path, tmp_path / "data")
    context = ApplicationContext(AppSettings(), paths, None, Console(), Console(stderr=True))

    def inspect(selected: ProjectPaths, operation: str) -> dict[str, object]:
        assert selected == paths
        assert operation == "pipeline"
        return {"effective": {"normalize": {}, "merge": {}}}

    monkeypatch.setattr(services, "runtime_report", inspect)
    assert BY_KEY["engine runtime"].area == "Workspace tools"
    assert await services.system("config show", {}, context, lambda _: None) == (
        context.configuration_report()
    )
    assert await services.system(
        "engine runtime", {"reference": "pipeline"}, context, lambda _: None
    ) == {"effective": {"normalize": {}, "merge": {}}}


@pytest.mark.asyncio
async def test_pipeline_source_menu_opens_reviewable_draft(tmp_path: Path) -> None:
    from textual.containers import VerticalScroll
    from textual.widgets import Input, Select

    from forge.tui.actions import Values
    from forge.tui.screens import Confirm, EntityChoices, EntityPicker, Form

    paths = ProjectPaths(tmp_path, tmp_path / "data")
    path = create_pipeline(paths.pipeline_config_dir, "sources")
    before = path.read_bytes()

    class Workspace(ForgeApp):
        CSS_PATH = Path(__file__).resolve().parents[3] / "tui/workspace.tcss"

        async def search_entities(self, key: str, search: str, values: Values) -> EntityChoices:
            assert key == "requests--1--scope--identifier"
            assert values["requests--1--exchange"] == "bybit"
            return (("Bybit · ETHUSDT · Spot · bybit.spot.spot.ETHUSDT", "ETHUSDT"),)

    app = Workspace(ApplicationContext(AppSettings(), paths, None, Console(), Console()))
    async with app.run_test(size=(120, 32)) as pilot:
        await app.switch("Workflows", "Pipeline")
        await pilot.pause(0.35)
        table = app.query_one("#results", Results)
        table.focus()
        await pilot.press("f2")
        await pilot.pause()
        menu = app.screen
        assert isinstance(menu, ActionMenu)
        index = menu.actions.index("Add Acquisition Request")
        menu.query_one(f"#menu-{index}").focus()
        await pilot.press("enter")
        await pilot.pause()
        form = app.screen
        assert isinstance(form, Form)
        for key, value in (
            ("exchange", "bybit"),
            ("instrument_type", "spot"),
            ("market_category", "spot"),
            ("scope--kind", "instrument"),
            ("data_type", "trades"),
        ):
            control = cast(Select[str], form.query_one(f"#field-requests--1--{key}", Select))
            control.value = value
        find = form.query_one("#find-requests--1--scope--identifier")
        form.query_one(".fields", VerticalScroll).scroll_to_widget(find, animate=False)
        await pilot.pause()
        assert await pilot.click(find)
        await pilot.pause()
        assert isinstance(app.screen, EntityPicker)
        await pilot.press("enter", "down", "enter")
        await pilot.pause()
        assert await pilot.click("#entity-use")
        await pilot.pause()
        assert form.query_one("#field-requests--1--scope--identifier", Input).value == "ETHUSDT"
        assert path.read_bytes() == before
        assert await pilot.click("#submit")
        await pilot.pause()
        assert isinstance(app.screen, Confirm)
        assert "ETHUSDT" in app.screen.explanation
        assert path.read_bytes() == before
        assert await pilot.click("#confirm")
        await pilot.pause()
        assert isinstance(app.screen, Inspection)
        assert app.controller.history == []
    saved = tomllib.loads(path.read_text())
    assert saved["requests"][1]["scope"]["identifier"] == "ETHUSDT"
    assert saved["interval"] == tomllib.loads(before.decode())["interval"]


@pytest.mark.asyncio
async def test_recipe_remove_review_archive_cancel_and_restore(tmp_path: Path) -> None:
    from textual.widgets import Button

    from forge.tui.screens import Confirm, Form

    paths = ProjectPaths(tmp_path, tmp_path / "data")
    created = create_with_requests(
        paths.acquisition_config_dir,
        "mixed",
        (request("BTCUSDT"), request("BTCUSDT", "order_book_l2"), request("ETHUSDT")),
        pipeline=False,
        start="2026-09-01T00:00:00Z",
        end="2026-09-02T00:00:00Z",
    )
    path = created.path
    before = path.read_bytes()
    app = ForgeApp(ApplicationContext(AppSettings(), paths, None, Console(), Console(), True))
    app.initial_area = "Workflows"
    app.initial_view = "Acquisition"
    async with app.run_test(size=(150, 40)) as pilot:
        await pilot.pause(0.3)
        app.action_context()
        await pilot.pause()
        assert isinstance(app.screen, ActionMenu)
        index = app.screen.actions.index("Remove from Recipe…")
        await pilot.click(f"#menu-{index}")
        assert isinstance(app.screen, ActionMenu)
        assert "order_book_l2" in app.screen.actions[1]
        await pilot.press("down", "enter")
        await pilot.pause()
        assert isinstance(app.screen, Form)
        assert path.read_bytes() == before
        await pilot.click("#submit")
        await pilot.pause()
        assert isinstance(app.screen, Confirm)
        assert "order_book_l2" in app.screen.explanation
        await pilot.click("#confirm")
        await pilot.pause()
        assert isinstance(app.screen, Inspection)
        saved = path.read_bytes()
        config = tomllib.loads(saved.decode())
        assert [(r["scope"]["identifier"], r["data_type"]) for r in config["requests"]] == [
            ("BTCUSDT", "trades"),
            ("ETHUSDT", "trades"),
        ]

        await pilot.press("f2")
        assert isinstance(app.screen, ActionMenu)
        await pilot.click(f"#menu-{app.screen.actions.index('Delete Recipe…')}")
        await pilot.pause()
        for cancel in (True, False):
            assert isinstance(app.screen, Inspection)
            assert app.screen.heading == "Recipe deletion preview"
            await pilot.press("f2")
            assert isinstance(app.screen, ActionMenu)
            await pilot.click("#menu-0")
            await pilot.pause()
            assert isinstance(app.screen, Confirm)
            assert app.screen.query_one("#confirm", Button).variant == "error"
            await pilot.click("#back" if cancel else "#confirm")
            await pilot.pause()
            assert path.exists() is cancel
        while app.screen != app.root_screen:
            await pilot.press("escape")
        app.action_context()
        await pilot.pause()
        assert isinstance(app.screen, ActionMenu)
        index = app.screen.actions.index("Restore Archived Recipe…")
        await pilot.click(f"#menu-{index}")
        assert isinstance(app.screen, ActionMenu)
        await pilot.click("#menu-0")
        await pilot.pause()
        assert path.read_bytes() == saved
