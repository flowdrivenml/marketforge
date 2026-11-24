"""Behavioral visual checks against Textual's actual styled widgets."""

import asyncio
import tomllib
from html import unescape
from pathlib import Path
from typing import cast

import pytest
from rich.console import Console
from textual.app import App
from textual.color import Color
from textual.containers import VerticalScroll
from textual.events import MouseScrollDown
from textual.widgets import Button, Collapsible, Input, Select, Static
from textual.widgets._toast import Toast

from forge.historical.pipelines.config import PipelineRecipe
from forge.shared.config.models import AppSettings
from forge.shared.paths import ProjectPaths
from forge.shared.terminal.palette import BLUE, CYAN, GREEN, MUTED, RED, YELLOW
from forge.startup.context import ApplicationContext
from forge.tui import app as shell
from forge.tui.actions import Action, Field, Values
from forge.tui.app import ForgeApp
from forge.tui.design import success_message
from forge.tui.inspection import ActionMenu
from forge.tui.presentation import Cell, Results, cell_text, status
from forge.tui.recipe_forms import RecipeForm
from forge.tui.resources import Row
from forge.tui.screens import Confirm, Detail, Form


class DialogHarness(App[None]):
    CSS_PATH = Path(__file__).resolve().parents[3] / "tui/workspace.tcss"


def pipeline_fixture() -> dict[str, object]:
    return tomllib.loads("""
version = 1
[interval]
start = 2026-11-20T00:00:00Z
end = 2026-11-23T00:00:00Z
[[requests]]
exchange = "bybit"
instrument_type = "spot"
market_category = "spot"
data_type = "trades"
scope = {kind = "instrument", identifier = "SOLUSDT"}
""")


class VisualWorkspace(ForgeApp):
    CSS_PATH = Path(__file__).resolve().parents[3] / "tui/workspace.tcss"

    async def load_rows(
        self, area: str, view: str, search: str, offset: int
    ) -> tuple[list[Row], bool]:
        if search == "offline":
            raise ValueError("Fixture registry unavailable")
        if search:
            return [], False
        return [
            Row(
                "967",
                "BTCUSDT",
                "Publication",
                "Registered",
                {
                    "exchange": "bybit",
                    "symbol": "BTCUSDT",
                    "instrument_type": "spot",
                    "market_category": "spot",
                    "data_type": "trade",
                    "publication": 967,
                    "file_count": 2,
                    "state": "Registered",
                },
                "967",
            )
        ], False


@pytest.mark.asyncio
@pytest.mark.parametrize("size", [(80, 24), (100, 30), (120, 32), (160, 48)])
async def test_workspace_focus_selection_and_messages(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, size: tuple[int, int]
) -> None:
    async def registry(_: Path) -> str:
        return "dd917334-0000-0000-0000-000000000000"

    monkeypatch.setattr(shell, "selected_registry_id", registry)
    app = VisualWorkspace(
        ApplicationContext(
            AppSettings(),
            ProjectPaths(tmp_path, tmp_path),
            None,
            Console(),
            Console(),
            testing=True,
        )
    )
    app.initial_area, app.initial_view = "Data", "Normalized"
    async with app.run_test(size=size, notifications=True) as pilot:
        await pilot.pause(0.35)
        table = app.query_one("#results", Results)
        assert table.row_count == 1 and table.zebra_stripes
        active = app.query_one("#view-1", Button)
        assert active.has_class("active") and active.styles.color == Color.parse(BLUE)
        assert app.query_one("#area-2", Button).has_class("active")
        assert app.query_one("#page-next", Button).disabled
        assert table.virtual_size.width <= table.size.width
        field = app.query_one("#filter", Input)
        field.focus()
        await pilot.pause()
        unfocused_cursor = table.get_component_styles("datatable--cursor").background
        focused_input = field.styles.background
        table.focus()
        await pilot.pause()
        assert table.get_component_styles("datatable--cursor").background != unfocused_cursor
        assert table.get_component_styles("datatable--header").color == Color.parse(BLUE)
        assert table.get_component_styles("datatable--hover").background != unfocused_cursor
        assert table.cursor_foreground_priority == "renderable"
        assert field.styles.background != focused_input
        svg = app.export_screenshot()
        assert "BTCUSDT" in svg and "Normalized" in svg and "967" in svg

        if size[0] == 80:
            await pilot.press("ctrl+n")
            await pilot.pause()
        nav_active = app.query_one("#area-2", Button)
        other = app.query_one("#area-3", Button)
        assert nav_active.region.width
        other.focus()
        await pilot.pause()
        assert nav_active.has_class("active")
        assert nav_active.styles.border_left[1] == Color.parse(BLUE)
        assert other.styles.color == Color.parse(CYAN)
        assert nav_active.styles.background != other.styles.background
        if size[0] == 80:
            await pilot.press("ctrl+n")

        field.value = "empty"
        await pilot.pause(0.3)
        message = app.query_one("#resource-state", Static)
        assert message.display and not table.display
        assert "No normalized publications match" in str(message.content)
        field.value = "offline"
        await pilot.pause(0.3)
        assert message.has_class("-error") and "Fixture registry unavailable" in str(
            message.content
        )

        app.notify(success_message("Fixture validated"), title="Validation")
        app.notify("Fixture failed", title="Error", severity="error")
        async with asyncio.timeout(2):
            while len(app.query(Toast)) < 2:
                await pilot.pause(0.05)
        toasts = list(app.query(Toast))
        assert any(toast.has_class("-error") for toast in toasts)
        assert "Fixture validated" in unescape(app.export_screenshot()).replace("\xa0", " ")
        assert "Fixture failed" in unescape(app.export_screenshot()).replace("\xa0", " ")

        await app.switch("Workflows", "Acquisition")
        await pilot.pause()
        last_tab = app.query_one(f"#view-{len(shell.VIEWS['Workflows']) - 1}", Button)
        last_tab.focus()
        await pilot.pause(0.3)
        assert app.focused is last_tab
        assert last_tab.content_region.right <= app.query_one("#views").region.right
        assert str(last_tab.label) in unescape(app.export_screenshot()).replace("\xa0", " ")


@pytest.mark.asyncio
async def test_form_feedback_and_modal_roles(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def registry(_: Path) -> str:
        return "dd917334-0000-0000-0000-000000000000"

    async def validate(_: Values) -> dict[str, str]:
        return {}

    monkeypatch.setattr(shell, "selected_registry_id", registry)
    app = VisualWorkspace(
        ApplicationContext(
            AppSettings(), ProjectPaths(tmp_path, tmp_path), None, Console(), Console()
        )
    )
    app.initial_area, app.initial_view = "Data", "Normalized"
    async with app.run_test(size=(80, 24)) as pilot:
        form = Form(
            Action(
                "Validate fixture",
                "Workflows",
                (
                    Field("reference", "Recipe name", required=True, group="Recipe"),
                    Field("exchange", "Exchange", "choice", ("bybit", "okx")),
                ),
            ),
            local=True,
            validate=validate,
        )
        await app.push_screen(form)
        await pilot.pause()
        assert form.styles.background.a > 0
        await pilot.click("#form-validate")
        await pilot.pause()
        row = form.query_one("#row-reference")
        name = form.query_one("#field-reference", Input)
        assert row.has_class("-invalid")
        assert name.styles.border_left[1] == Color.parse(RED)
        assert form.query_one("#error-reference").display
        form.query_one("#field-reference", Input).value = "bybit_spot"
        await pilot.pause()
        assert row.has_class("-changed") and not row.has_class("-invalid")
        assert name.styles.border_left[1] == Color.parse(YELLOW)
        form.query_one("#form-validate", Button).press()
        async with asyncio.timeout(2):
            while not row.has_class("-validated"):
                await pilot.pause(0.05)
        assert name.styles.border_left[1] == Color.parse(GREEN)
        form.query_one(Select).focus()
        await pilot.press("enter")
        await pilot.pause()
        assert "okx" in app.export_screenshot()
        await pilot.press("escape")

        confirmation = Confirm(
            "Delete fixture", "Review the exact selected record.", destructive=True
        )
        await app.push_screen(confirmation)
        await pilot.pause()
        assert confirmation.query_one("#confirm", Button).has_class("-error")
        assert "Review the exact selected record" in unescape(app.export_screenshot()).replace(
            "\xa0", " "
        )
        await pilot.press("escape")
        assert app.screen is form

        menu = ActionMenu("BTCUSDT", ("Add to Recipe", "Inspect", "Delete"), anchor=(78, 22))
        await app.push_screen(menu)
        await pilot.pause()
        primary, inspect, destroy = menu.query(".menu Button").results(Button)
        assert app.focused is primary
        assert primary.styles.color == Color.parse(BLUE)
        assert primary.styles.border_left[1] == Color.parse(CYAN)
        assert destroy.styles.color == Color.parse(RED)
        await pilot.press("tab")
        assert app.focused is inspect
        assert inspect.styles.background != primary.styles.background
        assert inspect.region.right <= 80 and inspect.region.bottom <= 24


@pytest.mark.parametrize(
    ("label", "color"),
    [
        ("Complete", GREEN),
        ("Pending", YELLOW),
        ("Failed", RED),
        ("Running", CYAN),
        ("Waiting", MUTED),
    ],
)
def test_semantic_cell_styles(label: str, color: str) -> None:
    value = status(label)
    assert value.plain == label and value.style == color
    assert cell_text(Cell("Symbol", "BTCUSDT", 12)).style == CYAN
    assert cell_text(Cell("Files", 20, 8, number=True)).style == ""


@pytest.mark.asyncio
@pytest.mark.parametrize(("width", "height"), [(80, 24), (100, 30), (120, 32), (160, 48)])
async def test_pipeline_dialog_geometry_and_navigation(width: int, height: int) -> None:
    payload = pipeline_fixture()
    adapter = RecipeForm(PipelineRecipe, payload)
    fields, values = adapter.build()

    async def validate(raw: Values) -> dict[str, str]:
        return adapter.candidate(raw)[1]

    app = DialogHarness()
    async with app.run_test(size=(width, height)) as pilot:
        form = Form(
            Action("Edit · tui_test", "Workflows", fields),
            values,
            local=True,
            validate=validate,
            documentation=lambda: None,
            submit_label="Review changes",
        )
        results: list[Values | None] = []

        def capture(result: Values | None) -> None:
            results.append(result)

        await app.push_screen(form, capture)
        await pilot.pause()
        body = form.query_one(".dialog-body", VerticalScroll)
        footer = form.query_one(".form-footer")
        action = form.query_one("#submit", Button)
        start = form.query_one("#field-interval--start--date", Input)
        time = form.query_one("#field-interval--start--time", Input)
        end = form.query_one("#field-interval--end--date", Input)
        assert form.focused is start
        assert (start.region.y == time.region.y) == (width >= 96)
        assert (start.region.y == end.region.y) == (width >= 116)
        assert start.region.width >= len(start.value) + 3
        footer_region = footer.region
        assert body.region.bottom == footer_region.y
        assert action.region.bottom <= height and action.region.right <= width
        for field in fields:
            row = form.query_one(f"#row-{field.key}")
            if row.display and field.group != "Advanced Options":
                control = form.query_one(f"#field-{field.key}")
                assert control in form.focus_chain
                assert row.region.contains_region(control.region)
        assert "Historical" in app.export_screenshot()
        assert "Review" in app.export_screenshot()

        await pilot.press("tab")
        assert form.focused is not start
        await pilot.press("shift+tab")
        assert form.focused is start
        if body.max_scroll_y == 0:
            advanced = form.query_one("#advanced-options", Collapsible)
            advanced.query_one("CollapsibleTitle").focus()
            await pilot.press("enter")
            await pilot.pause()
            assert not advanced.collapsed
            body.scroll_home(animate=False, immediate=True)
        body.focus(scroll_visible=False)
        await pilot.press("pagedown")
        await pilot.pause(0.3)
        assert body.scroll_y > 0
        assert footer.region == footer_region
        body.scroll_home(animate=False)
        await pilot.pause()
        x, y = body.region.offset
        app.post_message(MouseScrollDown(None, x + 2, y + 2, 0, 1, 0, False, False, False))
        await pilot.pause(0.3)
        assert body.scroll_y > 0

        exchange = cast(Select[str], form.query_one("#field-requests--0--exchange", Select))
        exchange.focus()
        await pilot.pause()
        await pilot.press("enter")
        assert exchange.expanded
        await pilot.press("escape")
        assert not exchange.expanded
        assert await pilot.click("#form-validate")
        assert "Configuration validated" in str(form.query_one("#form-progress", Static).content)
        assert await pilot.click("#submit")
        await pilot.pause()
        assert len(results) == 1 and results[0] is not None
        candidate, issues = adapter.candidate(results[0])
        assert not issues and candidate == payload
        assert app.screen is not form


@pytest.mark.asyncio
async def test_request_and_advanced_disclosures_preserve_draft_on_resize() -> None:
    payload = pipeline_fixture()
    payload["requests"] = [
        {
            "exchange": "bybit",
            "instrument_type": "spot",
            "market_category": "spot",
            "data_type": "trades",
            "scope": {"kind": "instrument", "identifier": f"FIXTURE_{index:02d}_USDT"},
        }
        for index in range(10)
    ]
    adapter = RecipeForm(PipelineRecipe, payload)
    fields, values = adapter.build()
    key = "requests--9--scope--identifier"
    message = "The selected instrument must match the full verified catalogue reference. " * 4

    async def validate(raw: Values) -> dict[str, str]:
        return {key: message, "version": message}

    app = DialogHarness()
    async with app.run_test(size=(160, 48)) as pilot:
        form = Form(
            Action("Edit Pipeline", "Workflows", fields),
            values,
            local=True,
            validate=validate,
            expand=adapter.add,
        )
        await app.push_screen(form)
        await pilot.pause()
        request = form.query_one("#request-9", Collapsible)
        advanced = form.query_one("#advanced-options", Collapsible)
        assert request.collapsed and advanced.collapsed
        assert "FIXTURE_09_USDT" in str(request.title)
        header = request.query_one("CollapsibleTitle")
        body = form.query_one(".dialog-body", VerticalScroll)
        header.focus(scroll_visible=False)
        body.scroll_to_widget(header, center=True, animate=False, immediate=True)
        await pilot.pause()
        assert body.region.contains_region(header.region)
        assert await pilot.click(header, offset=(2, 0))
        symbol = form.query_one(f"#field-{key}", Input)
        symbol.value = "A_LONG_PENDING_IDENTIFIER_WITH_A_DISTINCT_SUFFIX_USDT"
        await pilot.pause()
        assert await pilot.click(header, offset=(2, 0))
        assert request.collapsed and "Modified" in str(request.title)
        await pilot.resize_terminal(80, 24)
        assert await pilot.click("#form-validate")
        await pilot.pause()
        assert not request.collapsed and form.focused is symbol
        assert advanced.collapsed and "1 issue" in str(advanced.title)
        assert symbol.value.endswith("DISTINCT_SUFFIX_USDT")
        error = form.query_one(f"#error-{key}", Static)
        assert error.display and error.region.height > 1
        assert symbol.region.bottom <= error.region.y
        assert (
            form.query_one(".dialog-body").region.bottom <= form.query_one(".form-footer").region.y
        )
        advanced.query_one("CollapsibleTitle").focus()
        await pilot.press("enter")
        await pilot.pause()
        assert not advanced.collapsed and form.query_one("#error-version").display
        add = form.query_one("#form-add", Button)
        add.focus()
        await pilot.press("enter")
        await pilot.pause()
        assert form.query_one(f"#field-{key}", Input).value == symbol.value
        assert form.query_one("#error-version").display
        assert not form.query_one("#advanced-options", Collapsible).collapsed
        assert not form.query_one("#request-10", Collapsible).collapsed
        assert form.has_pending()
        await pilot.press("escape")
        assert isinstance(app.screen, Confirm)
        await pilot.press("escape")
        assert app.screen is form and form.has_pending()


@pytest.mark.asyncio
@pytest.mark.parametrize("size", [(80, 24), (120, 32), (160, 48)])
async def test_long_confirmation_keeps_actions_at_bottom(size: tuple[int, int]) -> None:
    app = DialogHarness()
    async with app.run_test(size=size) as pilot:
        confirmation = Confirm(
            "Review configuration changes",
            "\n".join(f"Resources · setting {index}: old → new" for index in range(70)),
            preview="[resources]\nthreads = 4",
            confirm_label="Apply changes",
        )
        await app.push_screen(confirmation)
        await pilot.pause()
        dialog = confirmation.query_one(".confirmation")
        footer = confirmation.query_one(".buttons")
        body = confirmation.query_one(".confirm-scope", VerticalScroll)
        assert dialog.region.bottom <= size[1]
        assert body.region.bottom == footer.region.y
        assert footer.region.bottom == dialog.content_region.bottom
        body.focus()
        await pilot.press("pagedown")
        await pilot.pause(0.3)
        assert body.scroll_y > 0
        assert await pilot.click("#confirm-preview")
        preview = app.screen
        assert isinstance(preview, Detail) and preview.value_text == "[resources]\nthreads = 4"
        await pilot.press("escape")
        assert await pilot.click("#back")
        assert app.screen is not confirmation


@pytest.mark.asyncio
async def test_setting_details_wrap_actions_at_narrow_width() -> None:
    app = DialogHarness()
    async with app.run_test(size=(80, 24)) as pilot:
        detail = Detail(
            "Setting help · resources.memory",
            {"Type": "Bytes", "Constraints": "Non-negative memory budget"},
            actions={"Full documentation": lambda: None, "Copy field path": lambda: None},
        )
        await app.push_screen(detail)
        await pilot.pause()
        back = detail.query_one("#back", Button)
        copy_path = detail.query_one("#detail-1", Button)
        footer = detail.query_one(".detail-actions")
        assert copy_path.region.y > back.region.y
        assert footer.region.contains_region(copy_path.region)
        await pilot.resize_terminal(120, 32)
        await pilot.pause()
        assert copy_path.region.y == back.region.y
        assert footer.region.contains_region(copy_path.region)
        assert await pilot.click(copy_path)
        assert app.screen is not detail
