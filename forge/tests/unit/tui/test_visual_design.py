"""Behavioral visual checks against Textual's actual styled widgets."""

import asyncio
from html import unescape
from pathlib import Path

import pytest
from rich.console import Console
from textual.widgets import Button, Input, Select, Static
from textual.widgets._toast import Toast

from forge.shared.config.models import AppSettings
from forge.shared.paths import ProjectPaths
from forge.startup.context import ApplicationContext
from forge.tui import app as shell
from forge.tui.actions import Action, Field, Values
from forge.tui.app import ForgeApp
from forge.tui.design import success_message
from forge.tui.presentation import Results
from forge.tui.resources import Row
from forge.tui.screens import Confirm, Form


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
@pytest.mark.parametrize("size", [(80, 24), (160, 48)])
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
        assert active.has_class("active")
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
        await pilot.pause()
        assert app.focused is last_tab
        assert last_tab.region.right <= app.query_one("#views").region.right


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
        assert row.has_class("-invalid")
        assert form.query_one("#error-reference").display
        form.query_one("#field-reference", Input).value = "bybit_spot"
        await pilot.pause()
        assert row.has_class("-changed") and not row.has_class("-invalid")
        form.query_one("#form-validate", Button).press()
        async with asyncio.timeout(2):
            while not row.has_class("-validated"):
                await pilot.pause(0.05)
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
