"""Exercise the form through the same models and managed files as Forge."""

import os
import tomllib
from pathlib import Path
from typing import cast

import pytest
from rich.console import Console
from textual.app import App, ComposeResult
from textual.containers import VerticalScroll
from textual.widgets import Checkbox, Input, Select, Static

from forge.catalog.models import Specification
from forge.historical.archives.config import load_acquisition
from forge.historical.pipelines.config import PipelineRecipe, load_recipe
from forge.shared.config.models import AppSettings
from forge.shared.paths import ProjectPaths
from forge.startup.context import ApplicationContext
from forge.tui.actions import BY_KEY, Action, Values
from forge.tui.app import ForgeApp
from forge.tui.catalogue import declaration_fields, validate_declaration
from forge.tui.design import THEME
from forge.tui.inspection import Inspection
from forge.tui.recipe_forms import RecipeForm, update_recipe
from forge.tui.screens import Confirm, Detail, EntityChoices, EntityPicker, Form, StreamOrder

START = "2026-10-27T00:00:00Z"
END = "2026-10-28T00:00:00Z"


def text(form: Form, key: str, value: str) -> None:
    form.query_one(f"#field-{key}", Input).value = value


def choose(form: Form, key: str, value: str) -> None:
    cast(Select[str], form.query_one(f"#field-{key}", Select)).value = value


def snapshot(app: ForgeApp, name: str) -> None:
    destination = os.getenv("MARKETFORGE_FORM_SNAPSHOTS")
    if destination:
        Path(destination, f"{name}.svg").write_text(app.export_screenshot(title=name))


@pytest.mark.asyncio
@pytest.mark.parametrize("width,height", [(80, 24), (100, 30), (120, 32), (160, 48)])
async def test_acquisition_form_review_and_save(tmp_path: Path, width: int, height: int) -> None:
    class Workspace(ForgeApp):
        CSS_PATH = Path(__file__).resolve().parents[3] / "tui/workspace.tcss"

        async def search_entities(self, key: str, search: str, values: Values) -> EntityChoices:
            assert key == "requests--0--scope--identifier"
            assert values["requests--0--exchange"] == "bybit"
            assert values["requests--0--instrument_type"] == "perpetual"
            return (("BTCUSDT · Bybit · Perpetual / Linear", "BTCUSDT"),)

    paths = ProjectPaths(tmp_path, tmp_path / "data")
    app = Workspace(ApplicationContext(AppSettings(), paths, None, Console(), Console()))
    async with app.run_test(size=(width, height)) as pilot:
        app.open_action(BY_KEY["acquisition new"])
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, Form)
        assert not screen.query_one("#form-error").display
        assert "blank" in str(screen.query_one("#form-progress", Static).content)
        if width == 80:
            reference = screen.query_one("#field-reference", Input)
            reference.focus()
            await pilot.press("tab")
            assert screen.focused is not reference
            await pilot.press("shift+tab")
            assert screen.focused is reference
            assert await pilot.click("#help-interval--start")
            await pilot.pause()
            assert isinstance(app.screen, Detail) and "ISO 8601" in app.screen.value_text
            assert await pilot.click("#detail-0")
            await pilot.pause()
            assert isinstance(app.screen, Detail)
            assert "historical-data/#1-create-a-recipe" in app.screen.value_text
            await pilot.press("escape")
            assert app.screen is screen
        snapshot(app, f"{width}x{height}-incomplete")
        assert await pilot.click("#form-validate")
        assert screen.focused is screen.query_one("#field-reference")
        assert screen.query_one("#error-reference").display
        snapshot(app, f"{width}x{height}-invalid")
        text(screen, "reference", "pilot")
        text(screen, "interval--start", START)
        text(screen, "interval--end", END)
        choose(screen, "requests--0--exchange", "bybit")
        choose(screen, "requests--0--instrument_type", "perpetual")
        choose(screen, "requests--0--market_category", "linear")
        choose(screen, "requests--0--data_type", "trades")
        choose(screen, "requests--0--scope--kind", "instrument")
        screen.query_one(".fields", VerticalScroll).scroll_end(animate=False, immediate=True)
        await pilot.pause()
        assert await pilot.click("#find-requests--0--scope--identifier")
        await pilot.pause()
        picker = app.screen
        assert isinstance(picker, EntityPicker)
        await pilot.press("enter", "down", "enter")
        await pilot.pause()
        assert await pilot.click("#entity-use")
        await pilot.pause()
        assert screen.query_one("#field-requests--0--scope--identifier", Input).value == "BTCUSDT"
        assert await pilot.click("#form-validate")
        assert "Configuration validated" in str(screen.query_one("#form-progress", Static).content)
        snapshot(app, f"{width}x{height}-valid")
        assert await pilot.click("#submit")
        await pilot.pause()
        assert isinstance(app.screen, Confirm)
        assert "BTCUSDT" in app.screen.explanation
        await pilot.press("escape")
        assert app.screen is screen
        if width == 80:
            await pilot.press("escape")
            assert isinstance(app.screen, Confirm) and "Discard" in app.screen.title_text
            await pilot.press("escape")
            assert app.screen is screen
        assert await pilot.click("#submit")
        assert await pilot.click("#confirm")
        await pilot.pause()
        assert isinstance(app.screen, Inspection)
        assert app.screen.heading == "Acquisition · pilot", app.screen.evidence
    path = paths.acquisition_config_dir / "pilot.toml"
    config = load_acquisition(path)
    assert config.requests[0].scope.identifier == "BTCUSDT"
    assert config.requests[0].instrument_type == "perpetual"
    assert "version" not in config.model_fields_set


@pytest.mark.asyncio
async def test_pipeline_toggle_preserves_pending_values_and_writes_only_active_options() -> None:
    adapter = RecipeForm(PipelineRecipe, {}, new=True)
    fields, values = adapter.build()

    async def validate(values: Values) -> dict[str, str]:
        return adapter.candidate(values)[1]

    class Harness(App[None]):
        CSS_PATH = Path(__file__).resolve().parents[3] / "tui/workspace.tcss"

        def compose(self) -> ComposeResult:
            yield Static("Workspace")

    app = Harness()
    app.register_theme(THEME)
    app.theme = THEME.name
    async with app.run_test(size=(100, 30)) as pilot:
        screen = Form(
            Action("Pipeline", "Workflows", fields),
            values,
            local=True,
            validate=validate,
            expand=adapter.add,
        )
        app.push_screen(screen)
        await pilot.pause()
        assert not screen.query_one("#row-convert--target_quote").display
        conversion = screen.query_one("#field-convert", Checkbox)
        conversion.value = True
        await pilot.pause()
        assert screen.query_one("#row-convert--target_quote").display
        assert "*" in str(screen.query_one("#label-convert--target_quote", Static).content)
        text(screen, "convert--target_quote", "USDT")
        conversion.value = False
        await pilot.pause()
        assert not screen.query_one("#row-convert--target_quote").display
        conversion.value = True
        await pilot.pause()
        assert screen.query_one("#field-convert--target_quote", Input).value == "USDT"
        conversion.value = False
        text(screen, "interval--start", START)
        text(screen, "interval--end", END)
        choose(screen, "requests--0--exchange", "bybit")
        choose(screen, "requests--0--instrument_type", "spot")
        choose(screen, "requests--0--market_category", "spot")
        choose(screen, "requests--0--data_type", "trades")
        choose(screen, "requests--0--scope--kind", "instrument")
        text(screen, "requests--0--scope--identifier", "BTCUSDT")
        assert await pilot.click("#form-validate")
        assert "Configuration validated" in str(screen.query_one("#form-progress", Static).content)
        assert await pilot.click("#submit")
        await pilot.pause()


def test_specification_contract_and_decimal() -> None:
    fields = {field.key: field for field in declaration_fields("specs")}
    payload: Values = {
        "instrument": "bybit.perpetual.linear.BTCUSDT",
        "quantity_type": "contracts",
        "contract_value": "0.000000000000000001",
        "contract_value_asset": "BTC",
        "tick_size": "0.000000000000000001",
        "as_of": START,
        "provenance": "Fixture",
    }
    field = fields["contract_value_asset"]
    assert field.required_when and field.required_when(payload)
    assert field.visible_when and field.visible_when(payload)
    assert not field.visible_when({**payload, "instrument": "bybit.spot.spot.BTCUSDT"})
    assert validate_declaration("specs", payload) == {}
    assert str(Specification.model_validate(payload).tick_size) == "1E-18"
    assert validate_declaration("specs", {**payload, "tick_size": "bad"})["tick_size"]


def test_recipe_patch_preserves_inactive_sections_and_model_parity() -> None:
    original = b"""# keep this
version = 1
[interval]
start = 2026-10-27T00:00:00Z
end = 2026-10-28T00:00:00Z
[[requests]]
exchange = "bybit"
instrument_type = "spot"
market_category = "spot"
data_type = "trades"
scope = {kind = "instrument", identifier = "BTCUSDT"}
"""
    existing = tomllib.loads(original.decode())
    adapter = RecipeForm(PipelineRecipe, existing)
    _, pending = adapter.build()
    pending["version"] = 1
    candidate, issues = adapter.candidate(pending)
    assert issues == {}
    assert candidate == existing
    candidate["merge"] = {"timeline_ordering": "causal"}
    rendered = update_recipe(original, existing, candidate)
    assert rendered.startswith("# keep this")
    actual = load_recipe(rendered.encode())
    expected = PipelineRecipe.model_validate(candidate)
    assert actual == expected
    assert actual.model_fields_set == expected.model_fields_set
    assert actual.convert is None


@pytest.mark.asyncio
async def test_pipeline_authoring_adds_ordered_requests_and_reviews_optional_stage(
    tmp_path: Path,
) -> None:
    paths = ProjectPaths(tmp_path, tmp_path / "data")
    app = ForgeApp(ApplicationContext(AppSettings(), paths, None, Console(), Console()))
    async with app.run_test(size=(100, 30)) as pilot:
        app.open_action(BY_KEY["pipeline new"])
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, Form)
        text(screen, "reference", "staged")
        text(screen, "interval--start", START)
        text(screen, "interval--end", END)
        controls = {
            "exchange": "bybit",
            "instrument_type": "spot",
            "market_category": "spot",
            "scope--kind": "instrument",
        }
        for key, value in controls.items():
            choose(screen, f"requests--0--{key}", value)
        choose(screen, "requests--0--data_type", "trades")
        text(screen, "requests--0--scope--identifier", "BTCUSDT")
        screen.query_one(".fields", VerticalScroll).scroll_end(animate=False, immediate=True)
        await pilot.pause()
        assert await pilot.click("#form-add")
        await pilot.pause()
        assert screen.query_one("#field-reference", Input).value == "staged"
        for key, value in controls.items():
            choose(screen, f"requests--1--{key}", value)
        choose(screen, "requests--1--data_type", "order_book_l2")
        text(screen, "requests--1--scope--identifier", "BTCUSDT")
        screen.query_one("#field-convert", Checkbox).value = True
        text(screen, "convert--target_quote", "USDC")
        text(screen, "convert--max_staleness_ns", "900000000")
        await pilot.pause()
        assert await pilot.click("#submit")
        assert isinstance(app.screen, Confirm)
        assert "Acquisition → Normalization → Merge → Conversion" in app.screen.explanation
        assert await pilot.click("#confirm")
        await pilot.pause()
        assert isinstance(app.screen, Inspection) and app.screen.heading == "Pipeline · staged"
    actual = load_recipe((paths.pipeline_config_dir / "staged.toml").read_bytes())
    assert [request.data_type for request in actual.requests] == ["trades", "order_book_l2"]
    assert actual.convert is not None and actual.convert.target_quote == "USDC"
    assert actual.model_fields_set == {"version", "interval", "requests", "convert"}


@pytest.mark.asyncio
async def test_merge_stream_order_uses_exact_ids_with_mouse_and_keyboard(tmp_path: Path) -> None:
    app = ForgeApp(
        ApplicationContext(
            AppSettings(), ProjectPaths(tmp_path, tmp_path / "data"), None, Console(), Console()
        )
    )
    saved: list[tuple[str, ...] | None] = []
    async with app.run_test(size=(80, 24)) as pilot:
        ordering = StreamOrder(
            (
                ("BTCUSDT · Trades", "stream/one"),
                ("BTCUSDT · Depth", "stream/two"),
                ("BTCUSDC · Trades", "stream/three"),
            )
        )
        app.push_screen(ordering, saved.append)
        await pilot.pause()
        await pilot.press("ctrl+down")
        assert ordering.entries[1][1] == "stream/one"
        assert await pilot.click("#order-down")
        assert await pilot.click("#order-use")
        assert saved == [("stream/two", "stream/three", "stream/one")]


@pytest.mark.parametrize(
    "family,source",
    [
        (
            "normalize",
            {
                "registry_id": "dd917334-0000-0000-0000-000000000000",
                "archive_pins": [{"archive_id": 504, "selection_sha256": "a" * 64}],
            },
        ),
        (
            "merge",
            {
                "dataset_ids": [37, 12],
                "start_ns": 100,
                "end_ns": 200,
                "timeline_ordering": "causal",
            },
        ),
        (
            "convert",
            {
                "version": 1,
                "source": {
                    "kind": "publication",
                    "registry_publication_id": 9876,
                    "engine_publication_id": "18446744073709551615",
                    "registry_id": "dd917334-0000-0000-0000-000000000000",
                    "environment": "testing",
                    "manifest_sha256": "a" * 64,
                    "processing_job_sha256": "b" * 64,
                    "publication_checksums_sha256": "c" * 64,
                    "interval": {"start_ns": 100, "end_ns": 200},
                },
                "interval": {"start_ns": 100, "end_ns": 200},
                "selection": {"whole_publication": True},
                "conversion": {"target_quote": "USDC", "max_staleness_ns": 2000},
            },
        ),
        (
            "acquisition import",
            {
                "root": "/tmp/vendor",
                "files": ["source,part-1.jsonl"],
                "source_key": "vendor-recording",
                "identity": {
                    "exchange": "bybit",
                    "market_category": "linear",
                    "instrument_type": "perpetual",
                    "symbol": "BTCUSDT",
                },
                "data_type": "trades",
            },
        ),
        (
            "acquisition register",
            {
                "registration": {"version": 1, "name": "fixture"},
                "source": {
                    "key": "fixture",
                    "provider": "Recording provider",
                    "product": "Trade recording",
                    "root": "/tmp/vendor",
                    "provenance": "Contract fixture",
                },
                "inputs": [
                    {
                        "instrument": "bybit.spot.spot.BTCUSDT",
                        "profile": "bybit.spot.spot.trades",
                        "content": "trades",
                        "files": ["feed.csv"],
                    }
                ],
                "coverage": {"start": START, "end": END},
            },
        ),
    ],
)
def test_existing_recipe_form_round_trips_service_models(
    family: str, source: dict[str, object]
) -> None:
    from forge.shared.config.toml import dumps
    from forge.tui.recipe_forms import model_for

    if family == "acquisition register":
        from datetime import datetime

        source = {
            **source,
            "coverage": {
                "start": datetime.fromisoformat(START),
                "end": datetime.fromisoformat(END),
            },
        }
    data = tomllib.loads(dumps(source))
    model = model_for(family, data)
    assert model is not None
    baseline = model.model_validate(data)
    adapter = RecipeForm(model, data)
    fields, pending = adapter.build()
    values = {
        field.key: field.parse(pending[field.key])
        for field in fields
        if not field.visible_when or field.visible_when(pending)
    }
    candidate, issues = adapter.candidate(values)
    assert issues == {}
    assert candidate == data
    parsed = model.model_validate(tomllib.loads(dumps(candidate)))
    assert parsed == baseline
    assert parsed.model_fields_set == baseline.model_fields_set
