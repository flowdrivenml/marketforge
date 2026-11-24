"""Bounded recipe mutation and interactive table regressions."""

import tomllib
from pathlib import Path

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
        assert app.screen.heading == "b"
        await pilot.press("escape")
        assert app.state.selection == "b"


@pytest.mark.asyncio
async def test_instrument_apply_updates_exact_destination(tmp_path: Path) -> None:
    from textual.widgets import Input

    from forge.tui.screens import Confirm

    paths = ProjectPaths(tmp_path, tmp_path / "data")
    recipe = create_pipeline(paths.pipeline_config_dir, "target")
    other = create_pipeline(paths.pipeline_config_dir, "unrelated")
    retained = other.read_bytes()
    row = Row(
        "one",
        "ETHUSDT",
        "instrument",
        "",
        {
            "exchange": "bybit",
            "instrument_type": "spot",
            "market_category": "spot",
            "symbol": "ETHUSDT",
        },
    )
    app = ForgeApp(ApplicationContext(AppSettings(), paths, None, Console(), Console()))
    async with app.run_test(size=(120, 40)) as pilot:
        app.add_to_recipe(row, "pipeline")
        await pilot.pause()
        app.screen.query_one("#field-reference", Input).value = "target"
        await pilot.click("#submit")
        await pilot.pause()
        assert isinstance(app.screen, Confirm)
        assert str(recipe) in app.screen.explanation
        await pilot.click("#confirm")
        await pilot.pause()
        assert isinstance(app.screen, Inspection)
        requests = tomllib.loads(recipe.read_text())["requests"]
        assert [item["scope"]["identifier"] for item in requests] == ["BTCUSDT", "ETHUSDT"]
        assert other.read_bytes() == retained


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
