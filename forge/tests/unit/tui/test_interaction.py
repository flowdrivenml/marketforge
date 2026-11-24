"""Exact clipboard values and workflow handoffs in Textual."""

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest
from rich.console import Console
from textual.widgets import Input

from forge.historical.archives.storage.files import disk
from forge.shared.config.models import AppSettings
from forge.shared.paths import ProjectPaths
from forge.startup.context import ApplicationContext
from forge.tui import clipboard
from forge.tui.actions import BY_KEY, Values
from forge.tui.app import ForgeApp
from forge.tui.inspection import ActionMenu, Inspection
from forge.tui.presentation import Results
from forge.tui.resources import Row
from forge.tui.screens import Confirm, Detail, Form
from forge.tui.workflows import Edit, Progress


def test_clipboard_exactness_and_provider_order(monkeypatch: pytest.MonkeyPatch) -> None:
    path = "/marketforge/" + "long " * 120 + "[BTC]–現物.parquet"
    text = clipboard.plain(
        {
            "registry ID": 2039158607473567512,
            "observed": 1793059200093200000,
            "tick": Decimal("0.0000000100"),
            "path": path,
            "diagnostic": "\x1b[31m[red]not markup\nsecond line\x1b[0m",
            "password": "secret-value",
        }
    )
    for expected in (
        "2039158607473567512",
        "1793059200093200000",
        "0.0000000100",
        path,
        "[red]not markup",
    ):
        assert expected in text
    assert "\x1b" not in text
    assert "secret-value" not in text

    def installed(executable: str) -> str:
        return executable

    monkeypatch.setattr(clipboard.shutil, "which", installed)
    assert clipboard.providers(
        {"WAYLAND_DISPLAY": "wayland-0", "DISPLAY": ":0", "TMUX": "test"}, "linux"
    ) == [
        ("wl-copy",),
        ("xclip", "-selection", "clipboard"),
        ("xsel", "--clipboard", "--input"),
        ("tmux", "load-buffer", "-w", "-"),
    ]
    assert clipboard.providers({}, "darwin") == [("pbcopy",)]


def context(path: Path) -> ApplicationContext:
    return ApplicationContext(
        AppSettings(), ProjectPaths(path, path / "data"), None, Console(), Console(), True
    )


@pytest.mark.asyncio
async def test_market_basket_mouse_and_keyboard(tmp_path: Path) -> None:
    async def invoke(
        key: str, v: Values, ctx: ApplicationContext, progress: Progress, edit: Edit
    ) -> object:
        if key != "markets list":
            return {}
        offset = int(v.get("offset", 0))
        return {
            "instruments": [
                {
                    "id": offset + 1,
                    "symbol": f"BTCUSDT{offset}",
                    "reference": f"bybit.perpetual.linear.BTCUSDT{offset}",
                    "exchange": "bybit",
                    "instrument_type": "perpetual",
                    "market_category": "linear",
                }
            ],
            "has_more": offset == 0,
        }

    app = ForgeApp(context(tmp_path), invoke=invoke)
    async with app.run_test(size=(120, 35)) as pilot:
        await pilot.click("#area-3")
        await pilot.pause(0.3)
        table = app.query_one("#results", Results)
        table.focus()
        await pilot.press("space", "pagedown")
        await pilot.pause(0.3)
        assert len(app.state.basket) == 1
        await pilot.click(
            "#results",
            offset=(table.styles.gutter.left + 1, table.styles.gutter.top + table.header_height),
        )
        await pilot.pause()
        assert len(app.state.basket) == 2
        assert app.screen == app.root_screen
        await pilot.press("f2")
        await pilot.pause()
        menu = app.screen
        assert isinstance(menu, ActionMenu)
        await pilot.click(f"#menu-{menu.actions.index('Add to acquisition…')}")
        await pilot.pause()
        assert isinstance(app.screen, Form)
        assert "2 instrument(s)" in app.screen.spec.key
        for field, value in (
            ("reference", "from-market"),
            ("start", "2026-10-26T00:00:00Z"),
            ("end", "2026-10-27T00:00:00Z"),
        ):
            app.screen.query_one(f"#field-{field}", Input).value = value
        await pilot.click("#submit")
        await pilot.pause()
        assert isinstance(app.screen, Confirm)
        await pilot.click("#confirm")
        await pilot.pause()
        assert isinstance(app.screen, Inspection)
        from forge.historical.archives.config import load_acquisition

        recipe = await disk(
            load_acquisition, app.context.paths.acquisition_config_dir / "from-market.toml"
        )
        assert [entry.scope.identifier for entry in recipe.requests] == ["BTCUSDT0", "BTCUSDT50"]
        assert all(
            entry.exchange == "bybit" and entry.market_category == "linear"
            for entry in recipe.requests
        )
        assert {"Validate", "Plan", "Map", "Download"} <= set(app.screen.links)


@pytest.mark.asyncio
async def test_conversion_handoff_and_clipboard(tmp_path: Path) -> None:
    calls: list[tuple[str, Values]] = []

    async def invoke(
        key: str, v: Values, ctx: ApplicationContext, progress: Progress, edit: Edit
    ) -> object:
        calls.append((key, dict(v)))
        return {"path": str(tmp_path / "identity.toml"), "source": {"registry_publication_id": 826}}

    app = ForgeApp(context(tmp_path), invoke=invoke)
    row = Row("826", "2039158607473567512", "Publication", "merge", {"operation": "merge"})
    async with app.run_test(size=(120, 35)) as pilot:
        app.publication_workflows(row, (row,))["Convert publication…"]()
        await pilot.pause()
        assert isinstance(app.screen, Form)
        assert app.screen.query_one("#field-source", Input).value == "826"
        app.screen.query_one("#field-reference", Input).value = "identity"
        app.screen.query_one("#field-quote", Input).value = "USDT"
        await pilot.click("#submit")
        await pilot.pause()
        assert isinstance(app.screen, Confirm)
        await pilot.click("#confirm")
        await pilot.pause()
        assert isinstance(app.screen, Inspection)
        assert (
            "convert new",
            {"reference": "identity", "route": "publication", "source": "826", "quote": "USDT"},
        ) in calls
        assert "Prepare" in app.screen.links
        app.copy_value(row.name)
        await pilot.pause()
        assert isinstance(app.screen, Detail)
        await pilot.click("#copy")
        await pilot.pause()
        assert app.clipboard == row.name
    assert (
        BY_KEY["convert new"]
        .command(
            {"route": "publication", "reference": "identity", "source": "826", "quote": "USDT"},
            testing=True,
        )
        .endswith("--testing")
    )


@pytest.mark.asyncio
async def test_environment_switch_reopens_with_no_selections(tmp_path: Path) -> None:
    from dataclasses import replace

    app = ForgeApp(context(tmp_path))
    async with app.run_test(size=(120, 30)) as pilot:
        await app.switch("Data", "Merged")
        app.state.basket["826"] = Row("826", "826", "Publication", "merged", {})
        await pilot.press("ctrl+e")
        await pilot.pause()
        assert isinstance(app.screen, Confirm)
        await pilot.click("#confirm")
    assert app.return_value == "production"
    production = ForgeApp(replace(app.context, testing=False))
    assert production.states == {}
    assert production.database_environment == "production"
    assert production.workspace_header() == "MarketForge Workspace · Overview"
    assert "826" not in production.state.basket


@pytest.mark.asyncio
async def test_invalid_editor_draft_leaves_original(tmp_path: Path) -> None:
    from forge.historical.authoring import create_pipeline

    class TestApp(ForgeApp):
        CSS_PATH = Path(__file__).resolve().parents[3] / "tui/workspace.tcss"

        async def edit_recipe(self, path: Path) -> None:
            await disk(path.write_text, "broken = [")

    app = TestApp(context(tmp_path))
    original = create_pipeline(app.context.paths.pipeline_config_dir, "fixture")
    before = original.read_bytes()
    async with app.run_test(size=(120, 30)) as pilot:
        app.edit_selected(Row("fixture", "fixture", "pipeline", "", {}, "fixture", original))
        await pilot.pause()
        assert isinstance(app.screen, Detail)
        assert original.read_bytes() == before


@pytest.mark.asyncio
async def test_normalization_selection_saves_exact_ids_and_offers_prepare(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import tomllib
    from uuid import uuid4

    from forge.shared.database.connection import is_testing

    app = ForgeApp(context(tmp_path))
    source = Row("509", "BTCUSDT.csv.gz", "trades", "complete", {})
    destination = app.context.paths.normalization_recipe_dir / "selected.toml"
    registry = str(uuid4())

    async def resolve(
        paths: ProjectPaths, archive_ids: tuple[int, ...]
    ) -> tuple[str, list[dict[str, object]]]:
        assert is_testing()
        assert (paths, archive_ids) == (app.context.paths, (509,))
        return registry, [
            {
                "archive_id": 509,
                "selection_sha256": "f" * 64,
                "profile_id": "bybit.spot.spot.trades",
                "profile_sha256": "8" * 64,
            }
        ]

    monkeypatch.setattr("forge.historical.normalization.batch.inspect_exact_archives", resolve)
    async with app.run_test(size=(120, 35)) as pilot:
        app.normalize_selection((source,))
        await pilot.pause()
        app.screen.query_one("#field-reference", Input).value = "selected"
        await pilot.click("#submit")
        await pilot.pause()
        assert isinstance(app.screen, Confirm)
        await pilot.click("#confirm")
        await pilot.pause()
        assert isinstance(app.screen, Inspection)
        assert "Prepare" in app.screen.links
        assert tomllib.loads(destination.read_text())["archive_pins"][0]["archive_id"] == 509


def test_merge_recipe_preserves_explicit_generations(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from forge.historical.archives.config import AcquisitionInterval
    from forge.historical.merge.config import DerivedRecipe
    from forge.tui.workflows import new_merge

    app_context = context(tmp_path)
    interval = AcquisitionInterval.model_validate(
        {
            "start": datetime(2026, 10, 26, tzinfo=UTC),
            "end": datetime(2026, 10, 28, tzinfo=UTC),
        }
    )

    def source(paths: ProjectPaths, reference: str) -> tuple[Path, object, str]:
        from forge.historical.normalization.config import NormalizationRecipe

        assert reference == "bound"
        return (
            paths.normalization_recipe_dir / "bound.toml",
            NormalizationRecipe.model_validate(
                {
                    "interval": interval,
                    "requests": [
                        {
                            "exchange": "bybit",
                            "instrument_type": "perpetual",
                            "market_category": "linear",
                            "data_type": "trades",
                            "scope": {"kind": "instrument", "identifier": "BTCUSDT"},
                        }
                    ],
                }
            ),
            "fixture-fingerprint",
        )

    def discover(paths: ProjectPaths, recipe: DerivedRecipe) -> dict[str, object]:
        assert recipe.selection.publication_ids == [826, 829]
        assert recipe.selection.source == "bound"
        assert recipe.timeline_ordering == "causal"
        assert recipe.stream_ranks == {"bybit.btcusdt.trade": 2}
        assert recipe.interval.end < interval.end
        return {"source": {"sha256": "fixture-fingerprint"}}

    monkeypatch.setattr("forge.tui.workflows.normalization_source", source)
    monkeypatch.setattr("forge.tui.workflows.merge.source_selection", discover)
    new_merge(
        app_context,
        "chosen",
        "bound",
        {
            "publication_ids": "826,829",
            "timeline_ordering": "causal",
            "stream_ranks": "bybit.btcusdt.trade=2",
            "end": "2026-10-27T00:00:00Z",
        },
    )
    assert (app_context.paths.workflow_dir("merge") / "chosen.toml").is_file()


@pytest.mark.asyncio
async def test_application_settings_validate_preview_apply(tmp_path: Path) -> None:
    from textual.widgets import DataTable

    from forge.startup.context import build_context
    from forge.tui.configuration_screen import ConfigurationExplorer, FieldEditor

    (tmp_path / "forge").mkdir()
    (tmp_path / "forge/pyproject.toml").touch()
    (tmp_path / "engine").mkdir()
    path = ProjectPaths(tmp_path, tmp_path / "data").settings_file
    path.parent.mkdir(parents=True)
    original = "[http]\nmax_concurrent = 4 # Retain this comment\n"
    path.write_text(original)
    app = ForgeApp(build_context(project_root=tmp_path, testing=True))
    async with app.run_test(size=(110, 35)) as pilot:
        app.configuration_settings()
        await pilot.pause(0.3)
        assert isinstance(app.screen, ConfigurationExplorer)
        app.screen.query_one("#configuration-search", Input).value = "max_concurrent"
        await pilot.pause()
        app.screen.query_one("#configuration-fields", DataTable).focus()
        await pilot.press("enter")
        await pilot.pause()
        assert isinstance(app.screen, FieldEditor)
        app.screen.query_one("#setting-input", Input).value = "7"
        await pilot.click("#setting-update")
        await pilot.pause(0.2)
        await pilot.click("#apply")
        await pilot.pause(0.2)
        assert isinstance(app.screen, Confirm)
        assert await disk(path.read_text) == original
        await pilot.click("#confirm")
        await pilot.pause(0.2)
        assert isinstance(app.screen, ConfigurationExplorer)
        assert app.context.settings.http.max_concurrent == 7
        assert "max_concurrent = 7 # Retain this comment" in await disk(path.read_text)


def test_retained_log_range_severity_and_complete_export(tmp_path: Path) -> None:
    from forge.orchestration.log_files import export_log, read_lines
    from forge.shared.errors import MarketForgeError

    source = tmp_path / "events.jsonl"
    original = '{"level":"warn","message":"障害"}\r\n{"level":"info","message":"completed"}\n'
    source.write_bytes(original.encode())
    assert read_lines(source, 1, 2) == original
    assert (
        read_lines(source, 1, 2, search="障害", severity="warn")
        == original.splitlines(keepends=True)[0]
    )
    with pytest.raises(MarketForgeError, match="copy limit"):
        read_lines(source, 1, 1, max_bytes=6)
    destination = tmp_path / "export.jsonl"
    export_log(source, destination)
    assert destination.read_bytes() == original.encode()
    with pytest.raises(FileExistsError):
        export_log(source, destination)
    assert destination.read_bytes() == original.encode()
