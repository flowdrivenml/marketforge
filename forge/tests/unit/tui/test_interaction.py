"""Exact clipboard values and workflow handoffs in Textual."""

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import cast

import pytest
from rich.console import Console
from textual.widgets import Button, Checkbox, Input, Select, Static

from forge.historical.archives.storage.files import disk
from forge.historical.archives.templates import ACQUISITION_TEMPLATE
from forge.historical.pipelines.config import load_recipe
from forge.shared.config.models import AppSettings
from forge.shared.paths import ProjectPaths
from forge.startup.context import ApplicationContext
from forge.tui import clipboard, market_recipes
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
@pytest.mark.parametrize(
    ("family", "result"),
    [
        ("acquisition", "save"),
        ("pipeline", "save"),
        ("pipeline", "external edit"),
        ("acquisition", "duplicate"),
        ("acquisition", "cancel"),
        ("pipeline", "empty"),
        ("acquisition", "registry change"),
    ],
)
async def test_market_existing_recipe(
    tmp_path: Path, family: str, result: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def invoke(
        key: str, v: Values, ctx: ApplicationContext, progress: Progress, edit: Edit
    ) -> object:
        assert key == "markets list"
        return {
            "instruments": [
                {
                    "id": 103,
                    "symbol": "BNBUSD_270326",
                    "reference": "binance.inverse.BNBUSD_270326",
                    "exchange": "binance",
                    "instrument_type": "future",
                    "market_category": "inverse",
                }
            ]
        }

    ctx = context(tmp_path)
    directory = ctx.paths.workflow_dir(family)
    path = directory / "existing.toml"
    original = ACQUISITION_TEMPLATE
    if result == "duplicate":
        original = original.replace('"bybit"', '"binance"').replace('"perpetual"', '"future"')
        original = original.replace('market_category = "linear"', 'market_category = "inverse"')
        original = original.replace('"BTCUSDT"', '"BNBUSD_270326"')
    if family == "pipeline":
        original += (
            '\n[normalize]\nranking_policy = "custom"\n[merge]\ntimeline_ordering = "causal"\n'
            '[convert]\ntarget_quote = "USD"\nmax_staleness_ns = 200\n'
            'reference_stream_ids = ["reference.trades"]\n'
        )
    if result != "empty":
        directory.mkdir(parents=True)
        path.write_text(original)
        (directory / "bad.toml").write_text("invalid = true")
    app = ForgeApp(ctx, invoke=invoke)
    monkeypatch.setattr(app, "check_registry", lambda: None)
    registry = "00000000-0000-4000-8000-000000000001"

    async def verify_registry(_root: Path) -> str:
        return registry

    monkeypatch.setattr("forge.tui.app.selected_registry_id", verify_registry)
    app.database_id = registry
    app.initial_area, app.initial_view = "Markets", "Instruments"
    async with app.run_test(size=(72, 27)) as pilot:
        await pilot.pause(0.3)
        table = app.query_one("#results", Results)
        y = table.styles.gutter.top + table.header_height
        x = table.styles.gutter.left + table.render_line(table.header_height).text.index("BNBUSD")
        await pilot.mouse_down(table, offset=(x, y))
        await pilot.mouse_up(table, offset=(x + 2, y))
        await pilot.press("ctrl+y")
        assert app.clipboard == "BNB"
        await pilot.click(table, offset=(x, y), button=3)
        assert isinstance(app.screen, ActionMenu)
        assert app.screen.menu_anchor is not None
        assert app.screen.heading == "BNBUSD_270326 · Binance"
        assert app.screen.actions == ("Add to Acquisition", "Add to Pipeline", "Inspect")
        await pilot.press("down")
        assert app.focused is not None and app.focused.id == "menu-1"
        await pilot.press("escape")
        assert app.screen == app.root_screen
        await pilot.press("f2")
        if family == "pipeline":
            await pilot.press("down")
        await pilot.press("enter")
        await pilot.pause()
        if result == "empty":
            assert isinstance(app.screen, ActionMenu)
            assert app.screen.actions == ("Open Workflows", "Cancel")
            await pilot.press("escape")
            assert not path.exists()
            return
        assert isinstance(app.screen, market_recipes.MarketRecipeDialog)
        dialog = app.screen
        select = cast(Select[str], dialog.query_one("#market-recipe-target", Select))
        select.value = "bad"
        await pilot.pause()
        assert "Incompatible" in str(dialog.query_one("#market-recipe-destination", Static).content)
        save = dialog.query_one("#market-recipe-save", Button)
        assert save.disabled
        dialog.query_one("#market-recipe-search", Input).value = "exist"
        await pilot.pause()
        select.value = "existing"
        await pilot.pause()
        assert dialog.destination and dialog.destination.path == path
        assert save.disabled
        assert dialog.query_one("#order_book_l2", Checkbox).disabled
        assert not dialog.query_one("#trades", Checkbox).value
        dialog.query_one("#trades", Checkbox).focus()
        await pilot.press("space")
        preview = str(dialog.query_one("#market-recipe-preview", Static).content)
        if result == "duplicate":
            assert "No changes required" in preview
            assert save.disabled
        else:
            assert "+ BNBUSD_270326 · Trades · Binance · Future / Inverse" in preview
            if result == "external edit":
                original += "\n# external edit\n"
                path.write_text(original)
            if result == "registry change":
                app.database_id = "00000000-0000-4000-8000-000000000002"
            await pilot.click("#market-recipe-cancel" if result == "cancel" else save)
            await pilot.pause()
            if result in {"external edit", "registry change"}:
                assert app.screen is dialog
                assert "changed" in str(dialog.query_one("#form-error", Static).content)
                assert dialog.query_one("#trades", Checkbox).value
        if result != "save":
            assert path.read_text() == original
            return
        assert isinstance(app.screen, Inspection)
        assert app.screen.heading == "Recipe updated"
        assert app.controller.current is None
        from forge.historical.archives.config import load_acquisition

        if family == "pipeline":
            before = load_recipe(original.encode())
            saved = load_recipe(path.read_bytes())
            assert saved.model_dump(exclude={"requests"}) == before.model_dump(exclude={"requests"})
        else:
            saved = load_acquisition(path)
        assert len(saved.requests) == 2
        assert saved.requests[0].scope.identifier == "BTCUSDT"
        assert saved.requests[1].model_dump(exclude_none=True) == {
            "exchange": "binance",
            "instrument_type": "future",
            "market_category": "inverse",
            "data_type": "trades",
            "scope": {"kind": "instrument", "identifier": "BNBUSD_270326"},
        }
        assert set(p.name for p in directory.glob("*.toml")) == {"bad.toml", "existing.toml"}


@pytest.mark.asyncio
async def test_conversion_handoff_and_clipboard(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from forge.historical.conversion.recipes import PublicationSource

    async def selected_source(_paths: ProjectPaths, identity: int) -> PublicationSource:
        assert identity == 826
        return PublicationSource.model_validate(
            {
                "kind": "publication",
                "registry_publication_id": identity,
                "engine_publication_id": "2039158607473567512",
                "registry_id": "dd917334-0000-0000-0000-000000000000",
                "environment": "testing",
                "interval": {"start_ns": 1, "end_ns": 2},
                **dict.fromkeys(
                    ("manifest_sha256", "processing_job_sha256", "publication_checksums_sha256"),
                    "f" * 64,
                ),
            }
        )

    monkeypatch.setattr(
        "forge.historical.conversion.evidence.inspect_publication_source", selected_source
    )
    app = ForgeApp(context(tmp_path))
    row = Row("826", "2039158607473567512", "Publication", "merge", {"operation": "merge"})
    async with app.run_test(size=(120, 35)) as pilot:
        await app.switch("Workflows", "Conversion")
        app.open_action(BY_KEY["convert new"], None, {"route": "publication", "source": row.id})
        await pilot.pause()
        assert isinstance(app.screen, Form)
        assert app.screen.query_one("#field-source", Input).value == "826"
        app.screen.query_one("#field-reference", Input).value = "identity"
        app.screen.query_one("#field-quote", Input).value = "USDT"
        await pilot.click("#submit")
        await pilot.pause()
        assert isinstance(app.screen, Form)
        assert app.screen.query_one("#field-interval--start_ns--time", Input).disabled
        await pilot.click("#submit")
        await pilot.pause()
        assert isinstance(app.screen, Confirm)
        await pilot.click("#confirm")
        await pilot.pause()
        assert isinstance(app.screen, Inspection)
        assert (app.context.paths.workflow_dir("conversion") / "identity.toml").is_file()
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
        await app.switch("Workflows", "Normalization")
        app.run_worker(app.author_exact_normalization((int(source.id),)))
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


@pytest.mark.asyncio
async def test_market_menus_follow_entity_and_exact_scope(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    snapshot = "00000000-0000-4000-8000-000000000023"
    supported = {
        "id": 110,
        "symbol": "ETHUSDT",
        "reference": "bybit.linear.ETHUSDT",
        "exchange": "bybit",
        "instrument_type": "perpetual",
        "market_category": "linear",
    }
    unsupported = {
        **supported,
        "id": 111,
        "symbol": "ETH-USDT-SWAP",
        "reference": "okx.linear.ETH-USDT-SWAP",
        "exchange": "okx",
    }

    async def invoke(
        key: str, v: Values, ctx: ApplicationContext, progress: Progress, edit: Edit
    ) -> object:
        if key == "markets rank list":
            return {"rankings": [{"id": snapshot, "policy_name": "turnover", "included_count": 2}]}
        return {"instruments": [supported, unsupported]}

    ctx = context(tmp_path)
    policies = ctx.paths.config_dir / "markets/rankings"
    policies.mkdir(parents=True)
    (policies / "turnover.toml").write_text('name = "fixture"\n')
    app = ForgeApp(ctx, invoke=invoke)
    app.initial_area = "Markets"
    monkeypatch.setattr(app, "check_registry", lambda: None)
    app.database_id = "00000000-0000-4000-8000-000000000001"
    async with app.run_test(size=(52, 23)) as pilot:
        await pilot.pause(0.3)
        table = app.query_one("#results", Results)
        table.focus()
        table.move_cursor(row=1)
        await pilot.press("space")
        first = table.styles.gutter.top + table.header_height
        await pilot.click(table, offset=(12, first), button=3)
        assert isinstance(app.screen, ActionMenu)
        assert app.screen.actions == ("Add to Acquisition", "Add to Pipeline", "Inspect")
        assert app.screen.heading == "ETHUSDT · Bybit"
        identity = market_recipes.instrument_from_row(app.state.rows["bybit.linear.ETHUSDT"])
        assert identity is not None
        assert identity.request("order_book_l2").scope.identifier == "ETHUSDT"
        await pilot.press("escape")
        await pilot.click(table, offset=(12, first + 1), button=3)
        assert isinstance(app.screen, ActionMenu)
        assert app.screen.actions == ("Add to Acquisition", "Add to Pipeline", "Inspect")
        await pilot.press("escape")
        for view, expected in (
            ("Rankings", {"Inspect", "View Ranking Results", "Copy Full Identifier"}),
            ("Policies", {"Inspect", "Edit TOML", "Validate"}),
        ):
            await app.switch("Markets", view)
            await pilot.pause(0.3)
            await pilot.click(table, offset=(12, first), button=3)
            assert isinstance(app.screen, ActionMenu)
            assert set(app.screen.actions) == expected
            await pilot.press("escape")


@pytest.mark.asyncio
async def test_catalogue_and_retained_run_row_actions(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import json

    ctx = context(tmp_path)
    run_id = "11111111-1111-4111-8111-111111111111"
    run = ctx.paths.runs_dir / run_id
    run.mkdir(parents=True)
    (run / "report.json").write_text(
        json.dumps(
            {
                "kind": "historical_pipeline",
                "state": "complete",
                "name": "fixture",
                "started_at": "2026-11-20T00:00:00Z",
            }
        )
    )

    async def invoke(
        key: str, v: Values, ctx: ApplicationContext, progress: Progress, edit: Edit
    ) -> object:
        assert key == "catalog instrument list"
        return {
            "instruments": [
                {
                    "id": 20,
                    "identity_key": "bybit/spot/spot/BTCUSDT",
                    "symbol": "BTCUSDT",
                    "exchange": "bybit",
                    "instrument_type": "spot",
                    "market_category": "spot",
                }
            ]
        }

    app = ForgeApp(ctx, invoke=invoke)
    monkeypatch.setattr(app, "check_registry", lambda: None)
    app.initial_area, app.initial_view = "Catalogue", "Instruments"
    async with app.run_test(size=(95, 30)) as pilot:
        await pilot.pause(0.3)
        table = app.query_one("#results", Results)
        table.focus()
        await pilot.press("f2")
        assert isinstance(app.screen, ActionMenu)
        assert app.screen.actions == ("Inspect", "View Specifications", "Copy Full Identifier")
        await pilot.press("escape")
        await app.switch("Runs", "Retained")
        await pilot.pause(0.3)
        table.focus()
        await pilot.press("f2")
        assert isinstance(app.screen, ActionMenu)
        actions = app.screen.actions
        assert {"Inspect", "Open Report", "Open Logs", "Resume Pipeline"} <= set(actions)
        await pilot.click(f"#menu-{actions.index('Copy Full Identifier')}")
        assert isinstance(app.screen, Detail)
        await pilot.click("#copy")
        assert app.clipboard == run_id
