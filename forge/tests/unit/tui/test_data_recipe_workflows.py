"""Data actions exercised through the workspace's actual mouse and keyboard routes."""

import tomllib
from pathlib import Path
from uuid import UUID

import pytest
from rich.console import Console
from textual.widgets import Button, Input, Select

from forge.datasets import workspace_inventory
from forge.shared.config.models import AppSettings
from forge.shared.paths import ProjectPaths
from forge.startup.context import ApplicationContext
from forge.tui import data_recipes
from forge.tui.app import ForgeApp
from forge.tui.inspection import ActionMenu, Inspection
from forge.tui.presentation import Results
from forge.tui.screens import Confirm, Detail, EntityPicker, Form

REGISTRY = "00000000-0000-4000-8000-000000000001"
RAW = {
    "archives": ["local:21", "local:22"],
    "symbol": "BTCUSDT",
    "exchange": "Bybit",
    "market_category": "spot",
    "instrument_type": "spot",
    "data_type": "trades",
    "state": "complete",
}


class FixtureApp(ForgeApp):
    """Keep Pilot independent of any user registry."""

    CSS_PATH = Path(__file__).resolve().parents[3] / "tui/workspace.tcss"

    def check_registry(self) -> None:  # pyright: ignore[reportIncompatibleMethodOverride]
        self.database_id = REGISTRY


def app_for(root: Path, view: str) -> FixtureApp:
    context = ApplicationContext(
        AppSettings(), ProjectPaths(root, root / "data"), None, Console(), Console(), True
    )
    app = FixtureApp(context)
    app.initial_area = "Data"
    app.initial_view = view
    return app


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("view", "item", "visible"),
    [
        ("Raw", RAW, "BTCUSDT"),
        (
            "Normalized",
            {
                **RAW,
                "publication": 967,
                "namespace": "n",
                "instrument": 4,
                "stream": "btc.trades",
                "primary_files": [100],
            },
            "BTCUSDT",
        ),
        ("Merged", {"publication": 969, "state": "Registered"}, "969"),
        ("Converted", {"output_ref": "resolved:" + "a" * 64, "state": "Complete"}, "resolved:"),
    ],
)
async def test_data_mouse_selection_and_menu(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    view: str,
    item: dict[str, object],
    visible: str,
) -> None:
    async def stage_page(*_args: object) -> tuple[list[dict[str, object]], bool]:
        return [dict(item)], True

    monkeypatch.setattr(workspace_inventory, "stage_page", stage_page)
    app = app_for(tmp_path, view)
    async with app.run_test(size=(150, 33)) as pilot:
        await pilot.pause(0.3)
        table = app.query_one("#results", Results)
        y = table.styles.gutter.top + table.header_height
        line = table.render_line(table.header_height)
        x = table.styles.gutter.left + line.text.index(visible)
        await pilot.mouse_down(table, offset=(x, y))
        await pilot.mouse_up(table, offset=(x + 2, y))
        assert table.selected_text == visible[:3]
        await pilot.press("ctrl+y")
        assert app.clipboard == visible[:3]
        await pilot.click(table, offset=(x, y), button=3)
        assert isinstance(app.screen, ActionMenu)
        menu = app.screen
        assert menu.actions == (
            "Add to Recipe",
            "Inspect",
            "Copy Selected Text",
            "Copy Row",
            "Copy Identifier",
            "More Actions",
        )
        assert menu.menu_anchor is not None
        await pilot.click("#menu-2")
        assert app.clipboard == visible[:3]
        await pilot.press("f2")
        assert isinstance(app.screen, ActionMenu)
        if view == "Converted":
            await pilot.click("#menu-4")
            assert app.clipboard == item["output_ref"]
            await pilot.press("f2")
            await pilot.click("#menu-0")
            assert isinstance(app.screen, Detail)
            assert "No compatible downstream recipe" in app.screen.value_text
        await pilot.press("escape")
        await pilot.press("pagedown")
        assert table.selected_text == ""


@pytest.mark.asyncio
async def test_raw_group_add_to_existing_and_save(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def stage_page(*_args: object) -> tuple[list[dict[str, object]], bool]:
        return [dict(RAW)], False

    async def inspect(
        _paths: ProjectPaths, ids: tuple[int, ...]
    ) -> tuple[str, list[dict[str, object]]]:
        return REGISTRY, [
            {
                "archive_id": identity,
                "selection_sha256": "a" * 64,
                "profile_id": "bybit.spot.spot.trades",
                "profile_sha256": "b" * 64,
                "source": {"symbols": ["BTCUSDT", "BTCUSDC"]},
            }
            for identity in ids
        ]

    monkeypatch.setattr(workspace_inventory, "stage_page", stage_page)
    monkeypatch.setattr(data_recipes, "inspect_exact_archives", inspect)
    app = app_for(tmp_path, "Raw")
    directory = app.context.paths.normalization_recipe_dir
    directory.mkdir(parents=True)
    path = directory / "existing.toml"
    path.write_text(
        f'# existing comment\nregistry_id = "{UUID(REGISTRY)}"\nranking_policy = "custom"\n'
        f'[[archive_pins]]\narchive_id = 20\nselection_sha256 = "{"a" * 64}"\n'
    )
    async with app.run_test(size=(150, 38)) as pilot:
        await pilot.pause(0.3)
        table = app.query_one("#results", Results)
        await pilot.click(
            table, offset=(8, table.styles.gutter.top + table.header_height), button=3
        )
        await pilot.click("#menu-0")
        assert isinstance(app.screen, Form)
        await pilot.click("#submit")
        assert isinstance(app.screen, ActionMenu)
        await pilot.click("#menu-1")
        assert isinstance(app.screen, EntityPicker)
        search = app.screen.query_one("#entity-query", Input)
        search.value = "exist"
        await pilot.click("#entity-search")
        app.screen.query_one("#entity-result", Select).value = "existing"
        await pilot.pause(0.3)
        await pilot.click("#entity-use")
        assert isinstance(app.screen, Form)
        await pilot.click("#submit")
        assert isinstance(app.screen, Confirm)
        assert "BTCUSDC" in app.screen.explanation
        await pilot.click("#confirm")
        assert isinstance(app.screen, Inspection)
    saved = path.read_text()
    config = tomllib.loads(saved)
    assert "# existing comment" in saved
    assert config["ranking_policy"] == "custom"
    assert [pin["archive_id"] for pin in config["archive_pins"]] == [20, 21, 22]


@pytest.mark.asyncio
async def test_inspection_unicode_and_multiline_selection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rich.cells import cell_len
    from rich.text import Text
    from textual.widgets import Static

    from forge.tui.inspection import RecordTable

    async def stage_page(*_args: object) -> tuple[list[dict[str, object]], bool]:
        return [], False

    monkeypatch.setattr(workspace_inventory, "stage_page", stage_page)
    app = app_for(tmp_path, "Merged")
    async with app.run_test(size=(100, 26)) as pilot:
        await app.push_screen(
            Inspection(
                "File evidence",
                {},
                sections={
                    "Files": [{"Path": "/root/現物 data " + "a" * 56}],
                    "Summary": Text("BTCUSDT\nNanoseconds 1793059200093200000"),
                },
            )
        )
        await pilot.pause()
        table = app.screen.query_one(RecordTable)
        line = table.render_line(table.header_height).text
        x = cell_len(line[: line.index("現")]) + table.gutter.left
        y = table.gutter.top + table.header_height
        await pilot.mouse_down(table, offset=(x, y))
        await pilot.mouse_up(table, offset=(x + 4, y))
        await pilot.press("ctrl+y")
        assert app.clipboard.encode() == "現物 ".encode()
        assert "…" in line
        await pilot.click("#section-1")
        assert not table.selected_text
        summary = app.screen.query_one("#inspection-fields", Static)
        x, y = summary.gutter.left, summary.gutter.top
        await pilot.mouse_down(summary, offset=(x + 3, y))
        await pilot.mouse_up(summary, offset=(x + 5, y + 1))
        await pilot.press("ctrl+y")
        assert app.clipboard.startswith("USDT\nNano")


@pytest.mark.asyncio
@pytest.mark.parametrize("existing", [False, True])
async def test_merged_binds_conversion_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, existing: bool
) -> None:
    from forge.historical.conversion.recipes import PublicationSource, Recipe
    from forge.tui.resources import Row

    source = PublicationSource.model_validate(
        {
            "registry_publication_id": 969,
            "registry_id": REGISTRY,
            "environment": "testing",
            "engine_publication_id": "1234567890123456789",
            "manifest_sha256": "a" * 64,
            "processing_job_sha256": "b" * 64,
            "publication_checksums_sha256": "c" * 64,
            "interval": {"start_ns": 1793059200093200000, "end_ns": 1793059200193200000},
        }
    )

    async def stage_page(*_args: object) -> tuple[list[dict[str, object]], bool]:
        return [{"publication": 969, "state": "Registered"}], False

    async def inspect(_paths: ProjectPaths, identity: int) -> PublicationSource:
        assert identity == 969
        return source

    async def browse(*_args: object) -> object:
        return {"streams": [{"stream_id": "btc.trades", "kind": "trades"}]}

    monkeypatch.setattr(workspace_inventory, "stage_page", stage_page)
    monkeypatch.setattr(data_recipes, "inspect_publication_source", inspect)
    monkeypatch.setattr(data_recipes, "browse", browse)
    from forge.shared.config.toml import dumps

    app = app_for(tmp_path, "Merged")
    directory = app.context.paths.workflow_dir("conversion")
    directory.mkdir(parents=True)
    path = directory / "existing.toml"
    previous = Recipe.model_validate(
        {
            "version": 1,
            "source": source.model_copy(update={"registry_publication_id": 968}),
            "interval": source.interval,
            "selection": {"whole_publication": True},
            "conversion": {"target_quote": "USD", "max_staleness_ns": 1000000000},
            "reference_stream_ids": ["btc.trades"],
        }
    )
    if existing:
        path.write_text(dumps(previous.model_dump(mode="json", exclude_none=True)))
    async with app.run_test(size=(150, 38)) as pilot:
        await pilot.pause(0.3)
        await pilot.press("f2")
        await pilot.click("#menu-0")
        assert isinstance(app.screen, ActionMenu)
        if existing:
            await pilot.click("#menu-1")
            assert isinstance(app.screen, EntityPicker)
            await pilot.pause()
            app.screen.query_one("#entity-result", Select).value = "existing"
            await pilot.pause()
            await pilot.click("#entity-use")
            assert isinstance(app.screen, Confirm), getattr(app.screen, "issues", None)
            assert "Replace publication 968 with 969" in app.screen.explanation
            assert Recipe.model_validate(tomllib.loads(path.read_text())) == previous
            await pilot.click("#confirm")
        else:
            await pilot.click("#menu-0")
        assert isinstance(app.screen, Form)
        if not existing:
            for key, value in (
                ("reference", "existing"),
                ("conversion--target_quote", "USD"),
                ("conversion--max_staleness_ns", "1000000000"),
            ):
                app.screen.query_one(f"#field-{key}", Input).value = value
        app.screen.query_one("#submit", Button).focus()
        await pilot.press("enter")
        await pilot.pause(0.2)
        assert isinstance(app.screen, Confirm)
        if existing:
            assert "Replace publication 968 with 969" in app.screen.explanation
        assert source.engine_publication_id in app.screen.explanation
        await pilot.click("#confirm")
        assert isinstance(app.screen, Inspection)
    recipe = Recipe.model_validate(tomllib.loads(path.read_text()))
    assert recipe.source == source
    if existing:
        assert recipe.model_dump(exclude={"source"}) == previous.model_dump(exclude={"source"})
    row = Row("969", "969", "Publication", "Registered", {}, "969")
    _, _, duplicate = await data_recipes.conversion(
        app.context.paths, (row,), REGISTRY, recipe.model_dump(mode="json", exclude_none=True)
    )
    assert duplicate


@pytest.mark.asyncio
async def test_one_normalized_stream_extends_exact_merge_file_subset(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from forge.historical.merge import service
    from forge.shared.errors import MarketForgeError
    from forge.tui.resources import Row

    first = "bybit.spot.spot.BTCUSDT.trades"
    second = "bybit.spot.spot.BTCUSDC.trades"
    catalog = {
        101: (967, first, 1),
        102: (967, first, 3),
        103: (967, second, 1),
        104: (970, first, 1),
    }
    published = {
        967: [101, 102, 103],
        970: [104],
    }
    observed = 1_793_059_200_093_200_000
    calls: list[list[int]] = []

    async def registry(_root: Path) -> str:
        return REGISTRY

    async def select_inputs(
        _paths: ProjectPaths,
        *,
        ids: list[int],
        expected_publications: list[int],
        include_publication_ids: bool,
    ) -> list[dict[str, object]]:
        assert include_publication_ids
        assert set(ids) <= {
            i for publication in expected_publications for i in published[publication]
        }
        calls.append(ids)
        return [
            {
                "database_id": i,
                "registry_publication_id": catalog[i][0],
                "engine_id": 123456 if catalog[i][0] == 967 else 123457,
                "stream_id": catalog[i][1],
                "content_type": "trades",
                "start_ns": observed + catalog[i][2],
                "end_ns": observed + catalog[i][2],
                "registered_path": f"/existing records/{i}.parquet",
                "memberships": [{"stream_id": catalog[i][1]}],
            }
            for i in ids
        ]

    async def stage_page(*_args: object) -> tuple[list[dict[str, object]], bool]:
        return [
            {
                **RAW,
                "symbol": symbol,
                "publication": 967,
                "namespace": "n",
                "instrument": i,
                "stream": stream,
                "primary_files": files,
                "engine_publication": "123456",
            }
            for i, (symbol, stream, files) in enumerate(
                (("BTCUSDT", first, [101, 102]), ("BTCUSDC", second, [103])), 1
            )
        ], False

    monkeypatch.setattr(workspace_inventory, "stage_page", stage_page)
    monkeypatch.setattr(service, "selected_registry_id", registry)
    monkeypatch.setattr(service, "select_inputs", select_inputs)
    from forge.shared.config.toml import dumps

    app = app_for(tmp_path, "Normalized")
    directory = app.context.paths.workflow_dir("merge")
    directory.mkdir(parents=True)
    path = directory / "stream-only.toml"
    before, _ = await service.publication_recipe(
        app.context.paths, [967], dataset_ids=[101], start_ns=observed, end_ns=observed + 5
    )
    path.write_text(dumps(before.model_dump(mode="json", exclude_none=True)))
    async with app.run_test(size=(150, 38)) as pilot:
        await pilot.pause(0.3)
        await pilot.press("f2")
        await pilot.click("#menu-0")
        assert isinstance(app.screen, ActionMenu)
        await pilot.click("#menu-1")
        assert isinstance(app.screen, EntityPicker)
        await pilot.pause()
        app.screen.query_one("#entity-result", Select).value = "stream-only"
        await pilot.pause()
        await pilot.click("#entity-use")
        assert isinstance(app.screen, Form)
        await pilot.click("#submit")
        assert isinstance(app.screen, Confirm)
        assert first in app.screen.explanation
        assert second not in app.screen.explanation
        await pilot.click("#confirm")
        assert isinstance(app.screen, Inspection)
    config = service.Recipe.model_validate(tomllib.loads(path.read_text()))
    assert config.file_subset and config.publication_ids == [967]
    assert config.dataset_ids == [101, 102]
    assert (config.start_ns, config.end_ns) == (before.start_ns, before.end_ns)
    assert calls and all(ids in ([101], [101, 102]) for ids in calls)
    with pytest.raises(MarketForgeError, match="original verified database identity"):
        await service.reviewed_inputs(
            app.context.paths, config, config.dataset_ids, str(UUID(int=2))
        )
    revision = Row(
        "revision",
        "BTCUSDT",
        "Publication",
        "Registered",
        {"primary_files": [104], "stream": first},
        "970",
    )
    with pytest.raises(MarketForgeError, match="Overlapping registry publications"):
        await data_recipes.merge(
            app.context.paths, (revision,), REGISTRY, config.model_dump(mode="json")
        )
