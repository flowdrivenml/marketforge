"""Settings discovery, review, and conflict handling through Textual."""

import json
import shutil
import subprocess
import tomllib
from pathlib import Path
from typing import Any, Protocol, cast

import pytest
from rich.console import Console
from rich.text import Text
from textual.app import App, ComposeResult
from textual.widgets import Button, DataTable, Input, Select

from forge.shared.config.models import AppSettings
from forge.shared.paths import ProjectPaths
from forge.startup.context import ApplicationContext
from forge.tui.configuration import PAGES, ConfigurationDraft, documentation, editable
from forge.tui.configuration_screen import ConfigurationEditor, ConfigurationExplorer
from forge.tui.design import THEME
from forge.tui.screens import Confirm, Detail

ROOT = Path(__file__).resolve().parents[4]


class ExplorerApp(App[None]):
    CSS_PATH = ROOT / "forge" / "tui" / "workspace.tcss"

    def __init__(self, context: ApplicationContext, component: str) -> None:
        super().__init__()
        self.register_theme(THEME)
        self.theme = THEME.name
        self.context = context
        self.component = component
        self.copied: list[str] = []
        self.applied: list[AppSettings] = []

    def compose(self) -> ComposeResult:
        yield Button("Open settings")

    def on_mount(self) -> None:
        self.push_screen(
            ConfigurationExplorer(
                self.context,
                self.edit,
                self.copied.append,
                self.applied_settings,
                dict,
                self.component,
            )
        )

    async def edit(self, path: Path) -> None:
        raise AssertionError(path)

    def applied_settings(self, settings: AppSettings, path: Path) -> None:
        assert path == self.context.paths.settings_file
        self.applied.append(settings)


@pytest.fixture
def workspace(tmp_path: Path) -> ApplicationContext:
    (tmp_path / "forge").mkdir()
    (tmp_path / "forge" / "pyproject.toml").touch()
    (tmp_path / "engine").mkdir()
    paths = ProjectPaths(tmp_path, tmp_path / "data")
    paths.settings_file.parent.mkdir(parents=True)
    paths.settings_file.write_text('# comment\n[presentation]\ncolor = "auto"\n')
    return ApplicationContext(AppSettings(), paths, paths.settings_file, Console(), Console())


@pytest.fixture
def managed(workspace: ApplicationContext) -> ApplicationContext:
    paths = workspace.paths
    for source, destination in (
        (ROOT / ".bin", paths.bin_dir),
        (ROOT / "data" / ".config" / "engine", paths.engine_config_dir),
    ):
        if not source.is_dir():
            pytest.skip(f"Managed Engine fixtures unavailable: {source}")
        shutil.copytree(source, destination, ignore=shutil.ignore_patterns("normalize"))
    return workspace


class SettledWorkers(Protocol):
    async def wait_for_complete(self) -> None: ...


async def settle(app: ExplorerApp) -> None:
    await cast(SettledWorkers, app.workers).wait_for_complete()


async def wait_for(app: ExplorerApp) -> ConfigurationEditor:
    explorer = app.screen.query_one(ConfigurationEditor)
    await settle(app)
    assert not explorer.loading
    return explorer


def select(explorer: ConfigurationEditor, path: str) -> None:
    table = cast(DataTable[Text], explorer.query_one(DataTable))
    key = f"{explorer.component}:{path}"
    for index, row in enumerate(table.ordered_rows):
        if row.key.value == key:
            table.move_cursor(row=index)
            explorer.show_field(key)
            return
    raise AssertionError((key, explorer.errors))


@pytest.mark.asyncio
@pytest.mark.parametrize("conflict", [False, True])
async def test_application_review_and_conflict(
    workspace: ApplicationContext, conflict: bool
) -> None:
    app = ExplorerApp(workspace, "application")
    async with app.run_test(size=(145, 44)) as pilot:
        explorer = await wait_for(app)
        explorer.query_one("#configuration-search", Input).value = "color"
        await pilot.pause()
        select(explorer, "presentation.color")
        await pilot.pause()
        await pilot.click("#edit")
        color = cast(Select[str], app.screen.query_one("#setting-enum", Select))
        color.value = "never"
        await pilot.click("#setting-update")
        await settle(app)
        assert explorer.drafts["application"].dirty
        assert 'color = "auto"' in workspace.paths.settings_file.read_text()
        await pilot.click("#copy-link")
        await settle(app)
        assert app.copied == [documentation("application", "presentation.color")]
        await pilot.click("#apply")
        await pilot.pause()
        assert isinstance(app.screen, Confirm)
        if conflict:
            workspace.paths.settings_file.write_text('[presentation]\ncolor = "always"\n')
        await pilot.click("#confirm")
        await settle(app)
        if conflict:
            assert isinstance(app.screen, Detail)
            assert 'color = "always"' in workspace.paths.settings_file.read_text()
            assert explorer.drafts["application"].dirty
        else:
            assert (
                '# comment\n[presentation]\ncolor = "never"'
                in workspace.paths.settings_file.read_text()
            )
            assert app.applied[-1].presentation.color == "never"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("component", "path", "value"),
    [
        ("normalize", "resources.workers", "3"),
        ("merge", "parquet.row_group_target_mb", "8"),
        ("convert", "resources.workers", "3"),
        ("pipeline", "merge.resources.workers", "3"),
    ],
)
async def test_managed_review(
    managed: ApplicationContext,
    component: str,
    path: str,
    value: str,
) -> None:
    app = ExplorerApp(managed, component)
    async with app.run_test(size=(150, 44)) as pilot:
        explorer = await wait_for(app)
        if component == "pipeline":
            cell = cast(DataTable[Text], explorer.query_one(DataTable)).get_row(
                f"{component}:{path}"
            )[1]
            assert cell.plain == "Inherited" and cell.style == "#7dcfff"
        select(explorer, path)
        await pilot.pause()
        await pilot.click("#edit")
        app.screen.query_one("#setting-input", Input).value = value
        await pilot.click("#setting-update")
        await settle(app)
        assert explorer.drafts[component].dirty
        await pilot.click("#apply")
        await pilot.pause(0.5)
        assert isinstance(app.screen, Confirm), app.screen
        await pilot.click("#confirm")
        await settle(app)
        document = tomllib.loads(managed.paths.runtime_config(component).read_text())
        for key in path.split("."):
            document = document[key]
        assert document == int(value)
        if component == "pipeline":
            select(explorer, path)
            await pilot.pause()
            await pilot.click("#inherit")
            await settle(app)
            await pilot.click("#apply")
            await pilot.pause(0.5)
            await pilot.click("#confirm")
            await settle(app)
            assert "merge" not in tomllib.loads(managed.paths.runtime_config(component).read_text())


def test_schema_coverage_and_anchors(managed: ApplicationContext) -> None:
    for component, page in PAGES.items():
        draft = ConfigurationDraft(managed, component)
        fields = draft.fields
        markdown = (
            ROOT / "docs" / "content" / "reference" / "configuration" / f"{page}.md"
        ).read_text()
        assert fields
        assert draft.status == "Validated"
        if component in {"normalize", "merge", "convert"}:
            schema: dict[str, Any] = json.loads(
                subprocess.check_output(
                    [str(managed.paths.bin_dir / f"marketforge-{component}"), "--runtime-schema"]
                )
            )
            assert {field["path"] for field in fields} == {
                field["path"] for field in schema["fields"]
            }
        built = ROOT / "docs" / "site" / "reference" / "configuration" / page / "index.html"
        html = built.read_text() if built.is_file() else None
        for field in fields:
            anchor = f'id="setting-{field["path"]}"'
            assert markdown.count(anchor) == 1
            if html is not None:
                assert anchor in html
            assert field["purpose"]
            if component in {"normalize", "merge", "convert"}:
                assert field["purpose"] in markdown
            if field["mutable"]:
                assert editable(field)


@pytest.mark.asyncio
async def test_integrity_risk_and_indirect_draft_change(
    managed: ApplicationContext,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    links: list[str] = []

    def open_link(url: str) -> bool:
        links.append(url)
        return True

    monkeypatch.setattr("forge.tui.configuration_screen.webbrowser.open", open_link)
    app = ExplorerApp(managed, "normalize")
    field = "integrity_policy.invalid_record.action"
    async with app.run_test(size=(150, 44)) as pilot:
        explorer = await wait_for(app)
        explorer.query_one("#configuration-search", Input).value = "invalid_record action"
        await pilot.pause()
        select(explorer, field)
        await pilot.pause()
        await pilot.press("f1")
        await pilot.pause()
        assert isinstance(app.screen, Detail)
        await pilot.click("#detail-0")
        await settle(app)
        assert links == [documentation("normalize", field)]
        await pilot.click("#edit")
        cast(Select[str], app.screen.query_one("#setting-enum", Select)).value = "degrade"
        await pilot.click("#setting-update")
        await settle(app)
        draft = explorer.drafts["normalize"]
        threshold = next(
            row
            for row in draft.fields
            if row["path"] == "integrity_policy.invalid_record.max_count"
        )
        assert threshold["active"]
        await pilot.click("#apply")
        await pilot.pause(0.5)
        assert isinstance(app.screen, Confirm)
        assert "Acknowledge integrity risk" in app.screen.title_text
        await pilot.click("#confirm")
        await settle(app)
        configured = tomllib.loads(managed.paths.runtime_config("normalize").read_text())
        assert configured["integrity_policy"]["invalid_record"]["action"] == "degrade"


def test_future_fields_are_discoverable(
    managed: ApplicationContext,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from forge.historical.engine.runtime_config import RuntimeConfiguration

    original = RuntimeConfiguration.schema

    def extended(runtime: RuntimeConfiguration) -> dict[str, Any]:
        schema = original(runtime)
        worker = next(field for field in schema["fields"] if field["path"] == "resources.workers")
        return {
            **schema,
            "fields": [
                *schema["fields"],
                {**worker, "path": "resources.new_parameter"},
                {**worker, "path": "resources.future_type", "kind": "future"},
            ],
        }

    monkeypatch.setattr(RuntimeConfiguration, "schema", extended)
    draft = ConfigurationDraft(managed, "normalize")
    by_path = {field["path"]: field for field in draft.fields}
    draft.patch(by_path["resources.new_parameter"], "42")
    assert tomllib.loads(draft.candidate.decode())["resources"]["new_parameter"] == 42
    assert not editable(by_path["resources.future_type"])


def test_pinned_configuration_uses_retained_fingerprint(workspace: ApplicationContext) -> None:
    from hashlib import sha256

    from forge.shared.errors import MarketForgeError
    from forge.tui.configuration import retained_pipeline_settings

    run_id = "pipeline-12345678-1234-5678-1234-567812345678"
    root = workspace.paths.runs_dir / run_id
    path = root / "historical" / "merge.toml"
    path.parent.mkdir(parents=True)
    pinned = b"schema_version = 1\n[resources]\nworkers = 8\n"
    path.write_bytes(pinned)
    (root / "report.json").write_text(
        json.dumps(
            {
                "kind": "historical_pipeline",
                "environment": "production",
                "registry_id": "test-registry-fixture",
                "settings_files": {str(path): sha256(pinned).hexdigest()},
            }
        )
    )
    result = retained_pipeline_settings(workspace, run_id, "test-registry-fixture")
    assert result["Run"] == run_id
    with pytest.raises(MarketForgeError, match="different or unverified registry"):
        retained_pipeline_settings(workspace, run_id, "another-registry")
    path.write_bytes(pinned.replace(b"8", b"2"))
    with pytest.raises(MarketForgeError, match="fingerprint"):
        retained_pipeline_settings(workspace, run_id, "test-registry-fixture")


@pytest.mark.asyncio
async def test_batch_validates_cross_field_constraints_before_writing(
    managed: ApplicationContext,
) -> None:
    app = ExplorerApp(managed, "merge")
    source = managed.paths.runtime_config("merge")
    original = source.read_bytes()
    async with app.run_test(size=(150, 44)) as pilot:
        explorer = await wait_for(app)
        for path, value in (
            ("parquet.file_target_mb", "128"),
            ("parquet.row_group_target_mb", "64"),
        ):
            select(explorer, path)
            await pilot.pause()
            await pilot.click("#edit")
            app.screen.query_one("#setting-input", Input).value = value
            await pilot.click("#setting-update")
            await settle(app)
            assert source.read_bytes() == original
            assert explorer.drafts["merge"].dirty
            if value == "128":
                assert explorer.drafts["merge"].status != "Validated"
                await pilot.click("#apply")
                await settle(app)
                assert isinstance(app.screen, Detail)
                await pilot.press("escape")
        await pilot.click("#apply")
        await pilot.pause(0.5)
        assert isinstance(app.screen, Confirm)
        await pilot.click("#confirm")
        await settle(app)
        parquet = tomllib.loads(source.read_text())["parquet"]
        assert parquet["file_target_mb"] == 128
        assert parquet["row_group_target_mb"] == 64
        assert "compression_level = 3 # ZSTD" in source.read_text()


@pytest.mark.asyncio
async def test_configs_workspace_tabs_preserve_drafts_and_guard_exit(
    managed: ApplicationContext,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from forge.tui.app import AREAS, ForgeApp
    from forge.tui.configuration_screen import FieldEditor

    links: list[str] = []

    def open_link(url: str) -> bool:
        links.append(url)
        return True

    monkeypatch.setattr("forge.tui.configuration_screen.webbrowser.open", open_link)
    app = ForgeApp(managed)
    async with app.run_test(size=(78, 26)) as pilot:
        await pilot.press("ctrl+n")
        assert "System" not in AREAS
        await pilot.click(f"#area-{AREAS.index('Configs')}")
        editor = app.query_one(ConfigurationEditor)
        await pilot.pause(0.4)
        assert app.area == "Configs"
        assert [str(button.label) for button in app.query("#views Button").results(Button)] == [
            "General",
            "Normalize",
            "Merge",
            "Convert",
            "Pipeline",
        ]
        assert editor.drafts["application"].path == managed.paths.settings_file
        assert len(editor.visible_fields) == 9
        assert not editor.query(
            "#configuration-inspector, #configuration-detail, #configuration-category"
        )
        await pilot.click("#view-1")
        await pilot.pause(0.5)
        search = editor.query_one("#configuration-search", Input)
        search.value = "resources.workers"
        await pilot.pause()
        select(editor, "resources.workers")
        editor.query_one("#configuration-fields", DataTable).focus()
        await pilot.press("f1")
        await pilot.pause()
        assert isinstance(app.screen, Detail)
        await pilot.click("#detail-0")
        await pilot.pause()
        assert links == [documentation("normalize", "resources.workers")]
        await pilot.press("enter")
        assert isinstance(app.screen, FieldEditor)
        app.screen.query_one("#setting-input", Input).value = "5"
        await pilot.click("#setting-update")
        await pilot.pause(0.4)
        assert editor.counts() == {"normalize": 1}
        assert str(app.query_one("#view-1", Button).label) == "Normalize (1)"
        for index, component in enumerate(("merge", "convert", "pipeline"), start=2):
            await pilot.click(f"#view-{index}")
            await pilot.pause(0.5)
            draft = editor.drafts[component]
            assert draft.path == managed.paths.runtime_config(component)
            assert draft.status == "Validated"
            assert search.value == ""
        await pilot.click("#view-2")
        await pilot.pause()
        search.value = "resources.workers"
        await pilot.pause()
        select(editor, "resources.workers")
        await pilot.pause()
        await pilot.click("#edit")
        app.screen.query_one("#setting-input", Input).value = "4"
        await pilot.click("#setting-update")
        await pilot.pause(0.4)
        assert editor.counts() == {"normalize": 1, "merge": 1}
        await pilot.click("#apply")
        await pilot.pause(0.5)
        assert isinstance(app.screen, Confirm)
        await pilot.click("#confirm")
        await pilot.pause(0.5)
        assert editor.counts() == {"normalize": 1}
        table = cast(DataTable[Text], editor.query_one("#configuration-fields", DataTable))
        table.focus()
        await pilot.press("ctrl+r")
        await pilot.pause(0.5)
        assert editor.visible_fields["merge:resources.workers"][1]["effective"] == 4
        await pilot.click("#view-1")
        await pilot.pause()
        assert search.value == "resources.workers"
        assert editor.drafts["normalize"].dirty
        settings_table = cast(DataTable[Text], editor.query_one("#configuration-fields", DataTable))
        cell = settings_table.get_row("normalize:resources.workers")[0]
        assert cell.plain == "* resources.workers"
        assert cell.style == "#e0af68"
        await pilot.click("#area-0")
        await pilot.pause()
        assert isinstance(app.screen, Confirm)
        await pilot.press("escape")
        assert app.area == "Configs" and editor.dirty
        await pilot.click("#area-0")
        await pilot.click("#confirm")
        await pilot.pause()
        assert app.area == "Overview"
        assert not editor.dirty


@pytest.mark.asyncio
async def test_configs_missing_managed_engine_keeps_runtime_tabs(
    workspace: ApplicationContext,
) -> None:
    from textual.widgets import Static

    from forge.tui.app import ForgeApp

    app = ForgeApp(workspace)
    app.initial_area, app.initial_view = "Configs", "Normalize"
    async with app.run_test(size=(68, 24)) as pilot:
        await pilot.pause(0.4)
        editor = app.query_one(ConfigurationEditor)
        status = str(editor.query_one("#configuration-status", Static).content)
        assert "data/.config/engine/normalize.toml" in status
        assert "mf engine build normalize" in status
        assert len(app.query("#views Button")) == 5
        assert not editor.visible_fields
        await pilot.click("#view-3")
        await pilot.pause(0.2)
        status = str(editor.query_one("#configuration-status", Static).content)
        assert "mf engine build convert" in status


@pytest.mark.asyncio
async def test_general_groups_validation_and_independent_runtime_draft(
    managed: ApplicationContext,
) -> None:
    from textual.widgets import Static

    from forge.tui.app import ForgeApp

    app = ForgeApp(managed)
    app.initial_area = "Configs"
    async with app.run_test(size=(100, 30)) as pilot:
        await pilot.pause(0.4)
        editor = app.query_one(ConfigurationEditor)
        table = cast(DataTable[Text], editor.query_one(DataTable))
        search = editor.query_one("#configuration-search", Input)
        status = editor.query_one("#configuration-status", Static)
        assert app.query_one("#view-0", Button).has_class("active")
        assert table.get_row("application:version")[0].style == "#565f89"
        await pilot.click("#configuration-fields", offset=(3, 1))
        await pilot.pause()
        assert table.get_row("group:general")[0].plain.startswith("▸")
        assert table.get_row("group:http")[0].style == "#7dcfff"
        index = next(i for i, row in enumerate(table.ordered_rows) if row.key.value == "group:http")
        table.move_cursor(row=index)
        table.focus()
        await pilot.press("enter")
        await pilot.pause()
        assert "application:http.max_concurrent" not in editor.visible_fields
        assert table.get_row("group:http")[0].plain.startswith("▸")
        search.value = "max_concurrent"
        await pilot.pause()
        assert list(editor.visible_fields) == ["application:http.max_concurrent"]
        search.focus()
        await pilot.press("enter")
        await pilot.press("f1")
        await pilot.pause()
        assert isinstance(app.screen, Detail)
        assert "Maximum concurrent HTTP requests" in app.screen.value_text
        await pilot.press("escape")

        path = managed.paths.settings_file
        original = path.read_bytes()
        await pilot.click("#edit")
        app.screen.query_one("#setting-input", Input).value = "12"
        await pilot.press("escape")
        assert not editor.dirty and path.read_bytes() == original
        for value in ("0", "7"):
            await pilot.click("#edit")
            app.screen.query_one("#setting-input", Input).value = value
            await pilot.click("#setting-update")
            await pilot.pause(0.4)
            assert editor.counts() == {"application": 1}
            assert path.read_bytes() == original
            if value == "0":
                invalid = status.content
                assert isinstance(invalid, Text) and "INVALID" in invalid.plain
                assert any(span.style == "#f7768e" for span in invalid.spans)
                assert table.get_row("application:http.max_concurrent")[1].plain == "— → 0"
                await pilot.click("#apply")
                await pilot.pause()
                assert isinstance(app.screen, Detail)
                await pilot.press("escape")
        modified = status.content
        assert isinstance(modified, Text) and "MODIFIED" in modified.plain
        assert any(span.style == "#9ece6a" for span in modified.spans)

        await pilot.click("#view-1")
        await pilot.pause(0.4)
        search.value = "resources.workers"
        await pilot.pause()
        select(editor, "resources.workers")
        await pilot.pause()
        await pilot.click("#edit")
        app.screen.query_one("#setting-input", Input).value = "5"
        await pilot.click("#setting-update")
        await pilot.pause(0.4)
        await pilot.click("#view-0")
        await pilot.pause()
        assert editor.counts() == {"application": 1, "normalize": 1}
        assert search.value == "max_concurrent"
        await pilot.click("#apply")
        await pilot.pause(0.3)
        assert isinstance(app.screen, Confirm)
        await pilot.click("#confirm")
        await pilot.pause(0.4)
        assert app.context.settings.http.max_concurrent == 7
        assert tomllib.loads(path.read_text())["http"]["max_concurrent"] == 7
        assert editor.counts() == {"normalize": 1}
        table.focus()
        await pilot.press("ctrl+r")
        await pilot.pause(0.4)
        assert editor.visible_fields["application:http.max_concurrent"][1]["effective"] == 7
        search.value = ""
        await pilot.pause()
        assert table.get_row("group:http")[0].plain.startswith("▸")


@pytest.mark.asyncio
@pytest.mark.parametrize("size", [(80, 24), (100, 30), (120, 40), (160, 45)])
async def test_compact_config_toolbar_and_fields_fit(
    workspace: ApplicationContext,
    size: tuple[int, int],
) -> None:
    from forge.tui.app import ForgeApp

    app = ForgeApp(workspace)
    app.initial_area = "Configs"
    async with app.run_test(size=size) as pilot:
        await pilot.pause(0.25)
        editor = app.query_one(ConfigurationEditor)
        table = cast(DataTable[Text], editor.query_one(DataTable))
        tabs = list(app.query("#views Button").results(Button))
        actions = [
            editor.query_one(f"#{key}", Button)
            for key in ("edit", "help", "apply", "category-docs", "more")
        ]
        assert len(tabs) == 5
        assert len(editor.visible_fields) == 9
        assert all(button.region.right <= size[0] for button in tabs + actions)
        assert table.region.height > (size[1] // 2)
        assert table.region.bottom <= actions[0].region.y
        assert not editor.query("#configuration-inspector, #configuration-category")
