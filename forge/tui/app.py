"""Persistent Textual workspace for the Forge service clients."""

import asyncio
import difflib
import tomllib
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from functools import partial
from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, cast

from rich.text import Text
from textual import on, work
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.command import Hit, Hits, Provider
from textual.containers import Horizontal, Vertical, VerticalScroll
from textual.css.query import NoMatches
from textual.events import Resize
from textual.screen import Screen
from textual.signal import Signal
from textual.widgets import Button, DataTable, Footer, Input, Static

from forge.catalog.service import currency_codes
from forge.datasets import workspace_inventory
from forge.datasets.query import DatasetRequest, browse, publications, utc
from forge.historical.archives.config import ArchiveRequest
from forge.historical.archives.progress import ProgressSnapshot
from forge.historical.archives.storage.files import disk
from forge.historical.authoring import (
    add_requests,
    publish_new,
    recipe_writer,
    replace_merge_source,
    replace_reviewed,
)
from forge.historical.pipelines.progress import PipelineSnapshot, StageSnapshot
from forge.orchestration.inspection import (
    MANAGED_RUN,
    read_managed_logs,
    read_managed_report,
    run_page,
)
from forge.orchestration.registry import list_index, show_index, workspace_runs
from forge.orchestration.run_diagnostics import DiagnosticPage
from forge.orchestration.run_summary import when
from forge.orchestration.runs import RunInspection, safe_reference
from forge.shared.config.deletion import RecipeFamily
from forge.shared.config.library import list_config_names, resolve_config_file
from forge.shared.config.models import AppSettings
from forge.shared.config.toml import dumps
from forge.shared.database.connection import (
    DatabaseError,
    database_connection,
    database_dsn,
    database_environment,
    pinned_registry,
    selected_registry_id,
)
from forge.shared.documentation import SITE_URL
from forge.shared.errors import MarketForgeError
from forge.shared.ownership import WorkspaceOwnership
from forge.shared.terminal.editor import EditorError
from forge.shared.terminal.editor_window import (
    EditorCompletionUnconfirmed,
    EditorInterrupted,
    EditorLaunchFailed,
    edit_in_window,
)
from forge.shared.terminal.palette import BLUE, CYAN, FG, GREEN, MUTED, RED, YELLOW
from forge.startup.context import ApplicationContext
from forge.tui import catalogue as catalogue_ui
from forge.tui import data_actions, data_recipes, market_recipes, overview
from forge.tui import workspace_dashboard as dashboard
from forge.tui.actions import (
    ACTIONS,
    BY_KEY,
    EXECUTION_ACTIONS,
    WORKFLOW_CONTROLS,
    Action,
    Field,
    Value,
    Values,
    read_only,
)
from forge.tui.clipboard import copy_text, plain, safe_text
from forge.tui.configuration_screen import (
    CONFIG_TABS,
    ConfigurationEditor,
    ConfigurationExplorer,
    DraftsChanged,
)
from forge.tui.controller import Controller, Invoke, Operation, document, outcome
from forge.tui.design import THEME, WorkspaceTitle, success_message, utilization
from forge.tui.editor_inspection import (
    LARGE_INSPECTION,
    InspectionTarget,
    OpenInspection,
    RetainedFile,
)
from forge.tui.editor_inspection import stage as stage_inspection
from forge.tui.editor_sessions import ApplyDraft, EditorSession, ValidateDraft
from forge.tui.inspection import ActionMenu, Inspection
from forge.tui.operation_view import OperationView
from forge.tui.presentation import (
    Cell,
    Results,
    Selection,
    bind,
    cell_text,
    columns,
    membership,
    visible,
)
from forge.tui.recipe_forms import RecipeForm, model_for, summarize
from forge.tui.recipe_inspection import configuration as recipe_configuration
from forge.tui.recipe_inspection import retained_summary
from forge.tui.recipe_inspection import sections as recipe_sections
from forge.tui.resources import CATALOG, FAMILIES, Row, library, load_rows, records, value_at
from forge.tui.run_diagnostics import ErrorsPanel
from forge.tui.run_views import RunView, retained_files
from forge.tui.run_views import project as project_run
from forge.tui.screens import Confirm, Detail, EntityChoices, EntityPicker, Form, StreamOrder
from forge.tui.services import apply_cleanup, execute, ranks
from forge.tui.source_recipes import (
    conversion_candidate,
    save_normalization,
    validate_recipe_draft,
)

AREAS = (
    "Overview",
    "Workflows",
    "Data",
    "Markets",
    "Catalogue",
    "Runs",
    "Configs",
)
VIEWS = {
    "Overview": ("Workspace",),
    "Workflows": (
        "Pipeline",
        "Acquisition",
        "Normalization",
        "Merge",
        "Conversion",
        "Import",
        "Registration",
    ),
    "Data": dashboard.STAGES,
    "Markets": ("Instruments", "Rankings", "Policies"),
    "Catalogue": tuple(catalogue_ui.LABELS),
    "Runs": ("Retained", "Indexed", "Owned"),
    "Configs": tuple(CONFIG_TABS),
}


@dataclass(slots=True)
class ViewState:
    filter: str = ""
    offset: int = 0
    filters: Values = field(default_factory=Values)
    sort: str = ""
    descending: bool = False
    selection: str | None = None
    rows: dict[str, Row] = field(default_factory=dict[str, Row])
    more: bool = False
    scroll: tuple[float, float] = (0, 0)
    basket: dict[str, Row] = field(default_factory=dict[str, Row])


class Commands(Provider):
    async def search(self, query: str) -> Hits:
        app = cast(ForgeApp, self.app)
        matcher = self.matcher(query)
        for label, callback in (
            ("Switch environment", app.switch_environment),
            ("Editor sessions", app.action_editor_sessions),
            (
                "Edit Forge application settings",
                partial(app.open_workspace_view, "Configs", "General"),
            ),
            ("Inspect registry settings", partial(app.configuration_settings, "environment")),
            ("Refresh Market Observations", partial(app.open_action, BY_KEY["markets refresh"])),
            ("Synchronize Market Instruments", partial(app.open_action, BY_KEY["markets sync"])),
            ("Review Legacy Run Indexing", partial(app.run_maintenance, "index-legacy")),
            ("Review Run Reconciliation", partial(app.run_maintenance, "reconcile")),
        ):
            if score := matcher.match(label):
                yield Hit(score, matcher.highlight(label), callback, text=label)
        if app.area not in {"Overview", "Configs"} and app.state.basket:
            label = "Clear selection"
            if score := matcher.match(label):
                yield Hit(score, matcher.highlight(label), app.clear_basket, text=label)
        if app.area == "Catalogue":
            for label, callback in app.catalogue_actions(None).items():
                if score := matcher.match(label):
                    yield Hit(score, matcher.highlight(label), callback, text=label)
        for action in ACTIONS:
            score = matcher.match(action.key)
            if score > 0:
                yield Hit(
                    score,
                    matcher.highlight(action.key),
                    lambda action=action: app.open_action(action),
                    text=action.key,
                    help=action.area,
                )


class ForgeApp(App[str]):
    TITLE = "MarketForge / Forge"
    CSS_PATH = "workspace.tcss"
    COMMANDS = {Commands}
    BINDINGS = [
        Binding("ctrl+p", "command_palette", "Commands", show=False),
        Binding("ctrl+e", "switch_registry", "Env", priority=True),
        Binding("ctrl+n", "navigation", "Nav"),
        Binding("/", "filter", "Filter"),
        Binding("ctrl+r", "reload", "Reload"),
        Binding("pageup", "previous", "Prev page", show=False, priority=True),
        Binding("pagedown", "next", "Next page", show=False, priority=True),
        Binding("f9", "editor_sessions", "Editors", show=False),
        Binding("ctrl+o", "operation", "Run"),
        Binding("f2", "context", "Actions"),
        Binding("a", "context", "Actions", show=False),
        Binding("f,s", "filters", "Filter and sort", show=False),
        Binding("r", "reload", "Refresh", show=False),
        Binding("space", "basket", "Select", show=False),
        Binding("j", "row_down", "Next row", show=False),
        Binding("k", "row_up", "Previous row", show=False),
        Binding("y", "copy_row", "Copy", show=False),
        Binding("ctrl+y", "copy_data_selection", "Copy selected text", show=False),
        Binding("question_mark", "workspace_help", "Help", show=False),
        Binding("shift+f10", "context", "Context menu", show=False),
        Binding("ctrl+shift+x", "emergency_stop", "Stop all", priority=True),
        Binding("ctrl+c", "interrupt", "Interrupt", show=False, priority=True),
        Binding("ctrl+q", "quit_forge", "Quit", show=False, priority=True),
    ]

    def __init__(self, context: ApplicationContext, *, invoke: Invoke = execute) -> None:
        super().__init__()
        self.register_theme(THEME)
        self.theme = THEME.name
        self.context = context
        self.initial_area = AREAS[0]
        self.initial_view: str | None = None
        self.database_environment = "testing" if context.testing else "production"
        self.database_indicator = "checking"
        self.database_id: str | None = None
        self.controller = Controller(context, self.edit_recipe, invoke)
        self.area = AREAS[0]
        self.view = VIEWS[self.area][0]
        self.states: dict[tuple[str, str], ViewState] = {}
        self.version = 0
        self.root_screen = self.screen_stack[0] if self.screen_stack else None
        self.editor_sessions: dict[Path, EditorSession] = {}
        self.inspection_sessions: set[Path | int] = set()
        self.recipe_activation = False
        self.emergency_confirmation = False
        self.inventory_generation = 0
        self.inventory_pending = False
        self.inventory_lock = asyncio.Lock()
        self.inventory_data: workspace_inventory.WorkspaceInventory | None = None
        self.inventory_failed = False
        self.inventory_stage = "Raw"
        self.inventory_pages: dict[str, int] = dict.fromkeys(dashboard.STAGES, 0)
        self.inventory_rows: dict[str, dict[str, object]] = {}
        self.inventory_more = False
        self.inventory_page_cache: dict[str, tuple[int, list[dict[str, object]], bool]] = {}
        self.inventory_page_generation = 0
        self.inventory_selection: dict[str, tuple[int, float, float]] = {}
        self.validated_profiles: set[str] = set()
        self.raw_complete = False
        self.overview_history_lock = asyncio.Lock()
        self.overview_history_note = "Loading recent runs…"
        self.overview_retained: list[dict[str, object]] = []
        self.overview_indexed: list[dict[str, object]] = []
        self.overview_incomplete = False
        self.host_cpu: tuple[int, int] | None = None
        self.host_percent: float | None = None
        self.host_memory: tuple[int, int] | None = None
        self.host_disk: tuple[int, int] | None = None
        self.registry_checked: datetime | None = None
        self.overview_completed: str | None = None
        self.overview_recovered: set[tuple[str, str]] = set()

    @property
    def state(self) -> ViewState:
        scope = self.view + " (recorded complete)" if self.raw_complete else self.view
        return self.states.setdefault((self.area, scope), ViewState())

    def workspace_header(self) -> str:
        return f"MarketForge Workspace · {self.area}"

    def compose(self) -> ComposeResult:
        with Horizontal(id="workspace-title-bar"):
            yield Button(
                "STOP ALL",
                id="emergency-stop",
                variant="error",
                compact=True,
                action="app.emergency_stop",
            )
            yield WorkspaceTitle(
                self.workspace_header(),
                id="workspace-header",
                markup=False,
            )
        yield Static("", id="workspace-monitor", classes="workspace-monitor", markup=False)
        with Horizontal(id="workspace-body"):
            with VerticalScroll(id="navigation"):
                for index, area in enumerate(AREAS):
                    yield Button(area, id=f"area-{index}")
            with Vertical(id="main"):
                yield Horizontal(id="views")
                yield ConfigurationEditor(
                    self.context,
                    self.queue_editor,
                    self.copy_value,
                    self.reload_application_settings,
                    self.configuration_environment,
                    "application",
                    embedded=True,
                )
                with Horizontal(id="browse-tools"):
                    yield Input(placeholder="Search", id="filter")
                    yield Button("Filters", id="browse-filters")
                    yield Button("Reset", id="browse-reset", classes="tertiary")
                    yield Button("New recipe", id="recipe-new", variant="primary")
                yield Static("", id="query-state", markup=False)
                with VerticalScroll(id="overview-scroll"):
                    yield dashboard.WorkspaceDashboard()
                with Horizontal(id="resource-body"):
                    yield Results(cursor_type="row", id="results")
                    yield Static("", id="resource-state", markup=False)
                with Horizontal(id="workflow-actions"):
                    for verb in ("validate", "plan", "map", "prepare", "download", "run"):
                        yield Button(
                            verb.title(),
                            id=f"workflow-{verb}",
                            classes=(
                                "validate"
                                if verb in {"validate", "plan"}
                                else "prepare"
                                if verb in {"prepare", "map"}
                                else "run"
                            ),
                        )
                with Horizontal(id="context-actions"):
                    yield Button("Enter Inspect", id="inspect-selected", classes="compact")
                    yield Button("F2 Actions", id="context-menu", classes="compact")
                    yield Button("Edit", id="edit-source", classes="compact")
                    yield Button("PgUp Prev", id="page-previous", classes="compact tertiary")
                    yield Button("PgDn Next", id="page-next", classes="compact tertiary")
        yield Static("", id="operation", classes="-idle", markup=False)
        yield Footer()

    def resource_table(self) -> Results:
        return (self.root_screen or self.screen).query_one(
            "#overview-results" if self.area == "Overview" else "#results", Results
        )

    async def on_mount(self) -> None:
        screen_changes = cast(Signal[Screen[object]], self.screen_change_signal)  # pyright: ignore[reportUnknownMemberType]
        screen_changes.subscribe(self, self.mount_emergency_button)
        screen_changes.subscribe(self, self.clear_suspended_data_selection)
        self.root_screen = self.screen
        self.set_interval(0.25, self.render_operation)
        await self.switch(self.initial_area, self.initial_view)
        self.refresh_monitor()
        self.sample_host()
        self.sample_disk()
        self.check_registry()
        self.set_interval(1, self.refresh_monitor)
        self.set_interval(2, self.sample_host)
        self.set_interval(20, self.sample_disk)
        self.set_interval(20, self.check_registry)

    def clear_suspended_data_selection(self, screen: Screen[object]) -> None:
        if self.root_screen is not None and screen is not self.root_screen and self.area == "Data":
            self.resource_table().clear_text_selection()

    @work(group="workspace-registry", exit_on_error=False)
    async def check_registry(self) -> None:
        if (
            sum(
                worker.is_running for worker in self.workers if worker.group == "workspace-registry"
            )
            > 1
        ):
            return
        try:
            with (
                database_environment(testing=self.context.testing),
                pinned_registry(self.database_id),
            ):
                self.database_id = await selected_registry_id(self.context.paths.root)
                self.controller.registry_identity = self.database_id
        except Exception:
            self.database_indicator = "disconnected"
        else:
            self.database_indicator = "connected"
        self.registry_checked = datetime.now(UTC)
        self.refresh_monitor()

    @work(exclusive=True, group="workspace-host", exit_on_error=False)
    async def sample_host(self) -> None:
        sample = await disk(dashboard.host_sample)
        self.host_percent = dashboard.cpu_percent(self.host_cpu, sample.cpu)
        self.host_cpu, self.host_memory = sample.cpu, sample.memory
        self.refresh_monitor()

    @work(exclusive=True, group="workspace-disk", exit_on_error=False)
    async def sample_disk(self) -> None:
        root = self.context.paths.data_root
        try:
            value = await disk(dashboard.capacity, root)
        except OSError:
            value = None
        if root == self.context.paths.data_root:
            self.host_disk = value
            self.refresh_monitor()

    def refresh_monitor(self) -> None:
        if self.root_screen is None:
            return
        now = datetime.now(UTC)
        state = self.database_indicator
        if (
            state == "connected"
            and self.registry_checked
            and (now - self.registry_checked).total_seconds() > 45
        ):
            state = "stale"
        width = self.size.width
        colors = {
            "connected": GREEN,
            "disconnected": RED,
            "checking": YELLOW,
            "stale": YELLOW,
        }
        parts = Text("DB ", BLUE)
        parts.append("● ", colors.get(state, MUTED))
        parts.append(self.database_environment.upper(), FG)
        parts.append(f" {state}", colors.get(state, MUTED))
        if width >= 120 and self.database_id:
            parts.append(f" · {self.database_id[:8]}", MUTED)
        separator = "  │  " if width >= 120 else "  "
        cpu = self.host_percent
        parts.append(separator + "CPU ", BLUE)
        parts.append(
            ("—" if cpu is None else f"{cpu:.1f}%"),
            utilization(cpu),
        )
        memory = self.host_memory
        ram = 100 * memory[0] / memory[1] if memory and memory[1] else None
        usage = "—" if ram is None else f"{ram:.0f}%"
        if ram is not None and memory and width >= 100:
            usage = f"{memory[0] / 2**30:.1f}/{memory[1] / 2**30:.1f} GiB ({usage})"
        parts.append(separator + "RAM ", BLUE)
        parts.append(usage, utilization(ram))
        disk_sample = self.host_disk
        free = disk_sample[0] if disk_sample else None
        disk_label = "—" if free is None else dashboard.size_label(free) + " free"
        if free is not None and width < 100:
            disk_label = f"{free / 2**30:.0f} GiB free" if free >= 2**30 else disk_label
        disk_used = (
            100 * (1 - disk_sample[0] / disk_sample[1]) if disk_sample and disk_sample[1] else None
        )
        parts.append(separator + "Disk ", BLUE)
        parts.append(disk_label, utilization(disk_used))
        clock = now.strftime(" %H:%M:%S UTC")
        parts.truncate(max(1, width - 2 - len(clock)), overflow="ellipsis")
        parts.append(clock, FG)
        parts.no_wrap = True
        parts.overflow = "ellipsis"
        screens = (
            (self.root_screen,)
            if self.screen is self.root_screen
            else (self.root_screen, self.screen)
        )
        for screen in screens:
            for widget in screen.query(".workspace-monitor").results(Static):
                widget.tooltip = f"Registry identity: {self.database_id or 'unverified'}"
                update_static(widget, parts)

    def on_resize(self, event: Resize) -> None:
        if self.root_screen is not None:
            self.root_screen.set_class(event.size.width < 93, "narrow")
            self.root_screen.set_class(event.size.width < 110, "summary-stacked")
            self.root_screen.set_class(event.size.width >= 146, "wide")
            if self.screen == self.root_screen and self.state.rows:
                self.call_later(self.render_rows)
            self.root_screen.set_class(event.size.height < 18, "short")
            if self.area == "Overview":
                self.call_after_refresh(self.render_inventory_table)
            self.refresh_monitor()

    def action_navigation(self) -> None:
        if self.root_screen is not None:
            if self.size.width < 93:
                self.root_screen.toggle_class("nav-open")
                visible = self.root_screen.has_class("nav-open")
            else:
                self.root_screen.toggle_class("nav-hidden")
                visible = not self.root_screen.has_class("nav-hidden")
            if visible:
                self.root_screen.query_one("#area-0", Button).focus()

    def action_filter(self) -> None:
        if (
            self.screen == self.root_screen
            and self.area != "Overview"
            and not isinstance(self.focused, Input)
        ):
            self.query_one(
                "#configuration-search" if self.area == "Configs" else "#filter", Input
            ).focus()

    def action_reload(self) -> None:
        if self.area == "Configs" and self.screen == self.root_screen:
            self.query_one(ConfigurationEditor).interact("reload")
            return
        self.request_query()

    def check_action(self, action: str, parameters: tuple[object, ...]) -> bool | None:
        if self.area == "Overview" and action in {"basket", "copy_row", "filters", "context"}:
            return False
        if action in {"basket", "row_up", "row_down", "copy_row", "filters", "workspace_help"}:
            return self.screen == self.root_screen and (
                isinstance(self.focused, Results)
                if action in {"basket", "row_up", "row_down"}
                else not isinstance(self.focused, Input)
            )
        if action == "operation" and self.controller.current is None:
            return False
        if action == "reload" and isinstance(self.focused, Input):
            return False
        if action in {"previous", "next"}:
            return (
                self.screen == self.root_screen
                and (
                    self.area != "Overview"
                    or self.focused is not None
                    and self.focused.id == "workspace-inventory"
                )
                and not isinstance(self.focused, Input)
            )
        if action == "copy_data_selection":
            return self.screen == self.root_screen and isinstance(self.focused, Results)
        if action == "context":
            return self.screen == self.root_screen and not isinstance(self.focused, Input)
        if action == "filter" and self.area == "Overview" and self.screen == self.root_screen:
            return False
        return super().check_action(action, parameters)

    def page_size(self) -> int:
        return workspace_inventory.PAGE_SIZE if self.area == "Data" else 50

    def action_next(self) -> None:
        if self.screen == self.root_screen and self.area == "Overview":
            self.page_inventory(1)
        elif self.screen == self.root_screen and self.state.more:
            self.state.offset += self.page_size()
            self.request_query()

    def action_previous(self) -> None:
        if self.screen == self.root_screen and self.area == "Overview":
            self.page_inventory(-1)
        elif self.screen == self.root_screen and self.state.offset:
            self.state.offset = max(0, self.state.offset - self.page_size())
            self.request_query()

    async def switch(
        self, area: str, view: str | None = None, *, raw_complete: bool = False
    ) -> None:
        was_configs = self.area == "Configs"
        if was_configs:
            editor = self.query_one(ConfigurationEditor)
            if editor.editing:
                self.notify(
                    "Wait for the current configuration action to finish.", severity="warning"
                )
                return
            if area != "Configs" and editor.dirty:
                self.confirm_leave_configs(area, view, raw_complete)
                return
        results = self.resource_table()
        results.clear_text_selection()
        self.state.scroll = (results.scroll_x, results.scroll_y)
        if self.area == "Overview" and area != "Overview":
            self.inventory_generation += 1
            self.inventory_page_generation += 1
            self.inventory_pending = False
        self.area, self.view = (
            area,
            view if view is not None and view in VIEWS[area] else VIEWS[area][0],
        )
        self.raw_complete = raw_complete
        if area == "Overview":
            self.paint_inventory()
        self.screen.set_class(area == "Overview", "overview")
        self.screen.set_class(area == "Configs", "configs")
        self.refresh_workflow_actions()
        self.query_one("#context-menu", Button).label = (
            f"Actions · {len(self.state.basket)} selected" if self.state.basket else "F2 Actions"
        )
        for button in ("page-next", "page-previous"):
            self.query_one(f"#{button}", Button).display = area != "Overview"
        self.query_one("#overview-history-note", Static).display = area == "Overview" and bool(
            self.overview_history_note
        )
        self.query_one("#workspace-header", Static).update(self.workspace_header())
        self.screen.set_class(not self.context.testing, "production")
        self.render_rows()
        for index, name in enumerate(AREAS):
            self.query_one(f"#area-{index}", Button).set_class(name == area, "active")
        field = self.query_one("#filter", Input)
        field.value = self.state.filter
        field.placeholder = {
            "Workflows": "Find recipe",
            "Data": "Find data",
            "Markets": "Search instruments",
            "Catalogue": "Search catalogue",
            "Runs": "Find retained run",
        }.get(area, "Search")
        new = self.query_one("#recipe-new", Button)
        new.display = area in {"Workflows", "Catalogue"} or self.view == "Policies"
        new.label = f"New {catalogue_ui.LABELS[self.view]}" if area == "Catalogue" else "New recipe"
        self.query_one("#browse-filters", Button).display = bool(
            self.filter_fields() or self.sort_fields()
        )

        group = self.query_one("#views", Horizontal)
        if was_configs and area == "Configs":
            for index, label in enumerate(CONFIG_TABS):
                group.query_one(f"#view-{index}", Button).set_class(label == self.view, "active")
        else:
            await group.remove_children()
            await group.mount(
                *(
                    Button(
                        label,
                        id=f"view-{index}",
                        classes="compact active" if label == self.view else "compact",
                    )
                    for index, label in enumerate(VIEWS[area])
                )
            )
        if area == "Configs":
            self.version += 1
            editor = self.query_one(ConfigurationEditor)
            editor.activate(CONFIG_TABS[self.view])
            self.update_config_tabs(editor.counts())
            if not was_configs:
                editor.query_one("#configuration-search", Input).focus()
        else:
            self.request_query()

    @work(group="leave-configs", exclusive=True, exit_on_error=False)
    async def confirm_leave_configs(self, area: str, view: str | None, raw_complete: bool) -> None:
        if await self.confirm_discard_configs():
            self.query_one(ConfigurationEditor).discard()
            await self.switch(area, view, raw_complete=raw_complete)

    async def confirm_discard_configs(self) -> bool:
        auxiliary_dirty = (
            isinstance(self.screen, ConfigurationExplorer) and self.screen.editor.dirty
        )
        return not (
            self.query_one(ConfigurationEditor).dirty or auxiliary_dirty
        ) or await self.push_screen_wait(
            Confirm(
                "Discard pending configuration changes?",
                "Back keeps your drafts. Confirm leaves Configs and discards pending changes.",
            )
        )

    @on(DraftsChanged)
    def configs_changed(self, event: DraftsChanged) -> None:
        self.update_config_tabs(event.counts)

    def update_config_tabs(self, counts: dict[str, int]) -> None:
        if self.area == "Configs":
            for index, (label, key) in enumerate(CONFIG_TABS.items()):
                button = self.query_one(f"#view-{index}", Button)
                count = counts.get(key)
                button.set_class(count is not None, "pending")
                button.label = (
                    label if count is None else f"{label} ({count})" if count else f"{label} *"
                )

    def request_query(self) -> None:
        if self.area == "Configs":
            return
        if self.area == "Data":
            self.resource_table().clear_text_selection()
        self.version += 1
        if self.area == "Overview":
            self.load_overview_history(self.version)
            self.request_inventory()
            self.load_inventory_page()
        else:
            self.query_view(
                self.version, self.area, self.view, self.state.filter, self.state.offset
            )

    def request_inventory(self) -> None:
        if any(
            worker.group == "overview-inventory" and worker.is_running and not worker.is_cancelled
            for worker in self.workers
        ):
            self.inventory_pending = True
            return
        self.inventory_generation += 1
        self.refresh_inventory(self.inventory_generation)

    @work(group="query", exit_on_error=False)
    async def query_view(
        self, version: int, area: str, view: str, search: str, offset: int
    ) -> None:
        state = (self.root_screen or self.screen).query_one("#query-state", Static)
        state.remove_class("-error").add_class("-loading")
        state.update(f"Loading {view.lower()}…")
        self.resource_message(f"Loading {view.lower()}…")
        try:
            await asyncio.sleep(0.16)
            if version != self.version:
                return
            rows, more = await self.load_rows(area, view, search, offset)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            if version == self.version:
                state.remove_class("-loading").add_class("-error")
                state.update("Query failed · Ctrl+R to retry")
                self.resource_message(
                    safe_text(
                        f"{error}\nCtrl+R to retry."
                        if not self.state.rows
                        else f"Showing retained results · {error}"
                    ),
                    error=True,
                )
                if self.state.rows:
                    state.update(safe_text(f"Retained results · {error} · Ctrl+R to retry"))
                await self.refresh_context()
            return
        if version != self.version:
            return
        page = self.state
        page.rows = {row.id: row for row in rows}
        page.more = more
        self.render_rows()
        state.remove_class("-loading")
        if not rows:
            record_type = {
                "Raw": "raw archives",
                "Normalized": "normalized publications",
                "Merged": "merged publications",
                "Converted": "converted outputs",
                "Retained": "retained runs",
                "Indexed": "indexed runs",
                "Owned": "owned runs",
            }.get(view, "recipes" if area == "Workflows" else view.lower())
            message = (
                f"No {record_type} match the current search or filters."
                if search or page.filters
                else f"No {record_type} available."
            )
            self.resource_message(message)
        state.update(
            f"{self.search_scope()}  ·  {len(rows)} results · Page {offset // self.page_size() + 1}"
            + (f" · {page.sort} {'↓' if page.descending else '↑'}" if page.sort else "")
            + (
                " · "
                + ", ".join(
                    f"{key.replace('_', ' ')}={value}" for key, value in page.filters.items()
                )
                if page.filters
                else ""
            )
            + ("  ·  PgDn next page" if more else "")
        )
        await self.refresh_context()

    @work(exclusive=True, group="overview-history", exit_on_error=False)
    async def load_overview_history(self, version: int) -> None:
        failed = False
        truncated = False
        try:
            async with self.overview_history_lock:
                if version != self.version:
                    return
                retained, truncated = await disk(
                    run_page, self.context.paths.runs_dir, 50, 0, include_managed=True
                )
        except Exception:
            failed = True
            retained = self.overview_retained
        if version != self.version or self.area != "Overview":
            return
        registry_failed = False
        try:
            with (
                database_environment(testing=self.context.testing),
                pinned_registry(self.database_id),
            ):
                indexed: list[dict[str, object]] = [
                    dict(item) for item in await workspace_runs(self.context.paths)
                ]
        except Exception:
            indexed = self.overview_indexed
            registry_failed = True
        if self.area != "Overview" or version != self.version:
            return
        self.overview_indexed = indexed
        self.overview_incomplete = failed or truncated or registry_failed
        indexed_by_id: dict[str, dict[str, object]] = {
            str(item["run_id"]): {**item, "workspace_indexed": True} for item in indexed
        }
        for item in retained:
            identity = str(item.get("run_id"))
            environment = item.get("environment") or item.get("database_purpose")
            registry = item.get("registry_id") or item.get("database_id")
            if environment is not None and environment != self.database_environment:
                continue
            if registry is not None and self.database_id and str(registry) != self.database_id:
                continue
            if identity not in indexed_by_id and not (
                environment == self.database_environment
                or (registry is not None and str(registry) == self.database_id)
            ):
                continue
            if current := indexed_by_id.get(identity):
                indexed_by_id[identity] = {**current, **item, "workspace_indexed": True}
            else:
                indexed_by_id[identity] = item
        self.overview_retained = list(indexed_by_id.values())
        self.overview_history_note = (
            "Indexed history unavailable · retained and cached rows where available"
            if registry_failed
            else "Retained history unavailable"
            if failed
            else ""
            if self.overview_retained
            else "No attributable runs"
        )
        self.overview_recovered.update(
            key
            for item in self.overview_retained
            if item.get("registration") == "registered"
            and (key := overview.execution_key(document(item.get("execution")))) is not None
        )
        rows = overview.recent_rows(self.overview_retained, [])
        ordered = sorted(
            rows.values(),
            key=lambda row: (
                when(document(row.value).get("started_at")) or datetime.min.replace(tzinfo=UTC)
            ),
            reverse=True,
        )
        page = self.state
        page.rows = {row.id: row for row in ordered[: self.overview_run_limit()]}
        self.render_rows()
        self.paint_overview()

    def overview_run_limit(self) -> int:
        return min(8, max(5, self.size.height // 6))

    @work(exclusive=True, group="overview-inventory", exit_on_error=False)
    async def refresh_inventory(self, generation: int) -> None:
        try:
            with (
                database_environment(testing=self.context.testing),
                pinned_registry(self.database_id),
            ):
                value = await workspace_inventory.summary(self.context.paths)
        except Exception:
            if generation == self.inventory_generation and self.area == "Overview":
                self.inventory_failed = True
        else:
            if generation == self.inventory_generation and self.area == "Overview":
                self.inventory_data, self.inventory_failed = value, False
        finally:
            if generation == self.inventory_generation and self.area == "Overview":
                self.paint_inventory()
                if self.inventory_pending:
                    self.inventory_pending = False
                    self.call_after_refresh(self.request_inventory)

    def paint_inventory(self) -> None:
        if self.area == "Overview" and self.root_screen is not None:
            self.root_screen.query_one(dashboard.WorkspaceDashboard).show_summary(
                self.inventory_data, self.inventory_failed
            )

    def paint_overview(self) -> None:
        if self.root_screen is None or self.area != "Overview":
            return
        reports = list(self.overview_retained)
        for item in self.overview_retained:
            for stage, evidence in document(item.get("stages")).items():
                detail = document(evidence)
                if detail.get("started_at") or detail.get("finished_at"):
                    reports.append({**document(detail.get("result")), **detail, "operation": stage})
        for op in self.controller.history[-16:]:
            report = document(op.result)
            reports.append(
                {
                    **report,
                    "operation": op.title.split()[0],
                    "state": op.state,
                    "started_at": report.get("started_at") or op.started_at,
                    "owned_active": op is self.controller.current and self.controller.active(),
                }
            )
        self.root_screen.query_one(dashboard.WorkspaceDashboard).show_activity(
            reports, self.overview_incomplete
        )
        note = self.root_screen.query_one("#overview-history-note", Static)
        note.update(self.overview_history_note)
        note.display = bool(self.overview_history_note)

    @work(exclusive=True, group="workspace-stage", exit_on_error=False)
    async def load_inventory_page(self) -> None:
        self.inventory_page_generation += 1
        generation = self.inventory_page_generation
        stage = self.inventory_stage
        offset = self.inventory_pages[stage]
        table = self.query_one("#workspace-inventory", dashboard.OverviewTable)
        table.clear(columns=True)
        self.inventory_rows.clear()
        self.inventory_more = False
        message = self.query_one("#workspace-stage-message", Static)
        message.update("Loading recorded inventory…")
        for index, label in enumerate(dashboard.STAGES):
            self.query_one(f"#workspace-stage-{index}", Button).set_class(stage == label, "active")
        try:
            with (
                database_environment(testing=self.context.testing),
                pinned_registry(self.database_id),
            ):
                rows, more = await workspace_inventory.stage_page(self.context.paths, stage, offset)
        except Exception:
            if (cache := self.inventory_page_cache.get(stage)) is None or cache[0] != offset:
                if self.area == "Overview" and generation == self.inventory_page_generation:
                    message.update("Inventory unavailable")
                    table.styles.height = 1
                return
            _, rows, more = cache
            stale = " · Stale"
        else:
            self.inventory_page_cache[stage] = (offset, rows, more)
            stale = ""
        if self.area != "Overview" or generation != self.inventory_page_generation:
            return
        self.inventory_rows = {str(index): item for index, item in enumerate(rows)}
        self.inventory_more = more
        message.update(
            f"Page {offset // workspace_inventory.PAGE_SIZE + 1} · PgUp/PgDn to page{stale}"
            if rows
            else "No eligible recorded inventory" + stale
        )
        self.render_inventory_table()

        table.styles.height = min(9, max(2, len(rows) + 2))
        if rows and (saved := self.inventory_selection.get(stage)):
            row, x, y = saved
            table.move_cursor(row=min(row, len(rows) - 1), scroll=False)
            table.scroll_to(x, y, animate=False, immediate=True)

    def render_inventory_table(self) -> None:
        table = self.query_one("#workspace-inventory", dashboard.OverviewTable)
        first = next(iter(self.inventory_rows.values()), None)
        if first is None:
            return
        cursor = table.cursor_row
        table.clear(columns=True)
        layout = dashboard.stage_layout(
            dashboard.stage_columns(self.inventory_stage, first),
            table.size.width or self.size.width,
        )
        for cell in layout:
            table.add_column(cell.label, width=cell.width, key=cell.label)
        for key, row in self.inventory_rows.items():
            cells = {
                cell.label: cell for cell in dashboard.stage_columns(self.inventory_stage, row)
            }
            table.add_row(
                *(dashboard.stage_cell(cells[cell.label], cell.width) for cell in layout), key=key
            )
        table.move_cursor(row=cursor, scroll=False)

    def save_inventory_view(self) -> None:
        table = self.query_one("#workspace-inventory", dashboard.OverviewTable)
        self.inventory_selection[self.inventory_stage] = (
            table.cursor_row,
            table.scroll_x,
            table.scroll_y,
        )

    def page_inventory(self, step: int) -> None:
        stage = self.inventory_stage
        current = self.inventory_pages[stage]
        if (step > 0 and self.inventory_more) or (step < 0 and current):
            self.inventory_pages[stage] = max(0, current + step * workspace_inventory.PAGE_SIZE)
            self.inventory_selection.pop(stage, None)
            self.load_inventory_page()

    @on(dashboard.OverviewTable.Activated)
    def activate_dashboard_cell(self, event: dashboard.OverviewTable.Activated) -> None:
        if event.table_id == "overview-results":
            if row := self.state.rows.get(event.row):
                if event.column == 0:
                    self.copy_dashboard_id(row.id)
                else:
                    self.open_overview_run(row)
        elif item := self.inventory_rows.get(event.row):
            identity = item.get("publication") or item.get("output_ref")
            table = self.query_one("#workspace-inventory", dashboard.OverviewTable)
            column = tuple(table.columns)[event.column].value if event.column >= 0 else None
            if column in {"Pub ID", "Output"} and identity is not None:
                self.copy_dashboard_id(str(identity))
            else:
                self.inspect_inventory_item(item)

    @on(DataTable.RowSelected, "#workspace-inventory")
    def select_dashboard_inventory(self, event: DataTable.RowSelected) -> None:
        if item := self.inventory_rows.get(event.row_key.value or ""):
            self.inspect_inventory_item(item)

    def inspect_inventory_item(self, item: dict[str, object]) -> None:
        self.save_inventory_view()
        if identity := item.get("publication"):
            self.open_overview_publication(int(str(identity)))
        elif reference := item.get("output_ref"):
            self.open_indexed_output(str(reference))
        else:
            self.inspect_archive_window(item)

    @work(exit_on_error=False)
    async def inspect_archive_window(self, item: dict[str, object]) -> None:
        value = item.get("archives")
        refs = [str(part) for part in cast(list[object], value)] if isinstance(value, list) else []
        try:
            with (
                database_environment(testing=self.context.testing),
                pinned_registry(self.database_id),
            ):
                rows = await workspace_inventory.archive_group(
                    self.context.paths, [int(ref.removeprefix("local:")) for ref in refs]
                )
        except Exception:
            rows = []
        by_id = {str(row.get("local_id")): row for row in rows}

        def choose(_: str, entry: Mapping[str, object]) -> None:
            self.inspect_archive(str(entry["Archive ID"]))

        actions: dict[str, Callable[[], object]] = {
            "Copy archive IDs": partial(self.copy_dashboard_id, "\n".join(refs))
        }
        if self.context.testing and self.database_id and refs and all(ref in by_id for ref in refs):
            row = Row(refs[0], value_at(item, "symbol"), "Raw Archive", "", item)
            actions["Add to Recipe"] = partial(
                self.add_data_to_recipe, "Raw", (row,), self.database_id, self.context.testing
            )
        sections: dict[str, object] = {
            "Source": {
                **{key: val for key, val in item.items() if key != "archives"},
                "Interval": "Recorded UTC start and exclusive end. Event continuity is unverified.",
            },
            "Selected archives": [
                {"Archive ID": ref, **by_id.get(ref, {"Metadata": "Unavailable"})} for ref in refs
            ],
        }
        if members := item.get("instruments"):
            sections["Instruments"] = members
        self.push_screen(
            Inspection(
                "Raw archive window",
                {
                    **{key: val for key, val in item.items() if key != "archives"},
                    "Semantics": "Recorded half-open archive windows. Event continuity is unverified.",
                },
                sections=sections,
                actions=actions,
                choose=choose,
            )
        )

    @work(exit_on_error=False)
    async def copy_dashboard_id(self, identity: str) -> None:
        await copy_text(self, identity)

    def overview_needs_registration(self, evidence: Mapping[str, object]) -> bool:
        return overview.registration_problem(evidence) and (
            overview.execution_key(evidence) not in self.overview_recovered
        )

    @work(exit_on_error=False)
    async def open_workspace_view(self, area: str, view: str) -> None:
        await self.switch(area, view)

    def open_overview_run(self, row: Row) -> None:
        if isinstance(row.value, Operation):
            self.show_operation(row.value)
        elif document(row.value).get("workspace_indexed"):
            self.open_indexed_run(row.id)
        else:
            self.open_managed_run(row.id)

    def search_scope(self) -> str:
        if self.area == "Data" and self.view == "Raw":
            return "Recorded complete local archives"
        if self.area == "Runs" and self.view == "Retained":
            return "Retained artifacts"
        if self.area == "Catalogue" and self.view == "Profiles":
            return "Managed workspace profiles"
        if (self.area in {"Data", "Catalogue"} and self.view != "Converted") or self.view in {
            "Instruments",
            "Rankings",
        }:
            return "Registry search"
        return "Indexed conversion outputs" if self.view == "Converted" else self.view

    def resource_message(self, text: str, *, error: bool = False) -> None:
        if self.root_screen is None or self.area in {"Overview", "Configs"}:
            return
        message = self.root_screen.query_one("#resource-state", Static)
        empty = not self.state.rows
        message.display = empty
        self.resource_table().display = not empty
        message.set_class(error, "-error")
        if empty:
            message.update(text)

    def render_rows(self) -> None:
        if self.area == "Overview":
            self.render_recent_runs()
            return
        table = self.resource_table()
        page = self.state
        table.text_selection_enabled = True
        table.recipe_links = self.area == "Workflows" or self.view in {"Recipes", "Policies"}
        table.clear(columns=True)
        self.resource_message("Loading records…")
        first = next(iter(page.rows.values()), None)
        if first is None:
            return

        def row_columns(row: Row) -> tuple[Cell, ...]:
            if self.area == "Catalogue":
                validated = value_at(document(row.value), "sha256") in self.validated_profiles
                return catalogue_ui.columns(self.view, row, validated=validated)
            if self.area == "Data":
                return dashboard.stage_columns(self.view, document(row.value))
            return columns(self.view, row.name, row.kind, row.state, row.value)

        width = (self.root_screen or self.screen).query_one("#resource-body").size.width
        width = width or table.size.width or self.size.width - 4
        layout = (
            dashboard.stage_layout(row_columns(first), width)
            if self.area == "Data"
            else visible(row_columns(first), width)
        )
        table.add_column("", width=1, key="mark")
        for cell in layout:
            indicator = (
                (" ↓" if page.descending else " ↑")
                if self.heading_sort(cell.label) == page.sort
                else ""
            )
            table.add_column(cell.label + indicator, width=cell.width, key=cell.label)
        if page.selection not in page.rows:
            page.selection = first.id
        for row in page.rows.values():
            by_name = {cell.label: cell for cell in row_columns(row)}
            table.add_row(
                Text(
                    "✓" if row.id in page.basket else "›" if page.selection == row.id else "",
                    style=CYAN,
                ),
                *(
                    dashboard.stage_cell(by_name[c.label], c.width)
                    if self.area == "Data"
                    else cell_text(by_name[c.label])
                    if c.label in by_name
                    else Text("—")
                    for c in layout
                ),
                key=row.id,
            )
        table.move_cursor(row=tuple(page.rows).index(page.selection), scroll=False)
        table.scroll_to(*page.scroll, animate=False, immediate=True)

    def render_recent_runs(self) -> None:
        table = self.query_one("#overview-results", dashboard.OverviewTable)
        table.clear(columns=True)
        table.add_columns("Run ID", "Started UTC", "Operation", "Recipe", "Result", "Duration")
        for row in self.state.rows.values():
            item = document(row.value)
            action = dashboard.operation(item)
            duration = overview.run_duration(item)
            table.add_row(
                Text(row.id[:21] + "…" if len(row.id) > 22 else row.id),
                Text(dashboard.stamp(item.get("started_at"))),
                Text(dashboard.OPERATIONS.get(action, action)),
                Text(str(item.get("name") or item.get("recipe") or "—")[:24]),
                dashboard.result(dashboard.outcome(item)),
                Text(duration, justify="right"),
                key=row.id,
            )
        table.styles.height = max(2, len(self.state.rows) + 2)
        selected = self.state.selection
        if selected in self.state.rows:
            table.move_cursor(row=tuple(self.state.rows).index(selected), scroll=False)

    @on(Input.Changed, "#filter")
    def filter_changed(self, event: Input.Changed) -> None:
        if self.state.filter != event.value:
            self.state.filter = event.value
            self.state.offset = 0
            self.request_query()

    async def load_rows(
        self, area: str, view: str, search: str, offset: int
    ) -> tuple[list[Row], bool]:
        with database_environment(testing=self.context.testing), pinned_registry(self.database_id):
            return await load_rows(
                self.controller,
                area,
                view,
                search,
                offset,
                filters=self.state.filters,
                sort=self.state.sort,
                descending=self.state.descending,
            )

    def related(self, row: Row | None) -> tuple[str, ...]:
        if self.area == "Overview":
            return ("doctor", "pipeline run", "runs list")
        if self.area == "Data":
            return ()
        if self.area == "Runs":
            if row is not None and isinstance(row.value, Operation):
                return ()
            if row is not None and document(row.value).get("kind") == "historical_pipeline":
                return ("runs report", "runs logs", "pipeline resume")
            return ("runs report", "runs logs")
        if self.area == "Catalogue":
            return ()
        family = row.kind if row and row.path else FAMILIES.get(self.view)
        if family:
            return tuple(
                action.key
                for action in ACTIONS
                if action.key.rsplit(" ", 1)[0] == family
                and (row is None or not action.key.endswith(" new"))
                and action.key not in {"pipeline resume", "pipeline status"}
            )
        return ()

    def refresh_workflow_actions(self) -> None:
        area = (self.root_screen or self.screen).query_one("#workflow-actions", Horizontal)
        family = FAMILIES.get(self.view, "") if self.area == "Workflows" else ""
        verbs = WORKFLOW_CONTROLS.get(family, ())
        area.display = bool(verbs)
        row = self.state.rows.get(self.state.selection or "")
        for button in area.query(Button).results(Button):
            verb = (button.id or "").removeprefix("workflow-")
            button.display = verb in verbs
            button.disabled = row is None or not row.path
            button.tooltip = (
                f"{verb.title()} · {row.reference} · {self.database_environment.upper()}"
                if row and row.path
                else "Select a saved recipe first."
            )

    async def refresh_context(self) -> None:
        screen = self.root_screen or self.screen
        self.refresh_workflow_actions()
        row = self.state.rows.get(self.state.selection or "")
        inspect_button = screen.query_one("#inspect-selected", Button)
        inspect_button.disabled = row is None
        inspect_button.label = "Enter Inspect"
        screen.query_one("#edit-source", Button).display = bool(row and row.path)
        screen.query_one("#page-previous", Button).disabled = self.state.offset == 0
        screen.query_one("#page-next", Button).disabled = not self.state.more
        screen.query_one("#context-menu", Button).disabled = self.area != "Overview" and not (
            row or self.related(row) or self.area in {"Data", "Runs", "Catalogue", "Workflows"}
        )

    @on(DataTable.RowHighlighted, "#results, #overview-results")
    async def highlighted(self, event: DataTable.RowHighlighted) -> None:
        if not self.is_running:
            return
        if event.row_key.value in self.state.rows:
            old = self.state.selection
            self.state.selection = event.row_key.value
            if self.area != "Overview":
                self.resource_table().mark(old, self.state.selection, set(self.state.basket))
                await self.refresh_context()

    @on(DataTable.RowSelected, "#results, #overview-results")
    async def selected(self, event: DataTable.RowSelected) -> None:
        row = self.state.rows.get(event.row_key.value or "")
        if row is not None:
            if self.area == "Overview":
                self.open_overview_run(row)
            else:
                self.open_row(row)

    async def inspect_row(self, row: Row) -> None:
        if self.area == "Catalogue":
            await self.inspect_catalogue(row)
            return
        if row.kind == "Raw Archive":
            self.inspect_archive_window(dict(document(row.value)))
            return
        if row.kind == "Timeline":
            await self.inspect_timeline(row.reference)
            return
        if row.kind == "Indexed output file":
            part = value_at(document(row.value), "part", "")
            if part.isdecimal():
                self.open_indexed_output(row.reference, "schema", int(part))
            else:
                self.open_indexed_output(row.reference)
            return
        if row.kind == "Indexed conversion output":
            self.open_indexed_output(row.reference)
            return
        if row.kind == "Converted output":
            self.push_screen(
                Inspection(
                    f"Converted output · {row.name}",
                    row.value,
                    actions={
                        "Execution report": partial(self.open_managed_run, row.id),
                        "View logs": partial(self.open_overview_logs, row.id),
                    },
                )
            )
            return
        if row.kind == "Indexed run":
            self.open_indexed_run(row.id)
            return
        if self.area == "Runs" and not isinstance(row.value, Operation):
            self.open_managed_run(row.id)
            return
        if isinstance(row.value, Operation):
            self.show_operation(row.value)
            return
        if self.area == "Overview":
            self.open_overview_run(row)
            return
        actions: dict[str, Callable[[], object]] = {
            "Copy Full Identifier": lambda: self.copy_value(row.reference or row.id)
        }
        if row.path:
            actions["Edit TOML"] = partial(self.edit_selected, row)
        for key in self.related(row) if self.area != "Markets" else ():
            actions[key.rsplit(" ", 1)[-1].title()] = lambda key=key: self.open_action(
                BY_KEY[key], row
            )
        detail = row.value
        if row.kind == "Publication":
            await self.inspect_publication(int(row.reference or row.id))
            return
        if row.path:
            try:
                source = await disk(read_source, row.path)
                data = tomllib.loads(source)
                if model_for(row.kind, data):
                    self.push_screen(
                        Inspection(
                            row.reference,
                            {**document(row.value), "configuration": data, "source_toml": source},
                            sections={
                                **recipe_sections(row.kind, data, row.state, str(row.path)),
                                **await self.recipe_source_evidence(row.kind, data),
                                **await self.recipe_run_evidence(row, source),
                            },
                            actions=self.saved_recipe_actions(row.kind, row.reference, row.path),
                            evidence_label="F3 Advanced details",
                            files=self.inspection_files(
                                "Recipe TOML", row.path, self.context.paths.config_dir
                            ),
                            section_values={
                                "Configuration": tomllib.loads(source, parse_float=Decimal)
                            },
                        )
                    )
                    return
                detail = {**document(row.value), "source_toml": source}
            except (OSError, ValueError) as error:
                detail = {"error": str(error), "path": str(row.path)}
        selected = self.selection(row)
        if selected.kind == "file":
            actions.update(self.file_links(int(selected.identity)))
        self.push_screen(
            Inspection(
                row.name,
                detail,
                actions=actions,
                scalar_summary=self.area == "Markets",
                files=self.inspection_files("Recipe TOML", row.path, self.context.paths.config_dir),
            )
        )

    async def recipe_run_evidence(self, row: Row, source: str) -> dict[str, object]:
        if (
            self.database_id is None
            or row.path is None
            or row.kind not in {"acquisition", "normalize", "merge", "convert", "pipeline"}
        ):
            return {}
        path = await disk(row.path.resolve)
        fingerprint = sha256(source.encode()).hexdigest()
        matched: list[dict[str, object]] = []
        try:
            with (
                database_environment(testing=self.context.testing),
                pinned_registry(self.database_id),
            ):
                page = await list_index(
                    self.context.paths,
                    limit=8,
                    operation=row.kind,
                    recipe=row.reference,
                )
                for run in records(page, "runs"):
                    run_id = value_at(run, "run_id")
                    if not run_id:
                        continue
                    run_record = await show_index(self.context.paths, run_id)
                    for attempt in records(run_record, "attempts"):
                        attempt_id = str(attempt.get("attempt_id"))
                        outcome = attempt.get("outcome_path")
                        if not isinstance(outcome, str):
                            continue
                        summary = await disk(
                            retained_summary,
                            self.context.paths.runs_dir,
                            run_id,
                            attempt_id,
                            Path(outcome),
                        )
                        provenance = document(summary.get("recipe_source"))
                        if (
                            provenance.get("path") != str(path)
                            or provenance.get("sha256") != fingerprint
                        ):
                            continue
                        matched.append(
                            {
                                "run_id": run_id,
                                "attempt_id": attempt.get("attempt_id"),
                                "action": attempt.get("action") or run.get("action"),
                                "recorded_state": attempt.get("status"),
                                "preparation": document(summary.get("preparation")),
                                "report_path": attempt.get("report_path"),
                            }
                        )
        except Exception as error:
            return {"Prior runs": {"Evidence": f"Retained run lookup unavailable: {error}"}}
        if matched:
            return {"Prior runs": recipe_configuration({"Previously recorded attempts": matched})}
        return {}

    async def recipe_source_evidence(
        self, family: str, data: dict[str, object]
    ) -> dict[str, object]:
        selection = document(data.get("selection"))
        source = document(data.get("source"))
        pinned = source.get("registry_id") or data.get("registry_id")
        if pinned and pinned != self.database_id:
            return {
                "Source evidence": {
                    "State": "Configured source belongs to another or unavailable registry."
                }
            }
        ids: dict[int, str] = {}
        for refs in (data.get("publication_ids"), selection.get("publication_ids")):
            if isinstance(refs, list):
                ids.update(
                    (ref, "Configured publication")
                    for ref in cast(list[object], refs)
                    if type(ref) is int
                )
        if type(ref := source.get("registry_publication_id")) is int:
            ids[ref] = "Configured source publication"
        if type(ref := data.get("reference_publication_id")) is int:
            ids[ref] = "Configured reference publication"
        archives = records(data, "archive_pins")
        primary_files = data.get("dataset_ids")
        files = cast(list[object], primary_files) if isinstance(primary_files, list) else []
        if family == "convert" and not files:
            selected_files = selection.get("file_dataset_ids")
            files = cast(list[object], selected_files) if isinstance(selected_files, list) else []
        evidence: dict[str, object] = {}
        with database_environment(testing=self.context.testing), pinned_registry(self.database_id):
            for ref, label in ids.items():
                try:
                    page = await publications(self.context.paths, identity=ref)
                    matches = records(page, "publications")
                    if not matches:
                        raise ValueError("Publication unavailable")
                    item = matches[0]
                    if label == "Configured source publication" and str(
                        item.get("engine_dataset_id")
                    ) != source.get("engine_publication_id"):
                        raise ValueError(
                            "Registry metadata disagrees with the configured Engine identity"
                        )
                    streams = records(item, "streams")
                    by_id = {
                        value_at(stream, "stream_id", value_at(stream, "id")): stream
                        for stream in streams
                    }
                    members = [
                        {
                            **by_id.get(value_at(member, "stream_id"), {}),
                            **{key: value for key, value in member.items() if value is not None},
                        }
                        for member in records(item, "registered_memberships")
                    ] or streams
                    evidence[f"{label} {ref}"] = {
                        "engine_publication_id": item.get("engine_dataset_id"),
                        "generation": item.get("generation"),
                        "integrity": item.get("integrity"),
                        "membership": list(members) or "Membership unavailable",
                    }
                except Exception as error:
                    evidence[f"{label} {ref}"] = str(error)
            for ref in files:
                if type(ref) is not int:
                    continue
                try:
                    item = await browse(self.context.paths, DatasetRequest("show", id=ref))
                    evidence[f"Primary file {ref}"] = item or "Metadata unavailable"
                except Exception as error:
                    evidence[f"Primary file {ref}"] = str(error)
            for archive in archives:
                ref = str(archive.get("archive_id"))
                try:
                    page = await self.catalogue_call("storage show", {"archive": ref})
                    matches = records(page, "archives")
                    evidence[f"Raw archive {ref}"] = (
                        dict(matches[0]) if matches else "Metadata unavailable"
                    )
                except Exception as error:
                    evidence[f"Raw archive {ref}"] = str(error)
        if family == "merge" and (reference := selection.get("source")):
            from forge.historical.merge.config import normalization_source

            try:
                path, recipe, fingerprint = await disk(
                    normalization_source, self.context.paths, str(reference)
                )
                evidence["Current upstream recipe declaration"] = {
                    "path": str(path),
                    "sha256": fingerprint,
                    "requests": recipe.model_dump(mode="json")["requests"],
                }
            except Exception as error:
                evidence["Current upstream recipe declaration"] = str(error)
            evidence["Resolved source"] = (
                "No current preparation evidence established by this view."
            )
        if family == "convert" and source.get("reference"):
            from forge.historical.conversion.recipes import LoadedSource, Recipe, validate_binding

            try:
                recipe = Recipe.model_validate(data)
                binding = await disk(validate_binding, self.context.paths, Path("recipe"), recipe)
                if isinstance(binding, LoadedSource):
                    evidence["Configured upstream recipe declaration"] = binding.config.model_dump(
                        mode="json", exclude_unset=True
                    )
            except Exception as error:
                evidence["Configured upstream recipe declaration"] = str(error)
            evidence["Resolved source"] = (
                "No current preparation evidence established by this view."
            )
        if family == "normalize" and (archives or data.get("requests")):
            evidence["Profile resolution"] = (
                "See configured profile pins. Automatic profile resolution is established during preparation."
            )
        return {"Source evidence": recipe_configuration(evidence)} if evidence else {}

    @work(exit_on_error=False)
    async def open_indexed_output(
        self, reference: str, action: str = "show", part: int | None = None
    ) -> None:
        with database_environment(testing=self.context.testing), pinned_registry(self.database_id):
            try:
                result = await browse(
                    self.context.paths,
                    DatasetRequest(action, output_ref=reference, part=part, limit=25, precise=True),
                )
            except Exception as error:
                self.push_screen(
                    Detail("Indexed conversion output", str(error), raw=True, error=True)
                )
                return
        links: dict[str, Callable[[], object]] = {
            "Copy output reference": partial(self.copy_value, reference)
        }
        if self.context.testing and action == "show":
            row = Row(reference, reference, "Indexed conversion output", "", {}, reference)
            links["Preview output deletion…"] = partial(self.delete_publications, (row,))
        if action == "show":
            links["Observed timelines"] = partial(self.inspect_inventory_timelines, reference)
            links["Physical partitions"] = partial(
                self.inspect_partitions, {"output_ref": reference}
            )
        if action != "show":
            self.push_screen(
                Inspection(f"Conversion {action} · {reference}", result, actions=links)
            )
            return

        output = document(result.get("output"))
        if execution := output.get("producing_execution"):
            links["Open producing run"] = partial(self.open_managed_run, str(execution))
        files = [
            {"Part": number, **item} for number, item in enumerate(records(output, "files"), 1)
        ]
        for verb, label in (
            ("streams", "Source streams"),
            ("lineage", "Lineage"),
            ("verify", "Verify output"),
        ):
            links[label] = partial(self.open_indexed_output, reference, verb)
        if manifest := output.get("manifest_path"):
            links["Copy manifest path"] = partial(self.copy_value, str(manifest))

        def choose(section: str, item: Mapping[str, object]) -> None:
            if section == "Streams":
                self.push_screen(Inspection("Converted output source stream", item))
                return
            number = int(value_at(item, "Part"))
            actions: dict[str, Callable[[], object]] = {
                "Copy path": partial(self.copy_value, value_at(item, "path"))
            }
            for verb, label in (
                ("schema", "Schema"),
                ("head", "Sample"),
                ("verify", "Verify part"),
            ):
                actions[label] = partial(self.open_indexed_output, reference, verb, number)
            self.push_screen(Inspection(f"Converted file · part {number}", item, actions=actions))

        source = document(output.get("indexed_result"))
        conversion = document(output.get("conversion"))
        summary = {**output}
        if source:
            source_input = document(source.get("input"))
            summary["Source registry publication"] = source_input.get("registry_publication_id")
            identity = value_at(source_input, "registry_publication_id", "")
            if identity.isdecimal():
                links["Open source merged publication"] = partial(
                    self.open_overview_publication, int(identity)
                )
        members = records(output.get("summary"), "memberships")
        references = conversion.get("references")
        if isinstance(references, list):
            summary["Selected reference streams"] = len(cast(list[object], references))
        sections: dict[str, object] = {
            "Summary": summary,
            "Files": files,
            "Streams": members,
            "Conversion": conversion,
        }
        if references:
            sections["References"] = references
        if statistics := output.get("statistics"):
            sections["Statistics"] = statistics
        if publication_evidence := source.get("publication"):
            sections["Evidence"] = {
                **document(publication_evidence),
                **document(output.get("integrity_evidence")),
            }
        self.push_screen(
            Inspection(
                f"Conversion output {reference}",
                output,
                actions=links,
                sections=sections,
                choose=choose,
                files=self.inspection_files(
                    "Manifest", output.get("manifest_path"), self.context.paths.datasets_dir
                ),
            )
        )

    async def inspect_timeline(self, reference: str) -> None:
        with database_environment(testing=self.context.testing), pinned_registry(self.database_id):
            try:
                result = await browse(
                    self.context.paths, DatasetRequest("timeline", timeline_reference=reference)
                )
            except Exception as error:
                self.push_screen(Detail("Timeline", str(error), raw=True, error=True))
                return
        timeline = document(result.get("timeline"))

        def choose(_: str, item: Mapping[str, object]) -> None:
            identity = value_at(item, "registry_publication_id", "")
            if identity.isdecimal():
                self.open_overview_publication(int(identity))
            elif output := value_at(item, "output_ref", ""):
                self.open_indexed_output(output)

        self.push_screen(
            Inspection(
                "Observed timeline",
                timeline,
                sections={"Summary": timeline, "Publications": timeline.get("publications")},
                choose=choose,
            )
        )

    @work(exit_on_error=False)
    async def inspect_inventory_timelines(self, identity: int | str, offset: int = 0) -> None:
        with database_environment(testing=self.context.testing), pinned_registry(self.database_id):
            try:
                result = await browse(
                    self.context.paths,
                    DatasetRequest(
                        "list",
                        view="timelines",
                        offset=offset,
                        limit=workspace_inventory.PAGE_SIZE,
                        publication_id=identity if isinstance(identity, int) else None,
                        output_ref=identity if isinstance(identity, str) else None,
                    ),
                )
            except Exception as error:
                self.push_screen(Inspection("Observed timelines", {"error": str(error)}))
                return

        @work(exit_on_error=False)
        async def open_timeline(app: ForgeApp, reference: str) -> None:
            await app.inspect_timeline(reference)

        def choose(_: str, entry: Mapping[str, object]) -> None:
            open_timeline(self, value_at(entry, "timeline_reference"))

        items = records(result, "timelines")
        links: dict[str, Callable[[], object]] = {}
        if result.get("has_more"):
            links["Next page"] = partial(
                self.inspect_inventory_timelines, identity, offset + workspace_inventory.PAGE_SIZE
            )
        if offset:
            links["Previous page"] = partial(
                self.inspect_inventory_timelines, identity, offset - workspace_inventory.PAGE_SIZE
            )
        self.push_screen(
            Inspection(
                f"Observed timelines · {identity}",
                result,
                actions=links,
                sections={"Timelines": items},
                choose=choose,
            )
        )

    async def inspect_publication(
        self, identity: int, offset: int = 0, *, section: str = "Summary"
    ) -> None:
        try:
            with (
                database_environment(testing=self.context.testing),
                pinned_registry(self.database_id),
            ):
                value = await publications(
                    self.context.paths, identity=identity, file_offset=offset
                )
            matches = records(value, "publications")
            if not matches:
                raise ValueError("The retained publication is unavailable.")
            publication = matches[0]
        except Exception as error:
            self.push_screen(Inspection("Publication evidence", {"error": str(error)}))
            return
        engine = value_at(publication, "engine_dataset_id")
        row = Row(
            str(identity),
            engine,
            "Publication",
            value_at(publication, "operation"),
            publication,
            "",
        )

        def choose(section: str, item: Mapping[str, object]) -> None:
            if section == "Streams":
                stream = Selection("stream", value_at(item, "stream_id", value_at(item, "id")))
                self.push_screen(
                    Inspection(
                        f"Stream {stream.identity}",
                        item,
                        actions={"Copy stream ID": partial(self.copy_value, stream.identity)},
                    )
                )
                return
            if section != "Files":
                return
            file_id = value_at(item, "registry_primary_file_id")
            if file_id.isdecimal():
                self.inspect_file(int(file_id), value_at(item, "primary_file"))

        links: dict[str, Callable[[], object]] = {
            "Copy Full Identifier": partial(self.copy_value, engine or str(identity)),
        }
        if publication.get("primary_files_truncated"):
            links["Next file page"] = partial(self.publication_page, identity, offset + 50)
        if offset:
            links["Previous file page"] = partial(
                self.publication_page, identity, max(0, offset - 50)
            )
        if self.database_id and publication.get("operation") == "merge":
            links["Add to Recipe"] = partial(
                self.add_data_to_recipe, "Merged", (row,), self.database_id, self.context.testing
            )
        elif publication.get("operation") == "normalize":
            links["Browse Normalized Streams"] = partial(self.browse_data_publication, identity)
        links["Observed timelines"] = partial(self.inspect_inventory_timelines, identity)
        links["View supersession evidence"] = partial(self.inspect_supersession, identity)
        links["Physical partitions"] = partial(
            self.inspect_partitions, {"publication_id": identity}
        )
        if self.context.testing:
            links["Preview publication deletion…"] = partial(self.delete_publications, (row,))
        files = records(publication, "primary_files")
        if files:
            anchor = int(value_at(files[0], "registry_primary_file_id"))
            links["Publication lineage"] = partial(
                self.inspect_publication_lineage, anchor, identity
            )
        execution = document(publication.get("execution"))
        if execution.get("execution_id"):
            links["Related execution"] = partial(
                self.open_managed_run, str(execution["execution_id"])
            )
        summary = {
            key: val
            for key, val in publication.items()
            if key not in {"streams", "primary_files", "execution"}
        }
        streams = records(publication, "streams")
        members: dict[tuple[str, ...], Mapping[str, object]] = {}
        for member in records(publication, "registered_memberships"):
            key = tuple(
                value_at(member, name)
                for name in ("exchange", "instrument_id", "stream_id", "kind")
            )
            members[key] = {
                name: member[name]
                for name in (
                    "exchange",
                    "symbol",
                    "instrument_id",
                    "instrument_type",
                    "market_category",
                    "kind",
                    "stream_id",
                )
                if name in member
            }
        known_streams = {value_at(member, "stream_id") for member in members.values()}
        streams = [
            *members.values(),
            *(stream for stream in streams if value_at(stream, "stream_id") not in known_streams),
        ]
        summary["Instruments"] = membership(streams, "symbol")
        summary["Stream count"] = len(streams) if publication.get("streams") is not None else None
        requested = document(publication.get("requested_interval"))
        for key, label in (
            ("start_ns", "Requested start UTC"),
            ("end_ns", "Requested end UTC (exclusive)"),
        ):
            bound = requested.get(key)
            summary[label] = utc(bound) if isinstance(bound, int) else bound
        for key, label in (
            ("first_observed_ns", "First observation UTC"),
            ("last_observed_ns", "Last observation UTC"),
        ):
            bound = publication.get(key)
            summary[label] = utc(bound) if isinstance(bound, int) else bound
        summary["Registry ID"] = self.database_id
        summary["File offset"] = offset
        summary["Scope"] = (
            "Registry publication. Choose a primary file for schema, sampling, or verification."
        )
        self.push_screen(
            Inspection(
                f"Publication {identity}  ·  Engine {engine}",
                publication,
                actions=links,
                sections={
                    "Summary": summary,
                    "Streams": streams,
                    "Files": files,
                    "Evidence": {
                        "manifest_path": publication.get("manifest_path"),
                        "manifest_sha256": publication.get("manifest_sha256"),
                    },
                },
                choose=choose,
                section=section,
                files=self.inspection_files(
                    "Manifest", publication.get("manifest_path"), self.context.paths.datasets_dir
                ),
            )
        )

    @work(exit_on_error=False)
    async def publication_page(self, identity: int, offset: int) -> None:
        if isinstance(self.screen, Inspection):
            self.pop_screen()
        await self.inspect_publication(identity, offset, section="Files")

    @work(exit_on_error=False)
    async def inspect_publication_lineage(self, file_id: int, publication: int) -> None:
        await self.inspect_request(
            "datasets lineage",
            {"id": file_id, "publication": True, "checksums": False, "limit": 25},
            f"Publication {publication} / Lineage",
        )

    @work(exit_on_error=False)
    async def inspect_file(self, identity: int, name: str) -> None:
        await self.inspect_request(
            "datasets show",
            {"id": identity},
            f"Primary file {identity}  ·  {name}",
            self.file_links(identity),
        )

    @work(exit_on_error=False)
    async def inspect_partitions(self, values: Values) -> None:
        try:
            with (
                database_environment(testing=self.context.testing),
                pinned_registry(self.database_id),
            ):
                response = await self.controller.invoke(
                    "datasets partitions", values, self.context, lambda _: None, self.edit_recipe
                )
        except Exception as error:
            self.push_screen(Detail("Physical partitions", str(error), raw=True, error=True))
            return

        def choose(_: str, item: Mapping[str, object]) -> None:
            reference = value_at(item, "reference")
            if not reference:
                return
            selection = {**values, "partition": reference}
            actions: dict[str, Callable[[], object]] = {
                "Copy partition reference": partial(self.copy_value, reference),
                "Schema": partial(
                    self.inspect_dataset_selection, "datasets schema", selection, "Partition schema"
                ),
                "Query preview…": partial(
                    self.open_action, BY_KEY["datasets head"], preset=selection
                ),
                "Row groups": partial(
                    self.inspect_dataset_selection,
                    "datasets partitions",
                    {**selection, "row_groups": True, "offset": 0},
                    "Parquet row groups",
                ),
            }
            paths = item.get("paths")
            if isinstance(paths, list):
                actions["Copy physical paths"] = partial(
                    self.copy_value, "\n".join(str(path) for path in cast(list[object], paths))
                )
            self.push_screen(Inspection(reference, item, actions=actions))

        links: dict[str, Callable[[], object]] = {}
        offset = int(values.get("offset") or 0)
        limit = int(values.get("limit") or 50)
        total = document(response).get("total")
        if offset:
            links["Previous partition page"] = partial(
                self.inspect_partitions, {**values, "offset": max(0, offset - limit)}
            )
        if isinstance(total, int) and offset + limit < total:
            links["Next partition page"] = partial(
                self.inspect_partitions, {**values, "offset": offset + limit}
            )
        self.push_screen(
            Inspection(
                "Physical partitions", response, choose=choose, section="Partitions", actions=links
            )
        )

    def file_links(self, identity: int) -> dict[str, Callable[[], object]]:
        selection = Row(str(identity), str(identity), "File", "", {}, str(identity))
        return {
            "Physical partitions": partial(self.inspect_partitions, {"id": identity}),
            **{
                label: partial(self.open_action, BY_KEY[key], selection)
                for label, key in (
                    ("File schema", "datasets schema"),
                    ("Bounded file sample", "datasets head"),
                    ("File lineage", "datasets lineage"),
                    ("Verify primary file", "datasets verify"),
                )
            },
        }

    @work(exit_on_error=False)
    async def inspect_dataset_selection(self, key: str, values: Values, label: str) -> None:
        await self.inspect_request(key, values, label)

    def show_run_inspection(
        self,
        result: RunView,
        identity: str,
        key: str,
        selected_context: ApplicationContext,
        selected_registry: str | None,
        *,
        links: dict[str, Callable[[], object]] | None = None,
        show_errors: bool = False,
    ) -> None:
        if self.context is not selected_context or self.database_id != selected_registry:
            self.notify("Reopen the run in its selected registry.", severity="warning")
            return
        actions = {
            **(links or {}),
            **self.overview_run_links(identity, result.evidence),
        }
        if result.log_files:
            actions["Open retained log…"] = partial(self.retained_log, result.log_files)
        if result.command:
            actions["Copy CLI command"] = partial(self.copy_value, result.command)
        for key_name, label_name in (
            ("report_path", "report path"),
            ("artifact_root", "artifact directory"),
        ):
            if path := result.evidence.get(key_name):
                actions[f"Copy {label_name}"] = partial(self.copy_value, str(path))

        def choose(section: str, entry: Mapping[str, object]) -> None:
            if section == "Child runs":
                open_child(value_at(entry, "Run ID"))

        reader = result.diagnostics

        async def load_diagnostics() -> DiagnosticPage:
            if self.context is not selected_context or self.database_id != selected_registry:
                raise MarketForgeError("Reopen the run in its original registry environment.")
            assert reader is not None
            with (
                database_environment(testing=selected_context.testing),
                pinned_registry(selected_registry),
            ):
                return await reader.load()

        def label_errors(label: str) -> None:
            button = inspection.query_one("#inspect-errors", Button)
            button.label = label
            button.refresh(layout=True)

        def open_child(identity: str) -> None:
            if self.context is selected_context and self.database_id == selected_registry:
                self.open_indexed_run(identity)

        inspection = Inspection(
            result.title,
            result.evidence,
            sections=result.sections,
            section_values=({"Summary": result.evidence} if key == "runs report" else None),
            section="Errors" if show_errors else None,
            files=result.files,
            actions=actions,
            searchable=key == "runs logs",
            choose=choose,
            errors=ErrorsPanel(reader, load_diagnostics, open_child, label_errors)
            if reader is not None
            else None,
        )
        self.push_screen(inspection)

    async def inspect_request(
        self,
        key: str,
        values: Values,
        label: str,
        links: dict[str, Callable[[], object]] | None = None,
        *,
        show_errors: bool = False,
    ) -> None:
        selected_registry, selected_context = self.database_id, self.context
        try:
            with pinned_registry(selected_registry):
                result = await self.controller.invoke(
                    key, values, selected_context, lambda _: None, self.edit_recipe
                )
            if key == "pipeline status" and document(result).get("kind") == "historical_pipeline":
                result = await self.retained_run_view(
                    str(values["reference"]), selected_context, selected_registry
                )
            if isinstance(result, RunView):
                self.show_run_inspection(
                    result,
                    str(values.get("reference", "")),
                    key,
                    selected_context,
                    selected_registry,
                    links=links,
                    show_errors=show_errors,
                )
                return
            if key == "storage show" and (archives := records(result, "archives")):
                archive = archives[0]
                provenance = {
                    name: archive.get(name)
                    for name in (
                        "raw_source_key",
                        "external_source_key",
                        "provider",
                        "product",
                        "feed",
                        "feed_source",
                        "source_object_key",
                        "acquisition_method",
                        "format_profile",
                        "external_source_root",
                        "downloaded_at",
                        "last_verified_at",
                    )
                }
                archive_fields = {
                    **archive,
                    "Interval evidence": (
                        "Source-declared, exclusive end"
                        if archive.get("origin") == "external"
                        else "Registry-recorded archive interval, exclusive end"
                    ),
                }
                sections: dict[str, object] = {"Archive": archive_fields, "Source": provenance}
                for name, title in (
                    ("source_metadata", "Source metadata"),
                    ("import_provenance", "Import provenance"),
                    ("identity_json", "Registered scope"),
                ):
                    if evidence := archive.get(name):
                        sections[title] = evidence
                self.push_screen(Inspection(label, result, actions=links, sections=sections))
                return
            if key == "datasets show":
                file = document(document(result).get("file"))
                links = {
                    **(links or {}),
                    "Copy primary-file ID": partial(self.copy_value, str(values.get("id", ""))),
                }
                if path := file.get("storage_path"):
                    links["Copy primary-file path"] = partial(self.copy_value, str(path))
                if isinstance(publication := file.get("publication_id"), int):
                    links["Open registry publication"] = partial(
                        self.open_overview_publication, publication
                    )
            if key == "datasets verify":
                result = {
                    **document(result),
                    "verification_scope": "Selected primary file and shared manifest",
                }
            self.push_screen(
                Inspection(
                    label,
                    result,
                    actions=links,
                    choose=self.follow_lineage if key == "datasets lineage" else None,
                )
            )
        except Exception as error:
            self.push_screen(Inspection(label, {"error": str(error)}))

    @work(exit_on_error=False)
    async def retained_log(self, files: dict[str, Path]) -> None:
        from forge.orchestration.log_files import export_log, read_lines

        choice = await self.push_screen_wait(ActionMenu("Retained logs", tuple(files)))
        if choice is None:
            return
        path = files[choice]
        action = await self.push_screen_wait(
            ActionMenu(
                choice, ("Read or copy a line range…", "Copy exact path", "Export complete log…")
            )
        )
        if action is None:
            return
        if action == "Copy exact path":
            self.copy_value(str(path))
            return
        try:
            if action == "Export complete log…":
                proposed = await self.push_screen_wait(
                    Form(
                        Action(
                            "Export complete retained log",
                            "Runs",
                            (Field("destination", "New destination path", required=True),),
                        ),
                        root=self.context.paths.root,
                        local=True,
                    )
                )
                if proposed is not None:
                    destination = self.context.paths.root / await disk(
                        Path(str(proposed["destination"])).expanduser
                    )
                    if await self.push_screen_wait(
                        Confirm("Export complete log", f"{path}\n→ {destination}")
                    ):
                        await disk(export_log, path, destination)
                        self.notify(f"Log exported to {destination}")
                return
            fields = (
                Field("first", "First line (inclusive)", "integer", default=1, required=True),
                Field("last", "Last line (inclusive)", "integer", default=100, required=True),
                Field("search", "Search text within range"),
                *((Field("severity", "Exact event severity"),) if choice == "events.jsonl" else ()),
            )
            values = await self.push_screen_wait(
                Form(Action("Read retained log lines", "Runs", fields), local=True)
            )
            if values is None:
                return
            content = await disk(
                read_lines,
                path,
                int(values["first"]),
                int(values["last"]),
                search=str(values["search"]),
                severity=str(values.get("severity", "")),
            )
            if content:
                self.push_screen(
                    Detail(
                        choice,
                        content,
                        raw=True,
                        editor_target=RetainedFile.reference(
                            "Retained log", path, self.context.paths.runs_dir
                        ),
                    )
                )
            else:
                self.notify("No matching log lines.")
        except Exception as error:
            self.push_screen(Detail("Retained log", str(error), raw=True, error=True))

    def follow_lineage(self, section: str, item: Mapping[str, object]) -> None:
        identity = value_at(item, "id")
        if not identity.isdecimal():
            return
        if section in {"Raw inputs", "Selected archives"}:
            self.inspect_archive(identity)
        else:
            self.inspect_file(int(identity), value_at(item, "primary_file"))

    @work(exit_on_error=False)
    async def inspect_archive(self, identity: str) -> None:
        await self.inspect_request(
            "storage show",
            {"archive": identity},
            f"Raw archive · {identity}",
            {"Copy exact archive ID": partial(self.copy_dashboard_id, identity)},
        )

    async def retained_run_view(
        self, identity: str, context: ApplicationContext, registry: str | None
    ) -> RunView:
        sources: dict[str, Path] = {}
        item = {
            **await disk(read_managed_report, context.paths.runs_dir, identity, sources=sources),
            "run_id": identity,
        }
        record = None
        if registry is not None:
            try:
                with database_environment(testing=context.testing), pinned_registry(registry):
                    record = await show_index(context.paths, identity)
            except DatabaseError:
                # Retained browsing also works without a registry.
                pass
        return await disk(
            project_run,
            RunInspection(identity, item, record, report_files=sources),
            context,
            "report",
        )

    @work(exit_on_error=False)
    async def open_managed_run(self, identity: str, *, show_errors: bool = False) -> None:
        context, registry = self.context, self.database_id
        try:
            view = await self.retained_run_view(identity, context, registry)
            self.show_run_inspection(
                view, identity, "runs report", context, registry, show_errors=show_errors
            )
        except Exception as error:
            self.push_screen(Detail(identity, str(error), raw=True, error=True))

    def overview_run_links(
        self, identity: str, item: Mapping[str, object]
    ) -> dict[str, Callable[[], object]]:
        links: dict[str, Callable[[], object]] = {"Copy run ID": partial(self.copy_value, identity)}
        if identity:
            links["Open report"] = partial(
                self.open_action, BY_KEY["runs report"], None, {"reference": identity}
            )
            links["View run logs"] = partial(
                self.open_action, BY_KEY["runs logs"], None, {"reference": identity}
            )
            if not MANAGED_RUN.fullmatch(identity) and overview.pipeline_recovery(item):
                links["Resume retained pipeline"] = partial(
                    self.open_action, BY_KEY["pipeline resume"], None, {"reference": identity}
                )
        if self.overview_needs_registration(item) and (family := identity.split("-")[0]) in {
            "normalize",
            "merge",
        }:
            links["Recover registration"] = partial(
                self.open_action,
                BY_KEY[family + " register"],
                None,
                {"reference": str(item["execution_receipt"])},
            )
        if MANAGED_RUN.fullmatch(identity):
            links["View engine logs"] = partial(self.open_overview_logs, identity)
        for candidate in (item, *overview.stages(item)):
            prepared = document(candidate.get("prepared"))
            for key, label in (
                ("batch", "Inspect prepared batch"),
                ("job", "Inspect prepared job"),
            ):
                reference = prepared.get(key)
                if isinstance(reference, str) and reference:
                    links[f"{label} · {Path(reference).name}"] = partial(
                        self.open_overview_artifact, reference
                    )
        self.add_registry_links(links, item)
        return links

    def add_registry_links(
        self, links: dict[str, Callable[[], object]], result: Mapping[str, object]
    ) -> None:
        if (
            self.database_id is None
            or (result.get("registry_id") or result.get("database_id")) != self.database_id
        ):
            return
        from forge.orchestration.registry import resource_refs, valid_resource

        indexed = {
            reference
            for _, kind, reference in resource_refs(result)
            if kind == "conversion_output_ref"
        }
        for item in records(result, "resources"):
            reference = value_at(item, "reference", "")
            if item.get("kind") == "conversion_output_ref" and valid_resource(
                "conversion_output_ref", reference
            ):
                indexed.add(reference)
        if (
            result.get("status") == "complete"
            and result.get("indexing") == "complete"
            and isinstance(reference := result.get("work_key"), str)
            and valid_resource("conversion_output_ref", reference)
        ):
            indexed.add(reference)
        for reference in sorted(indexed)[:5]:
            links[f"Open conversion output {reference}"] = partial(
                self.open_indexed_output, reference
            )
        for publication in overview.publications(result):
            links[f"Publication {publication}"] = partial(
                self.open_overview_publication, publication
            )
        ids = dict.fromkeys(
            str(value) for value in document(result.get("catalogue_dataset_ids")).values()
        )
        for stage in document(result.get("stages")).values():
            registered = document(stage).get("primary_file_ids")
            if isinstance(registered, list):
                ids.update(dict.fromkeys(str(item) for item in cast(list[object], registered)))
        for identity in list(ids)[:5]:
            if identity.isdecimal():
                selection = Row(identity, identity, "File", "", {}, identity)
                links[f"File {identity}"] = partial(
                    self.open_action, BY_KEY["datasets show"], selection
                )

    @work(exit_on_error=False)
    async def open_overview_artifact(self, reference: str) -> None:
        path = Path(reference)
        try:
            value = await disk(overview.read_artifact, self.context.paths.runs_dir, path)
            target = RetainedFile.reference("Prepared artifact", path, self.context.paths.runs_dir)
            self.push_screen(Detail(str(path), value, raw=True, editor_target=target))
        except Exception as error:
            self.push_screen(
                Inspection(
                    "Prepared artifact preview unavailable",
                    {"error": str(error)},
                    files=self.inspection_files(
                        "Prepared artifact", path, self.context.paths.runs_dir
                    ),
                )
            )

    @work(exit_on_error=False)
    async def open_overview_logs(self, identity: str) -> None:
        try:
            sources: dict[str, Path] = {}
            logs = await disk(
                read_managed_logs, self.context.paths.runs_dir, identity, sources=sources
            )
            self.push_screen(
                Inspection(
                    f"Engine logs · {identity}",
                    logs,
                    actions={"Open retained log…": partial(self.retained_log, sources)}
                    if sources
                    else None,
                    searchable=True,
                    files=retained_files({}, self.context.paths.runs_dir, sources=sources),
                    sections={
                        stream: [{"Line": line} for line in text.splitlines()]
                        for stream, text in logs.items()
                        if isinstance(text, str)
                    },
                )
            )
        except Exception as error:
            self.push_screen(Inspection("Engine logs unavailable", {"error": str(error)}))

    @work(exit_on_error=False)
    async def open_overview_publication(self, identity: int) -> None:
        await self.inspect_publication(identity)

    def copy_value(self, value: str) -> None:
        self.push_screen(Detail("Value", value, raw=True))

    @on(Button.Pressed)
    async def pressed(self, event: Button.Pressed) -> None:
        if self.screen != self.root_screen:
            return
        key = event.button.id or ""
        if key.startswith("area-"):
            await self.switch(AREAS[int(key.split("-")[1])])
        elif key.startswith("view-"):
            await self.switch(self.area, VIEWS[self.area][int(key.split("-")[1])])
        elif key.startswith("workflow-"):
            family = FAMILIES.get(self.view, "")
            row = self.state.rows.get(self.state.selection or "")
            action = BY_KEY.get(f"{family} {key.removeprefix('workflow-')}")
            if action and row and row.path:
                self.open_action(action, row)
        elif key == "page-next":
            self.action_next()
        elif key == "page-previous":
            self.action_previous()
        elif key == "browse-filters":
            self.open_filters()
        elif key == "browse-reset":
            self.state.filters.clear()
            self.state.sort = ""
            self.state.offset = 0
            self.state.filter = ""
            self.query_one("#filter", Input).value = ""
            self.request_query()
        elif key == "recipe-new":
            if self.area == "Catalogue":
                self.new_catalogue(self.view)
            else:
                self.open_action(BY_KEY[FAMILIES[self.view] + " new"])
        elif key == "context-menu":
            self.action_context()
        elif key.startswith("workspace-stage-"):
            self.save_inventory_view()
            self.inventory_stage = dashboard.STAGES[int(key.rsplit("-", 1)[1])]
            self.load_inventory_page()
        elif key == "inspect-selected":
            row = self.state.rows.get(self.state.selection or "")
            if row:
                self.open_row(row)
        elif key == "edit-source":
            row = self.state.rows.get(self.state.selection or "")
            if row and row.kind == "Profile":
                self.begin_profile("edit", row)
            elif row and row.path:
                if row.kind == "convert":
                    self.edit_conversion_options(row)
                else:
                    self.edit_recipe_form(row)

    def action_context(self) -> None:
        if self.screen != self.root_screen:
            return
        if self.area == "Configs":
            self.query_one(ConfigurationEditor).interact("more")
            return
        row = self.state.rows.get(self.state.selection or "")
        if self.area == "Overview":
            return
        self.row_menu(row)

    @on(Results.ContextRequested)
    def context_requested(self, event: Results.ContextRequested) -> None:
        if self.area == "Overview":
            return
        self.row_menu(
            self.state.rows.get(event.identity),
            anchor=event.anchor,
        )

    def action_filters(self) -> None:
        self.open_filters()

    def action_workspace_help(self) -> None:
        self.push_screen(
            Detail(
                "Workspace shortcuts",
                "Tab / Shift+Tab  Focus\nEnter  Inspect\nSpace  Select\n"
                "j / k  Move rows\n/  Search\nf / s  Filters and sort\n"
                "PageUp / PageDown  Browse pages\na / F2  Actions\ny  Copy row\n"
                "Drag over table text, then Ctrl+Y  Copy selected text\n"
                "r  Refresh\nF1  Configs field help\nCtrl+E  Switch environment\n"
                "Esc  Close detail or dialog\nCtrl+Q  Quit",
                raw=True,
            )
        )

    def action_row_up(self) -> None:
        self.resource_table().action_cursor_up()

    def action_row_down(self) -> None:
        self.resource_table().action_cursor_down()

    def action_copy_data_selection(self) -> None:
        self.copy_data_selection(self.resource_table().selected_text)

    def copy_data_selection(self, value: str) -> None:
        if value:
            self.copy_dashboard_id(value)
        else:
            self.notify("Drag over table text to select it.", severity="warning")

    def action_copy_row(self) -> None:
        if row := self.state.rows.get(self.state.selection or ""):
            self.copy_value(plain(row.value))

    def action_basket(self) -> None:
        if self.state.selection:
            self.toggle_basket(self.state.selection)

    @on(Results.BasketRequested)
    def basket_requested(self, event: Results.BasketRequested) -> None:
        if self.area != "Overview":
            self.toggle_basket(event.identity)

    def toggle_basket(self, identity: str) -> None:
        page = self.state
        if identity in page.basket:
            del page.basket[identity]
        elif identity in page.rows:
            page.basket[identity] = page.rows[identity]
        self.resource_table().mark(identity, page.selection, set(page.basket))
        self.query_one("#context-menu", Button).label = (
            f"Actions · {len(page.basket)} selected" if page.basket else "F2 Actions"
        )

    def clear_basket(self) -> None:
        self.state.basket.clear()
        self.render_rows()
        self.query_one("#context-menu", Button).label = "F2 Actions"

    def action_switch_registry(self) -> None:
        if self.screen == self.root_screen:
            self.switch_environment()

    @work(exit_on_error=False)
    async def switch_environment(self) -> None:
        if self.controller.active() or self.editor_open or self.recipe_activation:
            self.notify("Wait for the current operation or editor to finish.", severity="warning")
            return
        if self.query_one(ConfigurationEditor).editing:
            self.notify("Wait for the current configuration action to finish.", severity="warning")
            return
        target = "production" if self.context.testing else "testing"
        if await self.push_screen_wait(
            Confirm(
                f"Open {target.upper()} workspace",
                "The registry connection and selection basket will be reset."
                + (
                    " Pending configuration drafts will be discarded."
                    if self.query_one(ConfigurationEditor).dirty
                    else ""
                ),
            )
        ):
            self.exit(target)

    @work(exit_on_error=False)
    async def select_conversion_reference(self, row: Row) -> None:
        from forge.historical.conversion.evidence import inspect_publication_source
        from forge.historical.conversion.recipes import PublicationSource, read_recipe
        from forge.historical.conversion.registry import require_external_reference_mode

        if row.path is None:
            return
        try:
            recipe, original = await disk(read_recipe, row.path)
            if not isinstance(recipe.source, PublicationSource):
                raise ValueError(
                    "Open a conversion recipe bound directly to a registry publication."
                )
            source_id = recipe.source.registry_publication_id
            with (
                database_environment(testing=self.context.testing),
                pinned_registry(self.database_id),
            ):
                source = document(
                    (
                        await browse(
                            self.context.paths, DatasetRequest("show", publication_id=source_id)
                        )
                    ).get("publication")
                )
            require_external_reference_mode(value_at(source, "data_type"))
            search = ""
            offset = 0
            while True:
                with (
                    database_environment(testing=self.context.testing),
                    pinned_registry(self.database_id),
                ):
                    page = await publications(
                        self.context.paths,
                        reference_for=source_id,
                        limit=12,
                        offset=offset,
                        search=search,
                    )
                entries = {
                    reference_label(entry): int(value_at(entry, "registry_publication_id"))
                    for entry in records(page, "publications")
                }
                previous_page = "Previous page"
                next_page = "Next page"
                choices = (
                    *entries,
                    *((previous_page,) if offset else ()),
                    *((next_page,) if page["has_more"] else ()),
                )
                values = await self.push_screen_wait(
                    Form(
                        Action(
                            "Find external trade publications",
                            "Workflows",
                            (
                                Field("search", "Search by publication ID, exchange or instrument"),
                                *(
                                    (
                                        Field(
                                            "publication",
                                            "Registered trade publication",
                                            "choice",
                                            choices,
                                        ),
                                    )
                                    if choices
                                    else ()
                                ),
                            ),
                        ),
                        {"search": search},
                        local=True,
                    )
                )
                if values is None:
                    return
                updated = str(values["search"])
                selected = str(values.get("publication", ""))
                if updated != search:
                    search, offset = updated, 0
                elif selected in entries:
                    publication_id = entries[selected]
                    break
                elif selected:
                    offset += 12 if selected == next_page else -12
                else:
                    self.notify("Enter a search or choose a publication.", severity="warning")
            with (
                database_environment(testing=self.context.testing),
                pinned_registry(self.database_id),
            ):
                await inspect_publication_source(self.context.paths, publication_id)
                available = await browse(
                    self.context.paths, DatasetRequest("streams", publication_id=publication_id)
                )
            streams = tuple(
                dict.fromkeys(
                    value_at(entry, "stream_id")
                    for entry in records(available, "streams")
                    if entry.get("stream_id") and entry.get("kind") in {"trade", "trades"}
                )
            )
            if not streams:
                raise ValueError("The selected publication has no registered trade streams.")
            fields = tuple(
                Field(f"stream-{index}", stream, "check") for index, stream in enumerate(streams)
            )
            values = await self.push_screen_wait(
                Form(
                    Action(
                        f"Trade streams · registry publication {publication_id}",
                        "Workflows",
                        (
                            *fields,
                            Field(
                                "automatic",
                                "Use this publication for automatic selection",
                                "check",
                                help="Preparation resolves eligible routes from this publication and the bound source.",
                            ),
                        ),
                    ),
                    local=True,
                )
            )
            if values is None:
                return
            selected_streams = tuple(
                stream for stream, field in zip(streams, fields, strict=True) if values[field.key]
            )
            if values.get("automatic"):
                if selected_streams:
                    raise ValueError(
                        "Clear the manual stream choices to confirm automatic selection."
                    )
            elif not selected_streams:
                raise ValueError("Select at least one trade stream or enable automatic selection.")
            if await disk(row.path.read_bytes) != original:
                raise ValueError("The recipe changed during selection. Reopen it and retry.")
            self.edit_conversion_options(row, (publication_id, selected_streams))
        except Exception as error:
            self.push_screen(
                Detail("External reference selection", str(error), raw=True, error=True)
            )

    async def available_currencies(self) -> tuple[str, ...]:
        try:
            with (
                database_environment(testing=self.context.testing),
                pinned_registry(self.database_id),
            ):
                async with database_connection(database_dsn(self.context.paths.root)) as connection:
                    return await currency_codes(connection)
        except Exception:
            return ()

    @work(exit_on_error=False)
    async def edit_conversion_options(
        self,
        row: Row,
        external: tuple[int, tuple[str, ...]] | None = None,
        *,
        manual_draft: tuple[str, ...] | None = None,
    ) -> None:
        from forge.historical.conversion.contract import Policy
        from forge.historical.conversion.discovery import discover
        from forge.historical.conversion.planning import references as manual_references
        from forge.historical.conversion.recipes import read_recipe, validate_binding
        from forge.historical.conversion.resolution import eligible_references
        from forge.markets.ranking.policy import DEFAULT_TURNOVER_POLICY

        if row.path is None:
            return
        path = row.path
        try:
            recipe, original = await disk(read_recipe, path)
            policy_fields = (
                Field("target_quote", "Target quote", required=True, group="Conversion policy"),
                Field(
                    "price_source",
                    "Reference price source",
                    "choice",
                    ("last_trade",),
                    group="Conversion policy",
                ),
                Field(
                    "max_staleness_ns",
                    "Maximum reference age (nanoseconds)",
                    "integer",
                    required=Policy.model_fields["max_staleness_ns"].is_required(),
                    group="Conversion policy",
                    help="Maximum age of a reference trade in nanoseconds.",
                ),
                Field("max_hops", "Maximum hops", "integer", group="Conversion policy"),
                Field(
                    "path_policy",
                    "Path policy",
                    "choice",
                    ("all", "primary"),
                    group="Conversion policy",
                ),
            )
            resource_fields = tuple(
                Field(key, label, "integer", group="Advanced Options")
                for key, label in (
                    ("workers", "Workers"),
                    ("memory_limit_mb", "Memory limit (MiB)"),
                    ("max_open_files", "Open file limit"),
                )
            )
            preset: Values = {
                field.key: str(getattr(model, field.key))
                for model, fields in (
                    (recipe.conversion, policy_fields),
                    (recipe.resources, resource_fields),
                )
                for field in fields
            }
            currencies = await self.available_currencies()
            selection = recipe.selection_policy(recipe.version)
            preset.update(
                reference_mode=selection.mode,
                ranking_policy=selection.ranking_policy or DEFAULT_TURNOVER_POLICY,
                ranking_snapshot=str(selection.ranking_snapshot or ""),
                temporal_policy=selection.temporal_policy,
                reference_stream_ids=", ".join(recipe.reference_stream_ids),
                reference_publication_id=recipe.reference_publication_id or "",
            )

            async def discover_references(draft: bytes) -> dict[str, Any]:
                from forge.historical.conversion.recipes import Recipe

                candidate = Recipe.model_validate(tomllib.loads(draft.decode()))
                upstream = await disk(validate_binding, self.context.paths, path, candidate)
                with (
                    database_environment(testing=self.context.testing),
                    pinned_registry(self.database_id),
                ):
                    found = await discover(self.context.paths, candidate, upstream)
                if found["blockers"]:
                    raise ValueError("\n".join(found["blockers"]))
                return found

            if manual_draft is not None and selection.mode == "automatic":
                preset.update(reference_mode="manual", reference_stream_ids=", ".join(manual_draft))
            if external is not None:
                preset.update(
                    reference_mode="manual" if external[1] else "automatic",
                    reference_publication_id=external[0],
                    reference_stream_ids=", ".join(external[1]),
                )

            def options(
                values: Values,
            ) -> tuple[
                dict[str, object], dict[str, object], list[str], int | None, dict[str, object]
            ]:
                reference = values["reference_publication_id"]
                references = [
                    item.strip()
                    for item in str(values.get("reference_stream_ids", "")).split(",")
                    if item.strip()
                ]
                policy: dict[str, object] = {
                    field.key: values[field.key]
                    for field in policy_fields
                    if values[field.key] != "" and str(values[field.key]) != str(preset[field.key])
                }
                resources: dict[str, object] = {
                    field.key: values[field.key]
                    for field in resource_fields
                    if values[field.key] != "" and str(values[field.key]) != str(preset[field.key])
                }
                mode = values["reference_mode"]
                configured: dict[str, object] = {"mode": mode}
                if mode == "automatic":
                    configured.update(
                        ranking_policy=values.get("ranking_policy") or DEFAULT_TURNOVER_POLICY,
                        temporal_policy=values.get("temporal_policy") or "historical",
                    )
                    if snapshot := values.get("ranking_snapshot"):
                        configured["ranking_snapshot"] = snapshot
                    references = []
                return (
                    policy,
                    resources,
                    references,
                    int(reference) if reference else None,
                    configured,
                )

            async def validate(values: Values) -> dict[str, str]:
                from pydantic import ValidationError

                from forge.tui.recipe_forms import Path as FieldPath
                from forge.tui.recipe_forms import validation_issues

                try:
                    arguments = options(values)
                    draft, _ = await disk(conversion_candidate, original, *arguments)
                    if arguments[4]["mode"] == "manual" and arguments[2]:
                        found = await discover_references(draft.encode())
                        for source in found["sources"]:
                            manual_references(source, found["external_reference"], arguments[2])
                except ValidationError as error:
                    keys: dict[FieldPath, str] = {
                        (group, field.key): field.key
                        for group, fields in (
                            ("conversion", policy_fields),
                            ("resources", resource_fields),
                        )
                        for field in fields
                    }
                    return validation_issues(error, keys)
                except (MarketForgeError, ValueError) as error:
                    return {"": str(error)}
                return {}

            async def search(key: str, pattern: str, values: Values) -> EntityChoices:
                if key == "target_quote":
                    return tuple((code, code) for code in currencies if pattern.upper() in code)
                if key == "ranking_policy":
                    directory = self.context.paths.config_dir / "markets" / "rankings"
                    names = {*await disk(list_config_names, directory), DEFAULT_TURNOVER_POLICY}
                    return tuple(
                        (name, name)
                        for name in sorted(names)
                        if pattern.casefold() in name.casefold()
                    )
                if external is not None and values.get("reference_publication_id") == external[0]:
                    choices = {item: item for item in external[1]}
                else:
                    # Discovery needs only the source and optional external binding for the picker.
                    policy, resources, _, reference, _ = options(values)
                    draft, _ = conversion_candidate(
                        original, policy, resources, [], reference, {"mode": "automatic"}
                    )
                    found = await discover_references(draft.encode())
                    by_source = [
                        eligible_references(source, found["external_reference"])[0]
                        for source in found["sources"]
                    ]
                    shared = set(by_source[0]).intersection(*by_source[1:])
                    choices = {
                        item: f"{by_source[0][item]['identity']['symbol']} · "
                        f"{by_source[0][item]['identity']['exchange']} · Trades · "
                        f"Publication {by_source[0][item]['merge_registry_publication_id']}"
                        for item in sorted(shared)
                    }
                return tuple(
                    (item, label)
                    for item, label in choices.items()
                    if pattern.casefold() in f"{item} {label}".casefold()
                )

            def automatic(values: Mapping[str, Value]) -> bool:
                return values.get("reference_mode") == "automatic"

            def manual(values: Mapping[str, Value]) -> bool:
                return values.get("reference_mode") == "manual"

            values = await self.push_screen_wait(
                Form(
                    Action(
                        "Edit conversion policy and references",
                        "Workflows",
                        (
                            *policy_fields,
                            Field(
                                "reference_mode",
                                "Reference selection mode",
                                "choice",
                                ("automatic", "manual"),
                                required=True,
                                group="Reference selection",
                                help="Automatic resolves eligible trades during preparation.",
                            ),
                            Field(
                                "ranking_policy",
                                "Ranking policy",
                                group="Reference selection",
                                visible_when=automatic,
                            ),
                            Field(
                                "temporal_policy",
                                "Ranking applicability",
                                "choice",
                                ("historical", "ex_post"),
                                group="Reference selection",
                                visible_when=automatic,
                            ),
                            Field(
                                "ranking_snapshot",
                                "Exact ranking snapshot (optional)",
                                group="Reference selection",
                                visible_when=automatic,
                            ),
                            Field(
                                "reference_stream_ids",
                                "Reference trade streams",
                                group="Reference selection",
                                visible_when=manual,
                                required_when=lambda values: bool(
                                    values.get("reference_publication_id")
                                ),
                            ),
                            Field(
                                "reference_publication_id",
                                "External reference registry publication ID",
                                "integer",
                                group="Reference streams",
                            ),
                            *resource_fields,
                        ),
                    ),
                    preset,
                    local=True,
                    submit_label="Review conversion settings",
                    validate=validate,
                    review=lambda pending: plain(
                        conversion_candidate(original, *options(pending))[1]
                    ),
                    documentation=partial(
                        self.catalogue_docs,
                        f"{SITE_URL}workflows/conversion/#3-review-reference-trades",
                    ),
                    search_fields=frozenset(
                        {"target_quote", "reference_stream_ids", "ranking_policy"}
                    ),
                    multiple_entities=frozenset({"reference_stream_ids"}),
                    entity_search=search,
                )
            )
            if values is None:
                return
            candidate, review = await disk(conversion_candidate, original, *options(values))
            with recipe_writer(path):
                await disk(replace_reviewed, path, original, candidate)
            self.push_screen(
                Inspection(
                    "Conversion recipe updated",
                    review,
                    actions=self.saved_recipe_actions("convert", row.reference, path),
                )
            )
        except Exception as error:
            self.push_screen(Detail("Conversion recipe settings", str(error), raw=True, error=True))

    def saved_recipe_actions(
        self, family: str, name: str, path: Path
    ) -> dict[str, Callable[[], object]]:
        row = Row(name, name, family, "", {"path": str(path)}, name, path)
        actions: dict[str, Callable[[], object]] = {
            "Edit": partial(self.edit_recipe_form, row),
            "Edit TOML": partial(self.edit_selected, row),
        }
        if family == "convert":
            actions["Replace Source Publication"] = partial(self.edit_recipe_sources, row)
            actions["Browse external trade references…"] = partial(
                self.select_conversion_reference, row
            )
            actions["Edit policy and reference trades…"] = partial(
                self.edit_conversion_options, row
            )
        for verb in ("validate", "plan", "prepare", "map", "download", "run"):
            if action := BY_KEY.get(f"{family} {verb}"):
                actions[verb.title()] = partial(self.open_action, action, row)
        if family in FAMILIES.values():
            actions["Remove from Recipe…"] = partial(self.remove_recipe_input, row)
            if family != "markets rank":
                actions["Delete Recipe…"] = partial(self.delete_managed_recipe, row)
        return actions

    @work(exit_on_error=False)
    async def browse_data_publication(self, identity: int) -> None:
        while self.screen != self.root_screen:
            self.pop_screen()
        await self.switch("Data", "Normalized")
        self.query_one("#filter", Input).value = str(identity)

    async def review_merge(self, values: Values) -> None:
        registry = self.database_id
        testing = self.context.testing

        async def preview() -> Mapping[str, object]:
            with database_environment(testing=testing), pinned_registry(registry):
                return document(
                    await self.controller.invoke(
                        "merge new",
                        {**values, "preview_only": True},
                        self.context,
                        lambda _: None,
                        self.edit_recipe,
                    )
                )

        try:
            review = await preview()
            streams = tuple(
                (
                    f"{value_at(item, 'stream_id')} · {value_at(item, 'content_type')}",
                    value_at(item, "stream_id"),
                )
                for item in records(review, "streams")
            )
            if len(streams) > 1 and not values.get("stream_ranks"):
                ordering = await self.push_screen_wait(StreamOrder(streams))
                if ordering is None:
                    return
                if ordering != tuple(identity for _, identity in streams):
                    values["stream_ranks"] = ",".join(
                        f"{identity}={index}" for index, identity in enumerate(ordering)
                    )
                    review = await preview()
            recipe = document(review.get("recipe"))
            summary = summarize(dict(recipe), "Publication membership checked", str(review["path"]))
            summary["Environment"] = self.database_environment
            summary["Stream order"] = "\n".join(
                f"{value_at(item, 'stream_rank')} · {value_at(item, 'stream_id')}"
                for item in records(review, "streams")
            )
            summary["Coverage"] = value_at(document(review.get("coverage")), "note")
            if await self.push_screen_wait(Confirm("Save merge recipe", plain(summary))):
                if registry != self.database_id or testing != self.context.testing:
                    raise ValueError("The selected environment changed during review.")
                values["selection_sha256"] = str(recipe["selection_sha256"])
                await self.create_recipe(BY_KEY["merge new"], values)
        except Exception as error:
            self.push_screen(Detail("Merge publication selection", str(error), raw=True))

    async def author_exact_normalization(self, archive_ids: tuple[int, ...]) -> None:
        from forge.historical.normalization.batch import inspect_exact_archives
        from forge.historical.normalization.config import ExactRecipe, NormalizationRecipe

        try:
            with database_environment(testing=True), pinned_registry(self.database_id):
                registry, archives = await inspect_exact_archives(self.context.paths, archive_ids)
            automatic = "Automatic resolution"
            fields = tuple(
                Field(
                    f"profile-{index}",
                    f"Archive {archive['archive_id']} · compatible profile",
                    "choice",
                    (automatic, str(archive["profile_id"])),
                    default=automatic,
                )
                for index, archive in enumerate(archives)
            )

            def recipe_for(values: Values) -> ExactRecipe:
                return ExactRecipe.model_validate(
                    {
                        "registry_id": registry,
                        **(
                            {"ranking_policy": values["ranking_policy"]}
                            if values.get("ranking_policy")
                            else {}
                        ),
                        "archive_pins": [
                            {
                                "archive_id": archive["archive_id"],
                                "selection_sha256": archive["selection_sha256"],
                                **(
                                    {
                                        "profile_id": archive["profile_id"],
                                        "profile_sha256": archive["profile_sha256"],
                                    }
                                    if values[field.key] == archive["profile_id"]
                                    else {}
                                ),
                            }
                            for archive, field in zip(archives, fields, strict=True)
                        ],
                    }
                )

            async def validate(values: Values) -> dict[str, str]:
                from pydantic import ValidationError

                from forge.tui.recipe_forms import validation_issues

                try:
                    recipe_for(values)
                    path = resolve_config_file(
                        self.context.paths.normalization_recipe_dir, str(values["reference"])
                    )
                    if await disk(path.exists):
                        return {"reference": "A recipe with this name already exists."}
                except ValidationError as error:
                    return validation_issues(error, {("ranking_policy",): "ranking_policy"})
                except ValueError as error:
                    return {"reference": str(error)}
                return {}

            def review(values: Values) -> str:
                selected = recipe_for(values)
                return "\n".join(
                    (
                        f"Recipe  {values['reference']}",
                        f"Environment  TESTING · {registry}",
                        "Profile resolution  Compatible profiles found",
                        "Preparation  Pending",
                        *(
                            f"Archive {pin.archive_id}  ·  {pin.profile_id or 'Automatically resolved'}"
                            for pin in selected.archive_pins
                        ),
                    )
                )

            values = await self.push_screen_wait(
                Form(
                    Action(
                        "Normalize selected archives",
                        "Data",
                        (
                            Field("reference", "Recipe name", required=True, group="Recipe"),
                            *fields,
                            Field(
                                "ranking_policy",
                                "Ranking policy",
                                group="Processing policy",
                                placeholder=f"Default: {NormalizationRecipe.model_fields['ranking_policy'].default}",
                            ),
                        ),
                    ),
                    local=True,
                    validate=validate,
                    review=review,
                    submit_label="Review recipe",
                    commit_label="Save recipe",
                    documentation=partial(
                        self.catalogue_docs,
                        f"{SITE_URL}workflows/normalization/#2-create-a-normalization-recipe",
                    ),
                )
            )
            if values is None:
                return
            name = str(values["reference"])
            path = resolve_config_file(self.context.paths.normalization_recipe_dir, name)
            recipe = recipe_for(values)
            with database_environment(testing=True), pinned_registry(registry):
                current_registry, current = await inspect_exact_archives(
                    self.context.paths, archive_ids
                )
            if current_registry != registry or current != archives:
                raise ValueError("The selected archive or profile evidence changed after review.")
            with WorkspaceOwnership(self.context.paths):
                await save_normalization(path, recipe)
        except Exception as error:
            self.push_screen(Detail("Normalization selection", str(error), raw=True, error=True))
        else:
            self.push_screen(
                Inspection(
                    "Normalization recipe saved",
                    recipe.model_dump(),
                    actions=self.saved_recipe_actions("normalize", name, path),
                )
            )

    @work(exit_on_error=False)
    async def dataset_deduplication(self) -> None:
        from forge.datasets import deduplication

        operation = await self.push_screen_wait(
            ActionMenu("Deduplication stage", ("normalize", "merge", "convert", "all"))
        )
        if operation is None:
            return
        paths = self.context.paths
        try:
            with (
                database_environment(testing=self.context.testing),
                pinned_registry(self.database_id),
            ):
                reviewed = await deduplication.inventory(paths, operation, apply=False)
                await disk(deduplication.save_preview, paths, reviewed)
        except Exception as error:
            self.push_screen(Detail("Deduplication preview", str(error), raw=True, error=True))
            return

        async def apply() -> object:
            plan = await disk(deduplication.load_preview, paths, Path(reviewed["report_path"]))
            return await deduplication.apply_inventory(paths, operation, preview=plan)

        actions: dict[str, Callable[[], object]] = {}
        if self.context.testing:
            actions["Apply reviewed deduplication…"] = partial(
                self.apply_review,
                Action("datasets deduplicate", "Data", effect="guarded", testing=True),
                {},
                apply,
            )
        self.push_screen(Inspection("Dataset deduplication plan", reviewed, actions=actions))

    @work(exit_on_error=False)
    async def inspect_supersession(self, identity: int) -> None:
        from forge.datasets.deduplication import inspect_supersession

        try:
            with (
                database_environment(testing=self.context.testing),
                pinned_registry(self.database_id),
            ):
                result = await inspect_supersession(self.context.paths, identity)
        except Exception as error:
            self.push_screen(Detail("Publication supersession", str(error), raw=True, error=True))
            return
        self.push_screen(Inspection(f"Supersession · publication {identity}", result))

    @work(exit_on_error=False)
    async def delete_publications(self, selected: tuple[Row, ...]) -> None:
        from forge.datasets import deduplication, explicit_delete

        if not self.context.testing:
            self.notify("Publication deletion requires the testing registry.", severity="warning")
            return
        if self.controller.active():
            self.notify(
                "Wait for the active operation before reviewing deletion.", severity="warning"
            )
            return
        ids = list(
            dict.fromkeys(
                int(item.reference or item.id) for item in selected if item.kind == "Publication"
            )
        )
        outputs = [item.reference for item in selected if item.kind == "Indexed conversion output"]
        try:
            with database_environment(testing=True), pinned_registry(self.database_id):
                reviewed = await explicit_delete.preview(
                    self.context.paths, ids, outputs, [], cascade=False
                )
                await disk(deduplication.save_preview, self.context.paths, reviewed)
        except Exception as error:
            self.push_screen(
                Detail("Publication deletion preview", str(error), raw=True, error=True)
            )
            return

        async def apply() -> object:
            plan = await disk(
                deduplication.load_preview, self.context.paths, Path(str(reviewed["report_path"]))
            )
            return await explicit_delete.apply(self.context.paths, plan)

        actions: dict[str, Callable[[], object]] = {}
        if reviewed.get("status") == "ready":
            actions["Apply reviewed deletion…"] = partial(
                self.apply_review,
                Action("datasets delete", "Data", effect="guarded", testing=True),
                {},
                apply,
            )
        self.push_screen(Inspection("Testing publication deletion plan", reviewed, actions=actions))

    def configuration_settings(self, initial: str = "application") -> None:
        self.push_screen(
            ConfigurationExplorer(
                self.context,
                self.queue_editor,
                self.copy_value,
                self.reload_application_settings,
                self.configuration_environment,
                initial,
            )
        )

    def configuration_environment(self) -> dict[str, object]:
        from os import environ

        from forge.shared.database.credentials import PRODUCTION_DATABASE_KEY, TEST_DATABASE_KEY

        return {
            "Environment": self.database_environment,
            "Registry identity": self.database_id,
            "Session registry check": self.database_indicator,
            "Workspace": str(self.context.paths.root),
            "Environment variable presence": {
                key: key in environ for key in (TEST_DATABASE_KEY, PRODUCTION_DATABASE_KEY)
            },
            "Credential resolution": "Environment or workspace .secrets/postgres.env",
        }

    def reload_application_settings(self, settings: AppSettings, path: Path) -> None:
        from dataclasses import replace

        from forge.shared.paths import ProjectPaths
        from forge.shared.terminal.console import create_console

        paths = ProjectPaths.resolve(
            root=self.context.paths.root, data_root=Path(settings.storage.data_root)
        )
        reload_workspace = paths != self.context.paths
        self.context = replace(
            self.context,
            settings=settings,
            settings_file=path,
            paths=paths,
            console=create_console(settings.presentation),
            error_console=create_console(settings.presentation, stderr=True),
        )
        self.controller.context = self.context
        if reload_workspace:
            self.exit(self.database_environment)

    @work(exit_on_error=False)
    async def open_indexed_run(self, identity: str, *, show_errors: bool = False) -> None:
        await self.inspect_request(
            "runs show",
            {"reference": identity},
            f"Run {identity}",
            {
                "View report": partial(
                    self.open_action, BY_KEY["runs report"], None, {"reference": identity}
                ),
                "View logs": partial(
                    self.open_action, BY_KEY["runs logs"], None, {"reference": identity}
                ),
                "Copy run ID": partial(self.copy_value, identity),
            },
            show_errors=show_errors,
        )

    @work(exit_on_error=False)
    async def delete_runs(self, selected: tuple[Row, ...]) -> None:
        from forge.orchestration import run_delete

        if not self.context.testing:
            self.notify("Switch to testing before reviewing run deletion.", severity="warning")
            return
        values = await self.push_screen_wait(
            Form(
                Action(
                    "Review run deletion",
                    "Runs",
                    (
                        Field("children", "Include indexed descendants", "check", default=False),
                        Field("history_only", "Preserve physical evidence", "check", default=True),
                    ),
                ),
                local=True,
            )
        )
        if values is None:
            return
        try:
            with database_environment(testing=True), pinned_registry(self.database_id):
                report = await run_delete.preview(
                    self.context.paths,
                    [row.id for row in selected],
                    children=bool(values.get("children")),
                    history_only=bool(values.get("history_only")),
                )
        except Exception as error:
            self.push_screen(Detail("Run deletion preview", str(error), raw=True, error=True))
            return

        @work(exit_on_error=False)
        async def apply(app: ForgeApp) -> None:
            if not await app.push_screen_wait(
                Confirm("Apply exact run deletion plan", str(report["plan_path"]), destructive=True)
            ):
                return
            try:
                with database_environment(testing=True), pinned_registry(self.database_id):
                    result = await run_delete.apply(app.context.paths, Path(report["plan_path"]))
            except Exception as error:
                app.push_screen(Detail("Run deletion", str(error), raw=True, error=True))
                return
            app.push_screen(Inspection("Run deletion result", result))
            app.request_query()

        self.push_screen(
            Inspection(
                "Testing run deletion plan",
                report,
                actions={"Apply exact reviewed plan…": partial(apply, self)}
                if report.get("status") == "preview"
                else None,
            )
        )

    @work(exit_on_error=False)
    async def run_maintenance(self, verb: str) -> None:
        from forge.orchestration.registry_maintenance import index_legacy, reconcile

        service = index_legacy if verb == "index-legacy" else reconcile
        try:
            with (
                database_environment(testing=self.context.testing),
                pinned_registry(self.database_id),
            ):
                result = await service(self.context.paths, limit=50, offset=0, apply=False)
        except Exception as error:
            self.push_screen(Detail(verb, str(error), raw=True, error=True))
            return

        async def apply() -> object:
            return await service(self.context.paths, limit=50, offset=0, apply=True)

        self.push_screen(
            Inspection(
                verb,
                result,
                actions={
                    "Apply verified metadata changes…": partial(
                        self.apply_review,
                        Action(f"runs {verb}", "Runs", effect="write", testing=True),
                        {},
                        apply,
                    )
                }
                if self.context.testing
                else None,
            )
        )

    @staticmethod
    def lifecycle_family(kind: str) -> RecipeFamily:
        return cast(
            RecipeFamily,
            {
                "normalize": "normalization",
                "convert": "conversion",
                "acquisition import": "import",
                "acquisition register": "registration",
            }.get(kind, kind),
        )

    @work(exit_on_error=False)
    async def restore_managed_recipe(self, kind: str) -> None:
        from forge.shared.config.deletion import archived_recipes, restore_recipe

        paths = self.context.paths
        family = self.lifecycle_family(kind)
        archived = await disk(archived_recipes, paths, family)
        if not archived:
            self.push_screen(Inspection("Archived recipes", {"Status": "No archived recipes"}))
            return
        choices = {f"{path.stem} · {path.parent.name}": path for path in archived}
        selected = await self.push_screen_wait(
            ActionMenu("Restore archived recipe", tuple(choices))
        )
        if selected is None:
            return
        try:
            await disk(restore_recipe, paths, family, choices[selected])
        except (MarketForgeError, OSError) as error:
            self.push_screen(Inspection("Restore unavailable", {"Error": str(error)}))
        else:
            self.request_query()

    @work(exit_on_error=False)
    async def remove_recipe_input(self, row: Row) -> None:
        from forge.historical.recipe_removal import configured_removals, remove_configured_input

        if row.path is None or self.recipe_activation:
            return
        self.recipe_activation = True
        registry, testing = self.database_id, self.context.testing
        try:
            original = await disk(row.path.read_bytes)
            data = tomllib.loads(original.decode())
            choices = {entry.label: entry for entry in configured_removals(row.kind, data)}
            if not choices:
                self.push_screen(
                    Inspection(
                        "Remove from Recipe",
                        {
                            "Recipe": row.reference,
                            "Configured entries": "No removable explicit entries. Use Edit to change the selection policy.",
                        },
                    )
                )
                return
            choice = await self.push_screen_wait(
                ActionMenu(f"Remove from {row.reference}", tuple(choices))
            )
            if choice is None:
                return
            self.check_recipe_environment(registry, testing)
            with database_environment(testing=testing), pinned_registry(registry):
                candidate = await remove_configured_input(
                    data,
                    row.kind,
                    choices[choice],
                    paths=self.context.paths,
                    registry=registry,
                )
            await self.author_recipe(
                row.kind,
                row,
                initial=candidate,
                expected=original,
                removal_review=choice,
            )
        except Exception as error:
            self.push_screen(Inspection("Removal unavailable", {"Error": str(error)}))
        finally:
            self.recipe_activation = False

    @work(exit_on_error=False)
    async def delete_managed_recipe(self, row: Row) -> None:
        from dataclasses import asdict

        from forge.historical.recipe_dependencies import inspect_references
        from forge.shared.config.deletion import delete_recipe

        family = self.lifecycle_family(row.kind)
        paths = self.context.paths

        def request(*, apply: bool = False, digest: str | None = None) -> object:
            return asdict(
                delete_recipe(
                    paths,
                    family,
                    row.reference,
                    apply=apply,
                    expected_sha256=digest,
                    expected_path=row.path,
                    dependencies=partial(inspect_references, paths, family),
                )
            )

        try:
            preview = document(await disk(request))
        except Exception as error:
            self.push_screen(Detail("Recipe deletion preview", str(error), raw=True, error=True))
            return

        @work(exit_on_error=False)
        async def apply(app: ForgeApp) -> None:
            if not await app.push_screen_wait(
                Confirm(
                    "Delete managed recipe",
                    f"{row.reference} · {family.title()}\n\n"
                    "The recipe will leave active Workflows and can be restored from the archive. "
                    "This is shared workspace configuration for Testing and Production. "
                    "Retained Runs and datasets will remain available.",
                    destructive=True,
                    confirm_label="Delete Recipe",
                    back_label="Cancel",
                )
            ):
                return
            try:
                result = await disk(request, apply=True, digest=str(preview["sha256"]))
            except Exception as error:
                app.push_screen(Detail("Recipe deletion", str(error), raw=True, error=True))
                return
            app.push_screen(Inspection("Recipe deletion result", result))
            app.request_query()

        self.push_screen(
            Inspection(
                "Recipe deletion preview",
                preview,
                actions={"Delete recipe…": partial(apply, self)}
                if preview.get("status") == "ready_to_delete"
                else None,
            )
        )

    @work(exit_on_error=False)
    async def edit_recipe_sources(self, row: Row) -> None:
        from forge.historical.conversion.evidence import inspect_publication_source
        from forge.historical.merge.service import publication_recipe
        from forge.historical.normalization.batch import inspect_exact_archives
        from forge.tui.screens import PathPicker

        if row.path is None or self.recipe_activation:
            return
        self.recipe_activation = True
        try:
            source_review = None
            original = await disk(row.path.read_bytes)
            data = tomllib.loads(original.decode())
            if row.kind in {"acquisition", "pipeline"}:
                await self.author_recipe(row.kind, row, expected=original, add_entry=True)
                return
            if row.kind == "normalize" and "archive_pins" not in data:
                await self.author_recipe(row.kind, row, expected=original)
                return
            if row.kind == "acquisition import":

                def resolve_root() -> Path:
                    return (
                        self.context.paths.root / Path(str(data["root"])).expanduser()
                    ).resolve()

                root = await disk(resolve_root)
                path = await self.push_screen_wait(PathPicker(root))
                if path is None:
                    return
                selected = str((await disk(path.resolve)).relative_to(root))
                data["files"] = list(
                    dict.fromkeys([*cast(list[str], data.get("files", [])), selected])
                )
            else:
                if row.kind == "merge" and not {"selection", "publication_ids"} & data.keys():
                    await self.author_recipe(row.kind, row, expected=original)
                    return
                key, label = {
                    "normalize": ("archive_ids", "Registered raw archives"),
                    "merge": ("publication_ids", "Registered normalized publications"),
                    "convert": ("source", "Registered merged publication"),
                }[row.kind]
                values = await self.push_screen_wait(
                    Form(
                        Action("Select sources", "Workflows", (Field(key, label, required=True),)),
                        {"route": "publication"},
                        local=True,
                        submit_label="Open draft",
                        search_fields=frozenset({key}),
                        multiple_entities=frozenset({key}) if key != "source" else frozenset(),
                        entity_search=self.search_entities,
                    )
                )
                if values is None:
                    return
                ids = [int(part) for part in str(values[key]).split(",")]
                with (
                    database_environment(testing=self.context.testing),
                    pinned_registry(self.database_id),
                ):
                    if row.kind == "normalize":
                        registry, archives = await inspect_exact_archives(
                            self.context.paths, tuple(ids)
                        )
                        if data.get("registry_id") != registry:
                            raise ValueError("This recipe belongs to another registry.")
                        pins = cast(list[dict[str, object]], data["archive_pins"])
                        known = {pin["archive_id"] for pin in pins}
                        pins.extend(
                            {
                                "archive_id": item["archive_id"],
                                "selection_sha256": item["selection_sha256"],
                            }
                            for item in archives
                            if item["archive_id"] not in known
                        )
                    elif row.kind == "merge":
                        if data.get("file_subset"):
                            raise ValueError(
                                "Use Data → Normalized → Add to Recipe to extend an exact file selection."
                            )
                        if "selection" in data:
                            selection = cast(dict[str, object], data["selection"])
                            selection["publication_ids"] = list(
                                dict.fromkeys(
                                    [*cast(list[int], selection.get("publication_ids", [])), *ids]
                                )
                            )
                        else:
                            if data.get("registry_id") != self.database_id:
                                raise ValueError("This recipe belongs to another registry.")
                            recipe, _ = await publication_recipe(
                                self.context.paths,
                                list(
                                    dict.fromkeys([*cast(list[int], data["publication_ids"]), *ids])
                                ),
                                start_ns=int(str(data["start_ns"])),
                                end_ns=int(str(data["end_ns"])),
                                stream_ranks=cast(dict[str, int], data.get("stream_ranks")),
                                timeline_ordering=cast(str | None, data.get("timeline_ordering")),
                            )
                            for name in (
                                "dataset_ids",
                                "publication_ids",
                                "registry_id",
                                "selection_sha256",
                            ):
                                data[name] = recipe.model_dump(mode="json")[name]
                    else:
                        if len(ids) != 1:
                            raise ValueError("Select one merged publication.")
                        bound = await inspect_publication_source(self.context.paths, ids[0])
                        data["source"] = bound.model_dump(mode="json")
                        data["interval"] = bound.interval.model_dump(mode="json")
                        data["selection"] = {"whole_publication": True}
                        source_review = f"Replace source with merged registry publication {ids[0]}"
            await self.author_recipe(
                row.kind,
                row,
                initial=data,
                expected=original,
                source_review=source_review,
                verify_conversion_plan=source_review is not None,
            )
        except Exception as error:
            self.push_screen(Inspection("Source selection unavailable", {"Error": str(error)}))
        finally:
            self.recipe_activation = False

    def recipe_menu(self, row: Row) -> dict[str, Callable[[], object]]:
        assert row.path is not None
        choices: dict[str, Callable[[], object]] = {"Inspect": partial(self.open_row, row)}
        choices.update(self.saved_recipe_actions(row.kind, row.reference, row.path))
        choices.pop("Browse external trade references…", None)
        choices.pop("Edit policy and reference trades…", None)
        for key in self.related(row):
            label = key.removeprefix(row.kind + " ").title()
            if label not in {"List", "Inspect", "Edit", "New", "Sources"}:
                choices.setdefault(label, partial(self.open_action, BY_KEY[key], row))
        source = {
            "acquisition": "Add Instrument",
            "pipeline": "Add Acquisition Request",
            "normalize": "Select Sources",
            "merge": "Select Normalized Sources",
            "convert": "Replace Source Publication",
            "acquisition import": "Add Source Files",
        }.get(row.kind)
        if source:
            choices[source] = partial(self.edit_recipe_sources, row)
        if row.kind == "pipeline":
            for verb in ("resume", "status"):
                choices[verb.title() + " Run"] = partial(
                    self.open_action, BY_KEY[f"pipeline {verb}"]
                )
        if row.kind == "convert":
            choices["Add Reference Streams"] = partial(self.select_conversion_reference, row)
        return choices

    def show_data_actions(
        self,
        title: str,
        actions: dict[str, Callable[[], object]],
        *,
        primary: str = "",
        anchor: tuple[int, int] | None = None,
    ) -> None:
        def choose(label: str | None) -> None:
            if label is not None:
                actions[label]()

        self.push_screen(ActionMenu(title, tuple(actions), primary=primary, anchor=anchor), choose)

    @work(exit_on_error=False)
    async def add_data_to_recipe(
        self, view: str, rows: tuple[Row, ...], registry: str | None, testing: bool
    ) -> None:
        await data_actions.add_data_to_recipe(self, view, rows, registry, testing)

    def row_menu(
        self,
        row: Row | None,
        *,
        anchor: tuple[int, int] | None = None,
    ) -> None:
        if self.area == "Data":
            data_actions.row_actions(self, row, anchor)
            return
        if self.area == "Catalogue":
            actions = self.catalogue_actions(row)
        elif self.area == "Workflows" and row and row.path:
            actions = self.recipe_menu(row)
        else:
            actions: dict[str, Callable[[], object]] = {}
            if row:
                if self.area == "Markets" and self.view == "Instruments":
                    selected = self.selected_rows(row)
                    if self.database_id and all(
                        market_recipes.instrument_from_row(item) for item in selected
                    ):
                        for label, family in (
                            ("Add to Acquisition", "acquisition"),
                            ("Add to Pipeline", "pipeline"),
                        ):
                            actions[label] = partial(
                                self.add_to_recipe,
                                selected,
                                family,
                                self.database_id,
                                self.context.testing,
                            )
                actions["Inspect"] = partial(self.open_row, row)
                if self.area == "Markets" and self.view == "Rankings":
                    actions["Copy Full Identifier"] = partial(self.copy_value, row.reference)
                    actions["View Ranking Results"] = partial(
                        self.open_action,
                        BY_KEY["markets rank show"],
                        None,
                        {"reference": row.reference},
                    )
                if self.area == "Markets" and self.view == "Policies" and row.path:
                    actions["Edit TOML"] = partial(self.edit_selected, row)
                    actions["Validate"] = partial(
                        self.open_action, BY_KEY["markets rank validate"], row
                    )
                if self.area == "Runs":
                    for key, label in (
                        ("runs report", "Open Report"),
                        ("runs logs", "Open Logs"),
                        ("pipeline resume", "Resume Pipeline"),
                    ):
                        if key in self.related(row):
                            actions[label] = partial(self.open_action, BY_KEY[key], row)
                    if row.kind == "Indexed run" and self.context.testing:
                        actions["Review run deletion…"] = partial(
                            self.delete_runs, self.selected_rows(row)
                        )
                    actions["Copy Full Identifier"] = partial(
                        self.copy_value, row.reference or row.id
                    )
            if self.area == "Runs" and row is None:
                for verb, label in (
                    ("index-legacy", "Review Legacy Run Indexing…"),
                    ("reconcile", "Review Run Reconciliation…"),
                ):
                    actions[label] = partial(self.run_maintenance, verb)
            if row is None and self.area == "Markets" and self.view == "Instruments":
                for verb, label in (
                    ("refresh", "Refresh Observations…"),
                    ("sync", "Synchronize Instruments…"),
                ):
                    actions[label] = partial(self.open_action, BY_KEY[f"markets {verb}"])
        if (
            self.area == "Workflows"
            and (family := FAMILIES.get(self.view))
            and family != "markets rank"
        ):
            actions["Restore Archived Recipe…"] = partial(self.restore_managed_recipe, family)
        if not actions:
            return
        title = row.name if row else self.view
        if row and row.path and self.area == "Workflows":
            title = f"{row.kind.upper()} · {row.name}"
        if row and self.area == "Markets" and self.view == "Instruments":
            exchange = value_at(document(row.value), "exchange")
            title = f"{row.name} · {exchange.title()}"
            if "Add to Acquisition" in actions:
                count = len(self.selected_rows(row))
                if count > 1:
                    title += f" · {count} selected"
        self.show_data_actions(title, actions, anchor=anchor)

    def selected_rows(self, row: Row) -> tuple[Row, ...]:
        basket = self.state.basket
        return tuple(basket.values()) if row.id in basket else (row,)

    @work(exit_on_error=False)
    async def catalogue_docs(self, url: str) -> None:
        import webbrowser

        if self.is_headless or not await disk(webbrowser.open, url):
            self.copy_value(url)

    def catalogue_actions(self, row: Row | None) -> dict[str, Callable[[], object]]:
        view = self.view
        if row is None:
            actions: dict[str, Callable[[], object]] = {
                f"New {catalogue_ui.LABELS[view]}": partial(self.new_catalogue, view),
                "Documentation": partial(self.catalogue_docs, catalogue_ui.documentation_url(view)),
            }
            if view == "Profiles":
                actions["Resume profile draft"] = partial(self.begin_profile, "resume")
            else:
                actions["Review saved declaration"] = partial(self.open_catalogue_declaration, view)
                actions["Edit saved declaration"] = partial(
                    self.open_catalogue_declaration, view, edit=True
                )
            return actions

        actions = {"Inspect": partial(self.open_row, row)}
        if view == "Profiles":
            actions["Clone Profile"] = partial(self.begin_profile, "clone", row)
            actions["Edit profile draft"] = partial(self.begin_profile, "edit", row)
            actions["Rust validate profile"] = partial(self.validate_installed_profile, row)
        elif row.kind == "instrument":
            actions["View Specifications"] = partial(
                self.catalogue_related, "Specifications", row.reference
            )
        actions["Copy Full Identifier"] = partial(self.copy_dashboard_id, row.reference)
        return actions

    async def catalogue_call(
        self, key: str, values: Values, *, expected_registry: str | None = None
    ) -> dict[str, object]:
        with (
            database_environment(testing=self.context.testing),
            pinned_registry(expected_registry or self.database_id),
        ):
            result = await self.controller.invoke(
                key, values, self.context, lambda _: None, self.edit_recipe
            )
        return dict(document(result))

    @work(exit_on_error=False)
    async def catalogue_related(self, view: str, search: str, exchange: str = "") -> None:
        while self.screen != self.root_screen:
            self.pop_screen()
        await self.switch("Catalogue", view)
        self.state.filters = {"exchange": exchange} if exchange else {}
        self.state.offset = 0
        self.state.filter = search
        self.query_one("#filter", Input).value = search
        self.request_query()

    async def inspect_catalogue(self, row: Row) -> None:
        context = self.catalogue_actions(row)
        context.pop("Inspect")
        if row.kind == "Profile":
            item = document(row.value)
            definition = document(item.get("definition"))
            summary = {**item, "Scope": "Managed workspace profile files", **definition}
            self.push_screen(
                Inspection(
                    row.reference,
                    item,
                    actions=context,
                    sections={
                        "Summary": summary,
                        "Source": definition.get("source"),
                        "Mappings": catalogue_ui.configured_mappings(definition),
                        "Schema": catalogue_ui.profile_parameters(definition, row.reference),
                    },
                    files=self.inspection_files(
                        "Profile TOML", row.path, self.context.paths.config_dir
                    ),
                    section_values={"Summary": definition},
                )
            )
            return
        try:
            result = await self.catalogue_call(
                f"catalog {row.kind} show", {"reference": row.reference}
            )
        except Exception as error:
            self.push_screen(Inspection(row.reference, {"error": str(error)}))
            return
        sections: dict[str, object] = {"Summary": result}
        if row.kind == "exchange":
            context["View instruments"] = partial(
                self.catalogue_related, "Instruments", "", row.reference
            )
            context["New Instrument"] = partial(self.new_catalogue, "Instruments", row.reference)
        elif row.kind == "instrument":
            context["View specifications"] = partial(
                self.catalogue_related, "Specifications", row.reference
            )
            context["New Specification"] = partial(
                self.new_catalogue, "Specifications", row.reference
            )
            if identity := result.get("historical_instrument_id"):
                context["Copy canonical instrument ID"] = partial(
                    self.copy_dashboard_id, str(identity)
                )
        else:
            item = document(row.value)
            definition = document(item.get("definition"))
            sections = {
                "Specification": {**item, **definition, "Instrument": row.reference},
                "Instrument": result.get("instrument"),
                "Current": result.get("current"),
                "Revisions": [
                    {**record, **document(record.get("definition"))}
                    for record in records(result, "evidence")
                ],
            }
            context["View instrument"] = partial(
                self.catalogue_related, "Instruments", row.reference
            )
            if item.get("fingerprint"):
                context["Copy specification fingerprint"] = partial(
                    self.copy_dashboard_id, str(item["fingerprint"])
                )
            if item.get("id"):
                context["Copy specification ID"] = partial(self.copy_dashboard_id, str(item["id"]))
        self.push_screen(Inspection(row.reference, result, actions=context, sections=sections))

    @work(exit_on_error=False)
    async def new_catalogue(
        self, view: str, dependency: str = "", *, draft: Path | None = None
    ) -> None:
        from uuid import uuid4

        from forge.historical.authoring import publish_new
        from forge.shared.config.recipe_editing import update_recipe

        if view == "Profiles":
            self.begin_profile("new")
            return
        kind = CATALOG[view]
        label = catalogue_ui.LABELS[view]
        try:
            dependency_key = {"instrument": "exchange", "specs": "instrument"}.get(kind)
            values: Values = {dependency_key: dependency} if dependency_key and dependency else {}
            fields = catalogue_ui.declaration_fields(kind)
            original = await disk(draft.read_bytes) if draft else None
            before = tomllib.loads(original.decode()) if original is not None else {}
            if original is not None:
                for field in fields:
                    if field.default != "":
                        values.setdefault(field.key, "")
                values.update({key: str(value) for key, value in before.items()})

            async def validate(candidate: Values) -> dict[str, str]:
                return catalogue_ui.validate_declaration(kind, candidate)

            result = await self.push_screen_wait(
                Form(
                    Action(f"New {label}", "Catalogue", fields),
                    values,
                    local=True,
                    submit_label="Review declaration",
                    search_fields=frozenset({dependency_key}) if dependency_key else frozenset(),
                    entity_search=self.search_entities,
                    validate=validate,
                    documentation=partial(
                        self.catalogue_docs, catalogue_ui.documentation_url(view)
                    ),
                )
            )
            if result is None:
                return
            path = draft or (
                self.context.paths.config_dir
                / "forge/catalogue-drafts"
                / f"{kind}-{uuid4().hex}.toml"
            )
            candidate: dict[str, object] = {key: val for key, val in result.items() if val != ""}
            if original is None:
                await disk(publish_new, path, dumps(candidate))
            else:
                # The form displays TOML dates and numbers as strings. Keep the
                # original typed values for fields left untouched in the form.
                for key in candidate.keys() & before.keys():
                    configured = candidate[key]
                    if configured == values.get(key):
                        candidate[key] = before[key]
                    elif isinstance(before[key], datetime) and isinstance(configured, str):
                        candidate[key] = datetime.fromisoformat(configured)
                content = update_recipe(original, before, candidate)
                if content.encode() != original:
                    with recipe_writer(path):
                        await disk(replace_reviewed, path, original, content)
            await self.review_catalogue_declaration(view, path)
        except Exception as error:
            self.push_screen(Inspection(f"{label} draft unavailable", {"error": str(error)}))

    @work(exit_on_error=False)
    async def open_catalogue_declaration(self, view: str, edit: bool = False) -> None:
        values = await self.push_screen_wait(
            Form(
                Action(
                    "Saved catalogue declaration",
                    "Catalogue",
                    (Field("source", "Declaration TOML", required=True),),
                ),
                root=self.context.paths.root,
                local=True,
            )
        )
        if values is not None:
            path = self.context.paths.root / await disk(Path(str(values["source"])).expanduser)
            await self.review_catalogue_declaration(view, path, edit=edit)

    async def review_catalogue_declaration(
        self, view: str, path: Path, *, edit: bool = False
    ) -> None:
        kind = CATALOG[view]
        label = catalogue_ui.LABELS[view]
        registry = self.database_id
        if edit:
            context = self.context

            async def validate(candidate: Path) -> object:
                with database_environment(testing=context.testing), pinned_registry(registry):
                    return await self.controller.invoke(
                        f"catalog {kind} validate",
                        {"source": str(candidate)},
                        context,
                        lambda _: None,
                        self.edit_recipe,
                    )

            async def apply(original: bytes, updated: bytes) -> None:
                self.check_recipe_environment(registry, context.testing)
                await disk(catalogue_ui.update_draft, path, original, updated.decode())
                self.notify(f"{label} draft updated. Open the declaration to review registration.")

            self.queue_editor(path, validate, apply, None)
            return
        try:
            original = await disk(path.read_bytes)
            with TemporaryDirectory(prefix="marketforge-catalogue-") as directory:
                frozen = Path(directory) / path.name
                await disk(frozen.write_bytes, original)
                submitted = original
                values: Values = {"source": str(frozen)}
                valid = await self.catalogue_call(f"catalog {kind} validate", values)
                declaration = document(valid.get("declaration"))
                if not registry or self.database_id != registry:
                    raise ValueError(
                        "Connect to a verified registry before reviewing registration."
                    )
                preview = await self.catalogue_call(
                    f"catalog {kind} preview", values, expected_registry=registry
                )
                identity = (
                    declaration.get("code")
                    or declaration.get("instrument")
                    or ".".join(
                        value_at(declaration, field)
                        for field in ("exchange", "instrument_type", "market_category", "symbol")
                    )
                )
                review = {
                    "Entity": label,
                    "Identity": identity,
                    "Environment": self.database_environment.upper(),
                    "Registry": registry,
                    "Draft": str(path),
                    "Validation": "Schema and registry preflight passed",
                    **preview,
                    "Declaration": declaration,
                }
                if not await self.push_screen_wait(
                    Confirm(
                        f"Register {label}",
                        catalogue_ui.review_text(review),
                        confirm_label="Register",
                    )
                ):
                    return
                if self.database_id != registry or await disk(path.read_bytes) != submitted:
                    raise ValueError("The draft or registry changed since review.")
                result = await self.catalogue_call(
                    f"catalog {kind} add", values, expected_registry=registry
                )
                self.push_screen(
                    Inspection(
                        f"{label} registered",
                        {**result, "Reference": identity},
                        actions={
                            "Copy exact reference": partial(self.copy_dashboard_id, str(identity))
                        },
                    )
                )
                self.request_query()
        except Exception as error:
            self.push_screen(
                Inspection(
                    f"{label} validation or registration blocked",
                    {"Draft": str(path), "error": str(error)},
                    actions={
                        "Edit declaration": partial(self.new_catalogue, view, draft=path),
                        "Edit draft in Neovim": partial(
                            self.edit_catalogue_declaration, view, path
                        ),
                    },
                )
            )

    @work(exit_on_error=False)
    async def edit_catalogue_declaration(self, view: str, path: Path) -> None:
        await self.review_catalogue_declaration(view, path, edit=True)

    @work(exit_on_error=False)
    async def begin_profile(self, operation: str, row: Row | None = None) -> None:
        from forge.historical.profiles import service as profiles

        paths = self.context.paths
        try:
            if operation == "edit" and row:
                profile_id = row.reference
                path = catalogue_ui.draft_path(paths, profile_id)
                if not await disk(path.exists):
                    path = await disk(profiles.create_draft, paths, profile_id)
            else:
                values = await self.push_screen_wait(
                    Form(
                        Action(
                            f"{operation.title()} profile",
                            "Catalogue",
                            (
                                Field(
                                    "reference",
                                    "Profile ID (exchange.type.market.trades or depth)",
                                    required=True,
                                ),
                            ),
                        ),
                        local=True,
                        submit_label="Open draft",
                        documentation=partial(
                            self.catalogue_docs, catalogue_ui.documentation_url("Profiles")
                        ),
                    )
                )
                if values is None:
                    return
                profile_id = str(values["reference"])
                if operation == "clone" and row and row.path:
                    path = await disk(catalogue_ui.clone_profile, paths, row.path, profile_id)
                    self.notify("Review the clone’s format code and mappings against its source.")
                elif operation == "new":
                    path = await disk(profiles.new_draft, paths, profile_id)
                else:
                    path = catalogue_ui.draft_path(paths, profile_id)
            await self.show_profile_draft(path)
        except Exception as error:
            self.push_screen(Inspection("Profile draft unavailable", {"error": str(error)}))

    async def show_profile_draft(self, path: Path, *, replace_screen: bool = False) -> None:
        from forge.historical.profiles import service as profiles

        entry = await disk(profiles.inspect, path)
        definition = document(entry["definition"])
        profile_id = str(entry["id"])

        def choose(_: str, field: Mapping[str, object]) -> None:
            self.edit_profile_field(path, field, str(entry["sha256"]))

        if replace_screen:
            self.pop_screen()
        self.push_screen(
            Inspection(
                f"Profile draft · {profile_id}",
                entry,
                sections={
                    "Fields": catalogue_ui.profile_parameters(definition, profile_id),
                    "Summary": {
                        **entry,
                        "Scope": "Managed workspace files shared by registry environments",
                    },
                },
                actions={
                    "Configure profile fields": partial(
                        self.configure_profile, path, str(entry["sha256"])
                    ),
                    "Edit in Neovim": partial(self.edit_profile_toml, path),
                    "Rust validate and review save": partial(self.save_profile, path),
                    "Documentation": partial(
                        self.catalogue_docs, catalogue_ui.documentation_url("Profiles")
                    ),
                },
                choose=choose,
            )
        )

    @work(exit_on_error=False)
    async def configure_profile(self, path: Path, fingerprint: str) -> None:
        from forge.historical.profiles import service as profiles

        try:
            original = await disk(path.read_bytes)
            if sha256(original).hexdigest() != fingerprint:
                raise ValueError("Draft changed since loading the fields. Reopen the draft.")
            fields, preset = catalogue_ui.profile_form(tomllib.loads(original.decode()), path.stem)

            async def rust_validate(values: Values) -> dict[str, str]:
                candidate, errors = catalogue_ui.profile_form_candidate(
                    original, fields, preset, values
                )
                if errors:
                    return errors
                try:
                    with TemporaryDirectory(prefix="marketforge-profile-form-") as folder:
                        draft = Path(folder) / path.name
                        await disk(draft.write_bytes, candidate)
                        await disk(profiles.validate, self.context.paths, draft)
                except Exception as error:
                    return {"": str(error)}
                return {}

            values = await self.push_screen_wait(
                Form(
                    Action(f"Configure profile · {path.stem}", "Catalogue", fields),
                    preset,
                    local=True,
                    submit_label="Update validated draft",
                    validate=rust_validate,
                    locked=frozenset({"profile"}),
                    open_docs=self.catalogue_docs,
                    documentation=partial(
                        self.catalogue_docs, catalogue_ui.documentation_url("Profiles")
                    ),
                )
            )
            if values is not None:
                candidate, _ = catalogue_ui.profile_form_candidate(original, fields, preset, values)
                if candidate != original:
                    await disk(catalogue_ui.update_draft, path, original, candidate.decode())
                await self.show_profile_draft(path, replace_screen=True)
        except Exception as error:
            self.push_screen(Inspection("Profile configuration blocked", {"Error": str(error)}))

    @work(exit_on_error=False)
    async def edit_profile_field(
        self, path: Path, field: Mapping[str, object], fingerprint: str
    ) -> None:
        try:
            original = await disk(path.read_bytes)
            if sha256(original).hexdigest() != fingerprint:
                raise ValueError("Draft changed since loading the fields. Reopen the draft.")
            key = str(field["Field"])
            if any(token in key for token in ("<", "[")):
                self.push_screen(
                    Inspection(
                        key,
                        field,
                        actions={
                            "Edit in Neovim": partial(self.edit_profile_toml, path),
                            "Field documentation": partial(
                                self.catalogue_docs, str(field["Documentation"])
                            ),
                        },
                    )
                )
                return
            import tomllib

            from forge.shared.config.toml import value

            choices = catalogue_ui.profile_choices(key, tomllib.loads(original.decode()))
            current = str(field.get("Value") or "")
            if choices and current:
                current = str(tomllib.loads("value = " + current)["value"])
            values = await self.push_screen_wait(
                Form(
                    Action(
                        key,
                        "Catalogue",
                        (
                            Field(
                                "value",
                                str(field["Help"])
                                if choices
                                else f"{field['Help']} · TOML value (blank removes field)",
                                "choice" if choices else "text",
                                choices,
                            ),
                        ),
                    ),
                    {"value": current if not choices or current in choices else ""},
                    local=True,
                    submit_label="Update draft",
                    documentation=partial(self.catalogue_docs, str(field["Documentation"])),
                )
            )
            if values is not None:
                raw = values["value"]
                updated = await disk(
                    catalogue_ui.change_profile_field,
                    original,
                    key,
                    value(raw) if choices and raw else str(raw),
                )
                await disk(catalogue_ui.update_draft, path, original, updated)
                await self.show_profile_draft(path, replace_screen=True)
        except Exception as error:
            self.push_screen(Inspection("Profile field update blocked", {"error": str(error)}))

    @work(exit_on_error=False)
    async def edit_profile_toml(self, path: Path) -> None:
        paths = self.context.paths

        async def validate(candidate: Path) -> object:
            return await disk(validate_recipe_draft, paths, "Profile", candidate)

        async def apply(original: bytes, updated: bytes) -> None:
            await disk(catalogue_ui.update_draft, path, original, updated.decode())
            if self.area == "Catalogue":
                self.request_query()
            # Refresh the profile view only after an explicit editor review.
            if (
                isinstance(self.screen, Inspection)
                and self.screen.heading == f"Editor · {path.name}"
            ):
                self.pop_screen()
                current = self.screen_stack[-1]
                if (
                    isinstance(current, Inspection)
                    and current.heading == f"Profile draft · {path.stem}"
                ):
                    await self.show_profile_draft(path, replace_screen=True)

        self.queue_editor(path, validate, apply, None)

    @work(exit_on_error=False)
    async def validate_installed_profile(self, row: Row) -> None:
        from forge.historical.profiles import service as profiles

        try:
            assert row.path is not None
            result = await disk(profiles.validate, self.context.paths, row.path)
            self.validated_profiles.add(str(result["sha256"]))
            self.render_rows()
            self.push_screen(
                Inspection(
                    "Rust profile validation",
                    result,
                    sections={
                        "Summary": result,
                        "Engine": result.get("engine"),
                    },
                )
            )
        except Exception as error:
            self.push_screen(Inspection("Rust profile validation failed", {"error": str(error)}))

    @work(exit_on_error=False)
    async def save_profile(self, path: Path) -> None:
        from forge.historical.profiles import service as profiles

        registry = self.database_id
        try:
            result = await disk(profiles.validate, self.context.paths, path)
            profile_id = str(result["id"])
            destination = profiles.installed(self.context.paths, profile_id)
            if not registry or registry != self.database_id:
                raise ValueError("Connect to a verified registry for the profile venue check.")
            await self.catalogue_call(
                "catalog exchange show", {"reference": profile_id.split(".", 1)[0]}
            )
            operation = (
                "Check for identical installed interpretation"
                if destination.exists()
                else "Install validated profile"
            )
            if not await self.push_screen_wait(
                Confirm(
                    "Save validated profile",
                    plain(
                        {
                            "Profile": profile_id,
                            "Scope": "Managed workspace files shared by registry environments",
                            "Venue check": f"{self.database_environment.upper()} · {registry}",
                            "Draft": str(path),
                            "Destination": str(destination),
                            "Operation": operation,
                            "Validation": result["validation"],
                            "SHA-256": result["sha256"],
                            "Warning": "Source data compatibility requires a separate source check.",
                        }
                    ),
                )
            ):
                return
            if registry != self.database_id:
                raise ValueError("Registry changed since review.")
            with database_environment(testing=self.context.testing), pinned_registry(registry):
                saved = await profiles.register(
                    self.context.paths, profile_id, path, expected_sha256=str(result["sha256"])
                )
            self.validated_profiles.add(str(saved["sha256"]))
            self.push_screen(
                Inspection(
                    "Profile save result",
                    saved,
                    sections={
                        "Summary": saved,
                        "Engine": saved.get("engine"),
                    },
                )
            )
            self.request_query()
        except Exception as error:
            self.push_screen(
                Inspection(
                    "Profile validation or save blocked", {"Draft": str(path), "error": str(error)}
                )
            )

    @work(exit_on_error=False)
    async def open_row(self, row: Row) -> None:
        await self.inspect_row(row)

    def filter_fields(self) -> tuple[Field, ...]:
        key = None
        if self.area == "Data":
            return ()

        elif self.area == "Markets" and self.view == "Instruments":
            key = "markets list"
        elif self.area == "Markets" and self.view == "Rankings":
            return (
                Field("currency", "Comparison currency"),
                Field("universe", "Universe", "choice", ("complete", "partial")),
            )
        if self.view == "Profiles":
            return (
                *BY_KEY["normalize profiles list"].fields,
                Field("format", "Input format", "choice", ("csv", "jsonl", "xlsx")),
            )
        if self.area == "Catalogue" and self.view == "Instruments":
            return tuple(
                Field(key, label)
                for key, label in (
                    ("exchange", "Exchange"),
                    ("instrument", "Exact symbol"),
                    ("instrument_type", "Instrument type"),
                    ("market_category", "Market category"),
                    ("base_asset", "Base asset"),
                    ("quote_asset", "Quote asset"),
                    ("settlement_asset", "Settlement asset"),
                    ("status", "Status"),
                )
            )
        if self.area == "Runs" and self.view == "Indexed":
            return tuple(
                Field(name, name.title())
                for name in ("operation", "action", "status", "recipe", "start", "end")
            )
        if self.area == "Runs" and self.view == "Retained":
            return tuple(
                Field(key, label)
                for key, label in (
                    ("exchange", "Exchange"),
                    ("operation", "Operation"),
                    ("recipe", "Recipe"),
                    ("state", "Recorded state"),
                    ("scope", "Instrument or scope"),
                    ("identity", "Run ID"),
                    ("start", "Started at or after (UTC)"),
                    ("end", "Started before (UTC)"),
                )
            ) + (
                Field("minimum_duration", "Minimum duration in seconds", "number"),
                Field("output", "Output evidence", "choice", ("present", "absent")),
            )
        excluded = {"limit", "offset", "operation", "sort"}
        common = (
            tuple(field for field in BY_KEY[key].fields if field.key not in excluded) if key else ()
        )
        if key == "markets list":
            return common + (
                Field("instrument_type", "Instrument type"),
                Field("settlement_asset", "Settlement asset"),
                Field("status", "Instrument status"),
                Field("availability", "Observations", "choice", ("available", "missing")),
            )
        return common

    def sort_fields(self) -> tuple[str, ...]:
        if self.area == "Workflows":
            return ("Recipe", "Workflow", "State")
        if self.area == "Data":
            return ()
        if self.area == "Markets" and self.view == "Rankings":
            return ("Policy", "Currency", "Ranked", "Published UTC", "Universe")
        if self.area == "Markets" and self.view == "Instruments":
            return ("Symbol", "Exchange", "Turnover 24h", "Observed UTC")
        if self.area == "Catalogue" and self.view == "Instruments":
            return ("Declaration", "Exchange", "Type", "State")
        if self.area == "Runs" and self.view == "Indexed":
            from forge.orchestration.sorting import RUNS

            return tuple(RUNS.fields)
        if self.area == "Runs" and self.view == "Retained":
            return (
                "Operation",
                "Exchange",
                "Scope",
                "Outcome",
                "Recipe",
                "Started UTC",
                "Duration",
            )
        return ()

    def heading_sort(self, label: str) -> str:
        if self.view == "Indexed":
            return {"Started UTC": "start", "Run ID": "id"}.get(label, label.lower())
        return label

    @on(DataTable.HeaderSelected, "#results")
    def sort_header(self, event: DataTable.HeaderSelected) -> None:
        label = self.heading_sort(event.column_key.value or "")
        if label in self.sort_fields():
            self.state.descending = not self.state.descending if self.state.sort == label else False
            self.state.sort = label
            self.state.offset = 0
            self.request_query()

    @work(exit_on_error=False)
    async def open_filters(self) -> None:
        sorts = self.sort_fields()
        fields = self.filter_fields() + (
            (
                Field("_sort", "Sort column", "choice", sorts),
                Field("_descending", "Descending", "check"),
            )
            if sorts
            else ()
        )
        if not fields:
            return
        values = await self.push_screen_wait(
            Form(
                Action("Filters", self.area, fields),
                {
                    **self.state.filters,
                    "_sort": self.state.sort,
                    "_descending": self.state.descending,
                },
                (),
                self.context.paths.root,
                local=True,
            )
        )
        if values is not None:
            self.state.sort = str(values.pop("_sort", ""))
            self.state.descending = bool(values.pop("_descending", False))
            self.state.filters = {key: value for key, value in values.items() if value != ""}
            self.state.offset = 0
            self.request_query()

    @work(exit_on_error=False)
    async def update_merge_source(self, row: Row) -> None:
        from forge.shared.config.library import resolve_config_file

        if not row.path:
            return
        directory = library(self.context, "merge")
        values = await self.push_screen_wait(
            Form(
                Action(
                    "Replace a derived merge source",
                    "Workflows",
                    (Field("reference", "Merge recipe", required=True),),
                ),
                {},
                await disk(list_config_names, directory),
                self.context.paths.root,
                local=True,
            )
        )
        if values is None:
            return
        name = str(values["reference"])
        try:
            destination = resolve_config_file(directory, name)
            expected = await disk(destination.read_bytes)
            if not await self.push_screen_wait(
                Confirm(
                    "Replace the single normalization source",
                    f"{destination}\nNew source · {row.reference}",
                )
            ):
                return
            path = await disk(
                replace_merge_source,
                self.context.paths,
                name,
                row.reference,
                expected,
            )
            self.notify(success_message(f"Updated {path.name}"), markup=True)
            self.request_query()
        except Exception as error:
            self.notify(str(error), severity="error")

    def check_recipe_environment(self, registry: str | None, testing: bool) -> None:
        if registry != self.database_id or testing != self.context.testing:
            raise ValueError("The selected environment changed. Refresh the source selection.")

    async def select_existing_recipe(
        self,
        family: str,
        title: str,
        source: str,
        describe: Callable[[bytes], Awaitable[tuple[bool, str]]],
    ) -> Path | None:
        directory = library(self.context, family)
        if not await disk(list_config_names, directory):
            choice = await self.push_screen_wait(
                ActionMenu(
                    f"No existing {title} recipes found. Create a recipe in Workflows → {title} first.",
                    ("Open Workflows", "Cancel"),
                )
            )
            if choice == "Open Workflows":
                await self.switch("Workflows", title)
            return None

        incompatible: set[str] = set()

        async def search(term: str) -> EntityChoices:
            matches: list[tuple[str, str]] = []
            incompatible.clear()
            for name in await disk(list_config_names, directory):
                if term.casefold() not in name.casefold():
                    continue
                path = resolve_config_file(directory, name)
                try:
                    compatible, state = await describe(await disk(path.read_bytes))
                except Exception as error:
                    compatible, state = False, str(error)
                if not compatible:
                    state = f"Incompatible · {state}"
                    incompatible.add(name)
                matches.append((f"{name} · {state}", name))
                if len(matches) == 30:
                    break
            return tuple(matches)

        name = await self.push_screen_wait(
            EntityPicker(
                f"Existing {title} recipe",
                search,
                "",
                context=source,
                selectable=lambda name: name not in incompatible,
            )
        )
        return resolve_config_file(directory, name) if name is not None else None

    @work(exit_on_error=False)
    async def add_to_recipe(
        self, selected: tuple[Row, ...], family: str, registry: str, testing: bool
    ) -> None:
        if self.recipe_activation:
            return
        self.recipe_activation = True
        try:
            self.check_recipe_environment(registry, testing)
            title = family.title()
            directory = library(self.context, family)
            names = tuple(await disk(list_config_names, directory))
            if not names:
                choice = await self.push_screen_wait(
                    ActionMenu(
                        f"No {title} recipes found. Create an {title} recipe in Workflows first."
                        if family == "acquisition"
                        else "No Pipeline recipes found. Create a Pipeline recipe in Workflows first.",
                        ("Open Workflows", "Cancel"),
                    )
                )
                if choice == "Open Workflows":
                    await self.switch("Workflows", title)
                return
            identities = tuple(market_recipes.instrument_from_row(row) for row in selected)
            instruments = tuple(item for item in identities if item is not None)
            if len(instruments) != len(selected):
                raise ValueError("Select registered instruments with complete market identities.")

            async def save(
                destination: market_recipes.Destination, requests: tuple[ArchiveRequest, ...]
            ) -> None:
                if self.controller.shutdown.emergency:
                    raise ValueError("Recipe saving is unavailable during emergency shutdown.")
                self.check_recipe_environment(registry, testing)
                with database_environment(testing=testing), pinned_registry(registry):
                    if await selected_registry_id(self.context.paths.root) != registry:
                        raise ValueError(
                            "The selected registry changed. Refresh the source selection."
                        )
                self.check_recipe_environment(registry, testing)
                with WorkspaceOwnership(self.context.paths):
                    await disk(
                        add_requests,
                        directory,
                        destination.path.stem,
                        requests,
                        expected=destination.original,
                        pipeline=family == "pipeline",
                    )

            result = await self.push_screen_wait(
                market_recipes.MarketRecipeDialog(family, directory, names, instruments, save)
            )
            if result is None:
                return
            path, additions = result
            self.request_query()
            self.push_screen(
                Inspection(
                    "Recipe updated",
                    {
                        "Recipe": path.stem,
                        "Path": str(path),
                        "Added": [market_recipes.preview_label(r) for r in additions],
                    },
                    actions=self.saved_recipe_actions(family, path.stem, path),
                )
            )
        except Exception as error:
            self.push_screen(Detail("Add to Recipe · Blocked", str(error), raw=True, error=True))
        finally:
            self.recipe_activation = False

    def show_duplicate_source(self) -> None:
        self.push_screen(
            Detail(
                "Already Included",
                "This source is already included in the selected recipe. No changes required.",
                raw=True,
            )
        )

    def selection(self, row: Row) -> Selection:
        item = document(row.value)
        if row.kind == "Publication":
            return Selection("publication", row.reference or row.id)
        if row.reference.isdecimal() and (row.kind == "File" or self.view == "Primary files"):
            return Selection("file", row.reference)
        if row.reference.isdecimal() and self.view in {"Raw", "Raw storage"}:
            return Selection("archive", row.reference)
        if row.kind == "Instruments" and item.get("symbol"):
            return Selection(
                "instrument",
                row.id,
                value_at(item, "reference", row.reference),
                value_at(item, "exchange", ""),
                value_at(item, "market_category", ""),
                value_at(item, "symbol", ""),
            )
        if self.area == "Runs" or row.kind in {"Run", "Retained run"}:
            return Selection("run", row.id)
        return Selection("recipe" if row.path else "other", row.id, row.reference)

    def preset(self, action: Action, row: Row | None) -> Values:
        return bind(action, self.selection(row)) if row else {}

    @work(exit_on_error=False)
    async def open_action(
        self, action: Action, row: Row | None = None, preset: Values | None = None
    ) -> None:
        choices: tuple[str, ...] = ()
        family = action.key.rsplit(" ", 1)[0]
        values_from_context = preset or self.preset(action, row)
        direct = {
            "datasets show",
            "datasets schema",
            "datasets verify",
            "datasets streams",
            "storage show",
            "markets show",
            "runs report",
            "runs logs",
            "pipeline status",
        }
        if values_from_context and action.key in direct:
            scope = (
                f" / Primary file {values_from_context['id']}"
                if "id" in values_from_context
                else ""
            )
            await self.inspect_request(action.key, values_from_context, action.key + scope)
            return
        if action.key in {
            "pipeline resume",
            "pipeline status",
            "runs show",
            "runs report",
            "runs logs",
        }:
            listing, _ = await disk(
                run_page,
                self.context.paths.runs_dir,
                100,
                0,
                pipeline_only=action.key.startswith("pipeline "),
            )
            choices = tuple(str(item["run_id"]) for item in listing)
        elif family in FAMILIES.values() and not action.key.endswith((" new", " register")):
            choices = await disk(list_config_names, library(self.context, family))
        if action.key == "merge new" and not values_from_context.get("from_normalize"):
            await self.new_merge_recipe()
            return
        if (
            action.key == "normalize new"
            and self.context.testing
            and not values_from_context.get("from_acquisition")
        ):
            source = await self.push_screen_wait(
                ActionMenu(
                    "New normalization recipe · choose inputs",
                    ("Historical requests", "Registered raw archives", "Acquisition recipe"),
                )
            )
            if source is None:
                return
            if source == "Registered raw archives":
                selected = await self.push_screen_wait(
                    Form(
                        Action(
                            "Select exact raw archives",
                            "Workflows",
                            (Field("archive_ids", "Selected archive IDs", required=True),),
                        ),
                        local=True,
                        submit_label="Resolve profiles",
                        search_fields=frozenset({"archive_ids"}),
                        entity_search=self.search_entities,
                        multiple_entities=frozenset({"archive_ids"}),
                    )
                )
                if selected:
                    try:
                        await self.author_exact_normalization(
                            tuple(int(item) for item in str(selected["archive_ids"]).split(","))
                        )
                    except ValueError as error:
                        self.push_screen(
                            Inspection("Archive selection blocked", {"Error": str(error)})
                        )
                return
            if source == "Acquisition recipe":
                seed = await self.push_screen_wait(
                    Form(
                        Action(
                            "Use an acquisition recipe",
                            "Workflows",
                            (Field("from_acquisition", "Acquisition recipe", required=True),),
                        ),
                        local=True,
                        submit_label="Configure normalization",
                        search_fields=frozenset({"from_acquisition"}),
                        entity_search=self.search_entities,
                    )
                )
                if seed is None:
                    return
                values_from_context = seed
        if action.key.endswith(" new") and family in {
            "acquisition",
            "pipeline",
            "normalize",
            "acquisition import",
            "acquisition register",
        }:
            initial: dict[str, object] = {}
            if seed := values_from_context.get("from_acquisition"):
                try:
                    source = resolve_config_file(library(self.context, "acquisition"), str(seed))
                    initial = await disk(lambda: tomllib.loads(source.read_text()))
                except (OSError, ValueError) as error:
                    self.push_screen(
                        Inspection("Acquisition recipe unavailable", {"Error": str(error)})
                    )
                    return
            try:
                await self.author_recipe(family, initial=initial)
            except Exception as error:
                self.push_screen(Inspection("Recipe authoring blocked", {"Error": str(error)}))
            return
        if action.key == "convert new":
            currencies = await self.available_currencies()
            if currencies:
                from dataclasses import replace

                action = replace(
                    action,
                    fields=(
                        *action.fields,
                        Field(
                            "catalogue_quote",
                            "Target quote from catalogue (type to search)",
                            "choice",
                            currencies,
                        ),
                    ),
                )
        locked_reference = bool(row and row.path and values_from_context.get("reference"))
        if (
            locked_reference
            and action.key.endswith((" validate", " show"))
            and len(action.fields) == 1
        ):
            values = values_from_context
        elif action.fields:
            values = await self.push_screen_wait(
                Form(
                    action,
                    values_from_context,
                    choices,
                    self.context.paths.root,
                    testing=self.context.testing,
                    search_fields=frozenset({"source"})
                    if action.key == "convert new"
                    else frozenset(),
                    entity_search=self.search_entities,
                    validate=self.validate_conversion_input
                    if action.key == "convert new"
                    else None,
                    submit_label="Configure recipe" if action.key == "convert new" else None,
                    locked=(
                        frozenset({"route", "source"})
                        if action.key == "convert new"
                        and values_from_context.get("route") == "publication"
                        else frozenset({"reference"})
                        if locked_reference
                        else frozenset()
                    ),
                )
            )
            if values is None:
                return
        else:
            values = {}
        if (
            action.effect == "editor"
            and action.key.endswith(" edit")
            and family in FAMILIES.values()
        ):
            name = str(values["reference"])
            try:
                path = resolve_config_file(library(self.context, family), name)
                self.edit_recipe_form(Row(name, name, family, "", {}, name, path))
            except Exception as error:
                self.push_screen(Detail(action.key, str(error), raw=True, error=True))
            return
        if action.key.startswith("catalog ") and action.key.endswith(" add"):
            view = next(view for view, kind in CATALOG.items() if kind == action.key.split()[1])
            await self.review_catalogue_declaration(
                view, self.context.paths.root / await disk(Path(str(values["source"])).expanduser)
            )
            return
        if action.key in {"normalize profiles add", "normalize profiles register"}:
            await self.save_profile(
                self.context.paths.root / await disk(Path(str(values["source"])).expanduser),
                expected_id=str(values["reference"]),
            ).wait()
            return
        if action.key in {"runs show", "runs report", "runs logs"}:
            await self.inspect_request(action.key, values, action.key)
            return
        if action.key == "convert new":
            await self.author_conversion(values)
            return
        if action.key.endswith(" new") and family in FAMILIES.values():
            if values.get("route") == "publication" or values.get("publication_ids"):
                if not await self.push_screen_wait(
                    Confirm(action.key, await self.review_scope(action, values))
                ):
                    return
            await self.create_recipe(action, values)
            return
        reviewed: tuple[Path, bytes] | None = None
        recipe_path = row.path if row else None
        if (
            action.key in EXECUTION_ACTIONS
            and family in FAMILIES.values()
            and not recipe_path
            and not action.key.endswith(" resume")
        ):
            reference = str(values.get("reference", ""))
            supplied = self.context.paths.root / await disk(Path(reference).expanduser)
            if supplied.suffix in {".toml", ".json"} and await disk(supplied.is_file):
                recipe_path = supplied
            elif "/" not in reference and not reference.endswith(".json"):
                try:
                    candidate = resolve_config_file(library(self.context, family), reference)
                    if await disk(candidate.is_file):
                        recipe_path = candidate
                except ValueError:
                    pass
        if action.key in EXECUTION_ACTIONS and recipe_path:
            try:
                reviewed = recipe_path, await disk(recipe_path.read_bytes)
                if recipe_path.suffix != ".json" and (
                    validation := BY_KEY.get(f"{family} validate")
                ):
                    with (
                        database_environment(testing=self.context.testing),
                        pinned_registry(self.database_id),
                    ):
                        evidence = await self.controller.invoke(
                            validation.key,
                            {"reference": values["reference"]},
                            self.context,
                            lambda _: None,
                            self.edit_recipe,
                        )
                    if outcome(evidence) not in {"valid", "validated", "complete", "completed"}:
                        self.push_screen(Inspection("Execution blocked by validation", evidence))
                        return
            except Exception as error:
                self.push_screen(Detail("Execution preflight", str(error), raw=True, error=True))
                return
        if action.effect in {"engine", "network", "write"} and not read_only(action, values):
            review = await self.review_scope(action, values)
            if reviewed:
                review += (
                    f"\nReviewed input  {reviewed[0]}\nSHA-256  {sha256(reviewed[1]).hexdigest()}"
                )
            label = (
                "Run"
                if action.key in EXECUTION_ACTIONS
                else "Prepare"
                if action.key.endswith(" prepare")
                else "Register"
                if action.key == "acquisition register run"
                else "Confirm"
            )
            if not await self.push_screen_wait(Confirm(action.key, review, confirm_label=label)):
                return
        await self.start_action(action, values, reviewed=reviewed)

    async def create_recipe(self, action: Action, values: Values) -> None:
        if self.controller.shutdown.emergency:
            return

        async def deferred_editor(_: Path) -> None:
            return

        try:
            with WorkspaceOwnership(self.context.paths), pinned_registry(self.database_id):
                result = document(
                    await self.controller.invoke(
                        action.key, values, self.context, lambda _: None, deferred_editor
                    )
                )
            family = action.key.rsplit(" ", 1)[0]
            name = str(values["reference"]).removesuffix(".toml")
            if self.area == "Workflows" and FAMILIES.get(self.view) == family:
                self.state.selection = name
            elif self.view == "Recipes":
                self.state.selection = f"{family}:{name}"
            path = result.get("path")
            if isinstance(path, str):
                if values.get("route") == "publication" or values.get("publication_ids"):
                    self.push_screen(
                        Inspection(
                            "Recipe saved",
                            result,
                            actions=self.saved_recipe_actions(family, name, Path(path)),
                        )
                    )
                else:
                    self.edit_recipe_form(Row(name, name, family, "", {}, name, Path(path)))
            self.request_query()
        except Exception as error:
            self.notify(str(error), severity="error")

    async def review_scope(self, action: Action, values: Values) -> str:
        text = review_text(values, self.database_environment)
        if self.database_id:
            text += f"\nRegistry  {self.database_id}"
        if action.key in EXECUTION_ACTIONS:
            text += (
                "\nExecution  Managed operation in a separate terminal when available. "
                "Closing an active execution terminal interrupts the operation."
            )
        family = action.key.rsplit(" ", 1)[0]
        reference = values.get("reference")
        if family not in FAMILIES.values() or not isinstance(reference, str) or "/" in reference:
            return text
        import tomllib

        try:
            data = await disk(
                read_source,
                library(self.context, family) / (reference.removesuffix(".toml") + ".toml"),
            )
            interval = document(tomllib.loads(data).get("interval"))
        except (OSError, ValueError):
            return text
        if interval.get("start") and interval.get("end"):
            text += f"\nRequested interval  {interval['start']} → {interval['end']} (end exclusive)"
        return text

    async def start_action(
        self, action: Action, values: Values, *, reviewed: tuple[Path, bytes] | None = None
    ) -> None:
        if self.controller.shutdown.emergency:
            return
        anchor = self.screen
        if action.effect == "editor":
            try:
                result = await self.controller.invoke(
                    action.key, values, self.context, lambda _: None, self.edit_recipe
                )
                self.push_screen(Detail(action.key, result))
                self.request_query()
            except Exception as error:
                self.push_screen(Detail(action.key, str(error), raw=True, error=True))
            return
        if action.key == "normalize prepare" and values.get("from_acquisition"):

            async def keep_generated_recipe(_: Path) -> None:
                return

            try:
                await self.controller.invoke(
                    "normalize new", values, self.context, lambda _: None, keep_generated_recipe
                )
            except Exception as error:
                self.push_screen(Detail(action.key, str(error), raw=True, error=True))
                return
            values = {**values, "from_acquisition": ""}
        if read_only(action, values) and self.controller.active():
            op = Operation("inspection", action.key)
            try:
                op.result = await self.controller.invoke(
                    action.key, values, self.context, lambda _: None, self.edit_recipe
                )
                op.state = outcome(op.result)
            except Exception as error:
                op.state, op.error = "failed", str(error)
            if self.screen is anchor:
                self.show_operation(op, action, values)
            else:
                self.notify(f"{op.title} · {op.state}")
            return
        try:
            execution = action.key in EXECUTION_ACTIONS and not read_only(action, values)
            desktop = execution and not self.is_headless
            if execution and not desktop and values.get("engine_tui"):
                values = {**values, "engine_tui": False}
            if desktop:

                def terminal_unavailable(message: str) -> None:
                    self.notify(safe_text(message), severity="warning")
                    if self.screen is anchor and self.controller.current is not None:
                        self.show_operation(self.controller.current, action, values)

                task = self.controller.launch(
                    action,
                    values,
                    reviewed=reviewed,
                    desktop=True,
                    on_terminal_unavailable=terminal_unavailable,
                )
            elif reviewed is None:
                task = self.controller.launch(action, values)
            else:
                task = self.controller.launch(action, values, reviewed=reviewed)
            if execution and not desktop:
                assert self.controller.current is not None
                self.show_operation(self.controller.current, action, values)
            op = await asyncio.shield(task)
        except Exception as error:
            self.notify(safe_text(str(error)), severity="warning")
            return
        if action.effect == "guarded" and op.error is None:
            await self.review_action(action, values, op)
        elif self.screen is anchor and (
            read_only(action, values) or action.key.endswith((" prepare", " map"))
        ):
            self.show_operation(op, action, values)
        else:
            self.notify(f"{op.title} · {op.state}. Ctrl+O opens the result.")

    async def review_action(self, action: Action, values: Values, op: Operation) -> None:
        reviewed = dict(document(op.result))
        apply: Callable[[], object] | None = None
        if (
            action.key in {"storage prune", "storage delete"}
            and reviewed.get("preview_path")
            and reviewed.get("review_complete")
        ):

            async def apply_storage() -> object:
                return await apply_cleanup(
                    "prune" if action.key.endswith("prune") else "delete", reviewed, self.context
                )

            apply = partial(self.apply_review, action, values, apply_storage)
        elif action.key == "markets rank delete":
            identities = frozenset(value_at(item, "id") for item in records(reviewed, "snapshots"))
            eligible = frozenset(
                value_at(item, "id")
                for item in records(reviewed, "snapshots")
                if item.get("eligible")
            )
            if eligible:

                async def apply_ranks() -> object:
                    return await ranks(
                        "delete", values, self.context, lambda _: None, (identities, eligible)
                    )

                apply = partial(self.apply_review, action, values, apply_ranks)
        self.push_screen(
            Inspection(
                "Review exact selection",
                op.result,
                actions={"Apply reviewed selection": apply} if apply else None,
            )
        )

    @work(exit_on_error=False)
    async def apply_review(
        self, action: Action, values: Values, callback: Callable[[], Awaitable[object]]
    ) -> None:
        if not await self.push_screen_wait(
            Confirm(
                "Apply reviewed selection", action.key, destructive=action.key.endswith(" delete")
            )
        ):
            return
        op = await asyncio.shield(self.controller.launch(action, values, callback))
        self.show_operation(op)
        self.request_query()

    @staticmethod
    def inspection_files(label: str, reference: object, scope: Path) -> tuple[RetainedFile, ...]:
        target = RetainedFile.reference(label, reference, scope)
        return (target,) if target is not None else ()

    async def terminal_editor(
        self,
        path: Path,
        *,
        readonly: bool = False,
        filetype: str = "",
        on_ready: Callable[[], None] | None = None,
    ) -> None:
        await edit_in_window(
            path,
            cwd=self.context.paths.root,
            editor="nvim" if readonly else self.context.settings.presentation.editor,
            readonly=readonly,
            filetype=filetype,
            on_ready=on_ready,
        )

    @property
    def editor_open(self) -> bool:
        return bool(
            self.inspection_sessions or any(item.busy for item in self.editor_sessions.values())
        )

    @work(group="editor-windows", exit_on_error=False)
    async def inspect_in_editor(self, target: InspectionTarget) -> None:
        if self.controller.shutdown.emergency:
            return
        identity = target.path if isinstance(target, RetainedFile) else id(target)
        if identity in self.inspection_sessions:
            self.notify("This inspection is already open.", severity="warning")
            return
        self.inspection_sessions.add(identity)
        origin = self.screen
        try:
            with TemporaryDirectory(prefix="marketforge-inspect-") as directory:
                path, filetype, size = await disk(stage_inspection, target, Path(directory))
                self.mark_missing_inspection(origin, target, missing=False)
                if self.screen is not origin:
                    return
                if size > LARGE_INSPECTION and not await self.push_screen_wait(
                    Confirm(
                        "Open large inspection?",
                        f"{size / (1024 * 1024):,.1f} MiB. Neovim may take time to load this evidence.",
                        confirm_label="Open",
                    )
                ):
                    return
                await self.terminal_editor(path, readonly=True, filetype=filetype)
        except OSError as error:
            if isinstance(error, FileNotFoundError):
                self.mark_missing_inspection(origin, target, missing=True)
            message = (
                f"{target.path}: {error.strerror or error}"
                if isinstance(target, RetainedFile)
                else str(error)
            )
            self.notify(message, title="Neovim inspection failed", severity="error")
        except EditorCompletionUnconfirmed as error:
            self.notify(str(error), title="Neovim completion unconfirmed", severity="warning")
        except EditorInterrupted as error:
            self.notify(str(error), title="Neovim inspection interrupted", severity="warning")
        except (EditorError, ValueError, TypeError, RuntimeError) as error:
            self.notify(str(error), title="Neovim inspection failed", severity="error")
        finally:
            self.inspection_sessions.discard(identity)

    @staticmethod
    def mark_missing_inspection(
        screen: Screen[object], target: InspectionTarget, *, missing: bool
    ) -> None:
        if isinstance(target, RetainedFile):
            for button in screen.query(OpenInspection):
                if button.target is target and button.parent is not None:
                    button.parent.set_class(missing, "artifact-missing")

    async def edit_recipe(self, path: Path) -> None:
        """Route service-requested editing through the managed recipe workflow."""
        for family in FAMILIES.values():
            directory = library(self.context, family)
            if path.parent.resolve() == directory.resolve():
                self.edit_selected(Row(path.stem, path.stem, family, "", {}, path.stem, path))
                return
        raise EditorError(f"No managed editing workflow for {path}.")

    def queue_editor(
        self, target: Path, validate: ValidateDraft, apply: ApplyDraft, original: bytes | None
    ) -> None:
        if self.controller.shutdown.emergency:
            return
        try:
            identity = target.resolve()
        except (OSError, RuntimeError) as error:
            self.notify(str(error), title="Editor target unavailable", severity="error")
            return
        if session := self.editor_sessions.get(identity):
            if session.busy:
                self.notify(f"{target.name} is already being edited.", severity="warning")
            else:
                self.show_editor_session(session)
            return
        session = EditorSession(identity, validate, apply, original)
        self.editor_sessions[identity] = session
        self.run_editor_session(session)

    @work(group="editor-windows", exit_on_error=False)
    async def run_editor_session(self, session: EditorSession, *, reopen: bool = True) -> None:
        session.busy = True
        session.state = "Opening"
        session.updated = None
        session.error = ""
        try:
            draft = await disk(session.stage)
            if reopen:
                self.notify(f"{session.target.name} · F9 for editor sessions")
                await self.terminal_editor(
                    draft, on_ready=lambda: setattr(session, "state", "Editing")
                )
            updated = await disk(draft.read_bytes)
            session.modified = updated != session.original
            if updated == session.original:
                await disk(session.discard)
                del self.editor_sessions[session.target]
                self.notify(f"{session.target.name} · No changes detected")
                return
            session.state = "Validating"
            await session.validate(draft)
            updated.decode("utf-8")
            if updated != await disk(draft.read_bytes):
                raise EditorError(
                    "The draft changed during validation. Reopen and close it to validate again."
                )
            session.updated = updated
            session.state = "Ready for review"
        except asyncio.CancelledError:
            session.state = "Interrupted"
            raise
        except Exception as error:
            if session.state == "Validating":
                session.state = "Validation failed"
            elif isinstance(error, EditorCompletionUnconfirmed):
                session.state = "Completion unconfirmed"
            elif isinstance(error, EditorInterrupted):
                session.state = "Interrupted"
            elif isinstance(error, EditorLaunchFailed):
                session.state = "Window or editor launch failed"
            else:
                session.state = "Editor failed"
            if session.draft is not None:
                try:
                    session.modified = await disk(session.draft.read_bytes) != session.original
                except OSError:
                    session.modified = None
            session.error = str(error)
        finally:
            session.busy = False
        self.notify(
            f"{session.target.name} · {session.state}. F9 for details.",
            severity="error" if session.error else "information",
        )

    @work(exit_on_error=False)
    async def action_editor_sessions(self) -> None:
        sessions = {str(target): session for target, session in self.editor_sessions.items()}
        if not sessions:
            self.notify("No pending editor sessions.")
            return
        choice = await self.push_screen_wait(ActionMenu("Editor sessions", tuple(sessions)))
        if choice is not None:
            self.show_editor_session(sessions[choice])

    def show_editor_session(self, session: EditorSession) -> None:
        actions: dict[str, Callable[[], object]] = {}
        details: dict[str, object] = {
            "Target": str(session.target),
            "Draft": str(session.draft or ""),
            "State": session.state,
            "Session": session.id,
        }
        if session.original is not None:
            details["Original SHA-256"] = sha256(session.original).hexdigest()
        if session.modified is not None:
            details["Draft changes"] = "Modified" if session.modified else "Unchanged"
        if session.error:
            details["Diagnostic"] = session.error
        if session.draft:
            actions["Copy draft path"] = partial(self.copy_value, str(session.draft))
        if not session.busy:
            if session.updated is not None and session.original is not None:
                details["Changes"] = "".join(
                    difflib.unified_diff(
                        session.original.decode(errors="replace").splitlines(keepends=True),
                        session.updated.decode().splitlines(keepends=True),
                        "Original",
                        "Validated draft",
                    )
                )
                actions["Apply reviewed changes"] = partial(self.apply_editor_session, session)
                if session.error:
                    actions["Compare with current file"] = partial(
                        self.compare_editor_session, session
                    )
            actions["Reopen in Neovim" if session.draft else "Retry"] = partial(
                self.reopen_editor_session, session
            )
            if session.draft and session.updated is None:
                actions["Validate saved draft"] = partial(self.validate_saved_draft, session)
            actions["Discard draft"] = partial(self.discard_editor_session, session)
        changes = details.pop("Changes", None)
        self.push_screen(
            Inspection(
                f"Editor · {session.target.name}",
                details,
                actions=actions,
                sections={"Summary": details, "Changes": changes} if changes is not None else None,
                section="Changes" if changes is not None else None,
            )
        )

    def validate_saved_draft(self, session: EditorSession) -> None:
        if (
            self.editor_sessions.get(session.target) is session
            and not session.busy
            and session.draft is not None
            and not self.controller.shutdown.emergency
        ):
            session.busy = True
            self.run_editor_session(session, reopen=False)

    @work(exit_on_error=False)
    async def compare_editor_session(self, session: EditorSession) -> None:
        if session.updated is None:
            return
        screen = self.screen
        try:
            current = await disk(session.target.read_bytes)
        except OSError as error:
            self.notify(str(error), title="Current file unavailable", severity="error")
            return
        if self.screen is screen:
            diff = "".join(
                difflib.unified_diff(
                    current.decode(errors="replace").splitlines(keepends=True),
                    session.updated.decode().splitlines(keepends=True),
                    "Current file",
                    "Validated draft",
                )
            )
            self.push_screen(
                Detail("Current file and editor draft", diff or "Same contents", raw=True)
            )

    def reopen_editor_session(self, session: EditorSession) -> None:
        if (
            not self.controller.shutdown.emergency
            and self.editor_sessions.get(session.target) is session
            and not session.busy
        ):
            session.busy = True
            self.run_editor_session(session)

    @work(exit_on_error=False)
    async def discard_editor_session(self, session: EditorSession) -> None:
        if self.editor_sessions.get(session.target) is not session or session.busy:
            return
        session.busy = True
        try:
            if await self.push_screen_wait(
                Confirm(
                    "Discard editor draft?", str(session.draft or session.target), destructive=True
                )
            ):
                await disk(session.discard)
                del self.editor_sessions[session.target]
                self.notify("Editor draft discarded.")
        except OSError as error:
            self.notify(str(error), severity="error")
        finally:
            session.busy = False

    @work(exit_on_error=False)
    async def apply_editor_session(self, session: EditorSession) -> None:
        if self.editor_sessions.get(session.target) is not session or session.busy:
            return
        original, updated, draft = session.original, session.updated, session.draft
        if original is None or updated is None or draft is None:
            return
        session.busy = True
        try:
            if not await self.push_screen_wait(
                Confirm("Apply reviewed changes?", str(session.target), confirm_label="Apply")
            ):
                return
            updated.decode("utf-8")
            if updated != await disk(draft.read_bytes):
                raise EditorError(
                    "The draft changed after validation. Reopen and validate it again."
                )
            await session.apply(original, updated)
            del self.editor_sessions[session.target]
            await disk(session.discard)
            self.notify(success_message("Reviewed changes accepted"), markup=True)
        except Exception as error:
            session.state = "Apply blocked"
            session.error = str(error)
            self.notify(str(error), title="Draft retained", severity="error")
        finally:
            session.busy = False

    async def author_conversion(self, values: Values) -> None:
        from forge.historical.conversion.evidence import inspect_publication_source
        from forge.historical.conversion.recipes import SourceKind, load_source

        route = str(values["route"])
        reference = str(values["source"])
        names = str(values["reference"])
        registry = self.database_id
        testing = self.context.testing
        try:
            with database_environment(testing=testing), pinned_registry(registry):
                if route == "publication":
                    source = await inspect_publication_source(self.context.paths, int(reference))
                    declaration = source.model_dump(mode="json")
                    interval = source.interval
                else:
                    loaded = await disk(
                        load_source, self.context.paths, cast(SourceKind, route), reference
                    )
                    declaration = loaded.declaration.model_dump(mode="json")
                    interval = loaded.interval
            if registry != self.database_id or testing != self.context.testing:
                raise ValueError("The selected registry changed during source binding.")
            await self.author_recipe(
                "convert",
                initial={
                    "version": 2,
                    "source": declaration,
                    "interval": interval.model_dump(),
                    **(
                        {"selection": {"whole_publication": True}} if route == "publication" else {}
                    ),
                    "conversion": {
                        "target_quote": values["quote"],
                        "max_staleness_ns": 60_000_000_000,
                    },
                },
                name=names,
            )
        except Exception as error:
            self.push_screen(Inspection("Conversion source unavailable", {"Error": str(error)}))

    async def validate_conversion_input(self, values: Values) -> dict[str, str]:
        from pydantic import TypeAdapter, ValidationError

        from forge.historical.conversion.contract import Currency
        from forge.historical.merge.config import DatabaseID
        from forge.shared.config.library import config_filename

        issues: dict[str, str] = {}
        try:
            config_filename(str(values["reference"]))
        except ValueError as error:
            issues["reference"] = str(error)
        try:
            TypeAdapter(Currency).validate_python(values["quote"])
        except ValidationError as error:
            issues["quote"] = str(error.errors(include_url=False)[0]["msg"])
        if values["route"] == "publication":
            identity = str(values["source"])
            if not identity.isascii() or not identity.isdecimal():
                issues["source"] = "Choose a registered merged publication."
            else:
                try:
                    TypeAdapter(DatabaseID).validate_python(int(identity))
                except (ValueError, ValidationError):
                    issues["source"] = (
                        "Choose a positive registry publication ID in the supported range."
                    )
        return issues

    async def new_merge_recipe(self) -> None:
        route = await self.push_screen_wait(
            ActionMenu(
                "New merge recipe · choose inputs",
                ("Registered normalized publications", "Normalization recipe", "Physical file IDs"),
            )
        )
        if route is None:
            return
        if route == "Physical file IDs":
            await self.author_recipe("merge")
            return
        field = (
            Field(
                "publication_ids",
                "Selected registry publication IDs",
                required=True,
                group="Source publications",
            )
            if route == "Registered normalized publications"
            else Field("from_normalize", "Normalization recipe", required=True, group="Source")
        )
        values = await self.push_screen_wait(
            Form(
                Action(
                    "New merge recipe",
                    "Workflows",
                    (
                        Field("reference", "Recipe name", required=True, group="Recipe"),
                        field,
                        Field(
                            "timeline_ordering",
                            "Timeline ordering",
                            "choice",
                            ("chronological", "causal"),
                            group="Merge policy",
                        ),
                    ),
                ),
                local=True,
                submit_label="Review selection",
                search_fields=frozenset({field.key}),
                entity_search=self.search_entities,
                multiple_entities=frozenset({"publication_ids"}),
                historical_interval=True if field.key == "publication_ids" else None,
            )
        )
        if values is None:
            return
        if values.get("publication_ids"):
            await self.review_merge(values)
        else:
            await self.create_recipe(BY_KEY["merge new"], values)

    async def search_entities(self, key: str, search: str, values: Values) -> EntityChoices:
        leaf = key.rsplit("--", 1)[-1]
        if leaf == "exchange":
            page = await self.catalogue_call(
                "catalog exchange list", {"search": search, "limit": 50}
            )
            return tuple(
                (f"{value_at(item, 'code')} · {value_at(item, 'name')}", value_at(item, "code"))
                for item in records(page, "exchanges")
            )
        if leaf in {"profile", "format_profile"}:
            from forge.historical.profiles import service as profiles

            prefix = key.rsplit("--", 1)[0] + "--" if "--" in key else ""
            instrument = str(values.get(prefix + "instrument", "")).split(".", 3)
            options: dict[str, str] = {}
            if len(instrument) == 4:
                options = dict(
                    zip(
                        ("exchange", "instrument_type", "market_category"),
                        instrument[:3],
                        strict=True,
                    )
                )
            elif leaf == "format_profile":
                options = {
                    part: str(values["identity--" + part])
                    for part in ("exchange", "instrument_type", "market_category")
                    if values.get("identity--" + part)
                }
            if not options or len(options) != 3:
                raise ValueError("Select the trading venue and instrument classification first.")
            kind = values.get(prefix + "content") or values.get("data_type")
            if kind:
                options["type"] = "depth" if kind == "order_book_l2" else str(kind)
            found = await disk(profiles.discover, self.context.paths, **options)
            return tuple(
                (
                    f"{value_at(item, 'id')} · {value_at(document(definition.get('source')), 'format')}",
                    value_at(item, "id"),
                )
                for item in found
                for definition in (document(item.get("definition")),)
                if search.casefold() in value_at(item, "id").casefold()
            )
        if leaf in {"instrument", "identifier"}:
            params: Values = {"search": search, "limit": 50}
            if leaf == "identifier":
                if values.get(key.removesuffix("identifier") + "kind") != "instrument":
                    raise ValueError(
                        "Select the instrument scope to browse registered instruments."
                    )
                prefix = key.removesuffix("scope--identifier")
                for attribute in ("exchange", "instrument_type", "market_category"):
                    if not values.get(prefix + attribute):
                        raise ValueError(
                            "Select an exchange, instrument type and market category first."
                        )
                    params[attribute] = values[prefix + attribute]
            page = await self.catalogue_call("catalog instrument list", params)
            choices: list[tuple[str, str]] = []
            for item in records(page, "instruments"):
                exchange, kind, market, symbol = (
                    value_at(item, name)
                    for name in ("exchange", "instrument_type", "market_category", "symbol")
                )
                identity = ".".join((exchange, kind, market, symbol))
                choices.append(
                    (
                        f"{exchange} · {symbol} · {kind} / {market} · {identity}",
                        symbol if leaf == "identifier" else identity,
                    )
                )
            return tuple(choices)
        if leaf == "publication_ids" or key == "source":
            if key == "source" and (route := values.get("route")) != "publication":
                files = await disk(list_config_names, library(self.context, str(route)))
                return tuple((name, name) for name in files if search.casefold() in name.casefold())
            with (
                database_environment(testing=self.context.testing),
                pinned_registry(self.database_id),
            ):
                page = await publications(
                    self.context.paths,
                    search=search,
                    filters=DatasetRequest(
                        "list", operation="merge" if key == "source" else "normalize"
                    ),
                )
            choices = []
            for item in records(page, "publications"):
                identity = value_at(item, "registry_publication_id")
                members = records(item, "streams")
                instruments = ", ".join(
                    dict.fromkeys(value_at(stream, "symbol") for stream in members)
                )
                kinds = ", ".join(
                    dict.fromkeys(
                        value_at(stream, "content_type", value_at(stream, "kind"))
                        for stream in members
                    )
                )
                label = (
                    f"#{identity} · Engine {value_at(item, 'engine_dataset_id')} · generation {value_at(item, 'generation')} · "
                    f"{instruments} · {kinds} · {value_at(item, 'integrity')} · "
                    f"{value_at(item, 'first_observed_ns')}–{value_at(item, 'last_observed_ns')} ns"
                )
                choices.append((label, identity))
            return tuple(choices)
        if key == "dataset_ids":
            with (
                database_environment(testing=self.context.testing),
                pinned_registry(self.database_id),
            ):
                page = await browse(
                    self.context.paths,
                    DatasetRequest("list", operation="normalize", status="complete", limit=80),
                )
            choices = []
            for item in records(page, "datasets"):
                identity = value_at(item, "id")
                label = f"#{identity} · {value_at(item, 'primary_file')} · {value_at(item, 'data_type')} · {value_at(item, 'first_observed_utc')}"
                if search.casefold() in label.casefold():
                    choices.append((label, identity))
            return tuple(choices)
        if leaf == "specification":
            instrument = values.get(key.removesuffix("specification") + "instrument")
            if not instrument:
                raise ValueError(
                    "Select the registered instrument before selecting its specification."
                )
            page = await self.catalogue_call("catalog specs show", {"reference": instrument})
            return tuple(
                (
                    f"{value_at(item, 'fingerprint')} · {value_at(item, 'evidence_kind')} · {value_at(item, 'source_observed_at')}",
                    value_at(item, "fingerprint"),
                )
                for item in records(page, "evidence")
                if item.get("fingerprint") and search.casefold() in str(item).casefold()
            )
        if key == "archive_ids":
            page = await self.catalogue_call(
                "storage list", {"search": search, "limit": 50, "status": "complete"}
            )
            return tuple(
                (
                    f"#{value_at(item, 'id')} · {value_at(item, 'exchange')} · {value_at(item, 'scope_identifier')} · {value_at(item, 'data_type')} · {value_at(item, 'status')}",
                    value_at(item, "id"),
                )
                for item in records(page, "archives")
            )
        if key in {"from_normalize", "from_acquisition"}:
            names = await disk(list_config_names, library(self.context, key.removeprefix("from_")))
            return tuple((name, name) for name in names if search.casefold() in name.casefold())
        raise ValueError("This field has no registered source selector.")

    async def author_recipe(
        self,
        family: str,
        row: Row | None = None,
        *,
        initial: dict[str, object] | None = None,
        expected: bytes | None = None,
        add_entry: bool = False,
        name: str = "",
        source_review: str | None = None,
        removal_review: str | None = None,
        verify_conversion_plan: bool = False,
    ) -> None:
        path = row.path if row else None
        original = await disk(path.read_bytes) if path else None
        if expected is not None and original != expected:
            raise ValueError("The recipe changed while selecting sources. Reopen it and try again.")
        from forge.shared.config.recipe_editing import update_recipe

        before = tomllib.loads(original.decode()) if original is not None else {}

        def render(candidate: dict[str, object]) -> str:
            return (
                dumps(candidate) if original is None else update_recipe(original, before, candidate)
            )

        data = initial if initial is not None else before
        model = model_for(family, data)
        if model is None:
            return
        adapter = RecipeForm(model, data, new=path is None)
        fields, preset = adapter.build()
        if add_entry:
            fields, preset = adapter.add(preset)
            if family == "acquisition":
                preset[f"requests--{adapter.count - 1}--scope--kind"] = "instrument"
        if path is None:
            fields = (Field("reference", "Recipe name", required=True, group="Recipe"), *fields)
            if name:
                preset["reference"] = name
        directory = library(self.context, family)
        registry = self.database_id
        testing = self.context.testing
        title = f"{'Edit' if row else 'New'} · {row.reference if row else family.title()}"

        def destination(values: Values) -> Path:
            return path if path else resolve_config_file(directory, str(values["reference"]))

        async def validate(values: Values) -> dict[str, str]:
            candidate, issues = adapter.candidate(values)
            if issues:
                return issues
            try:
                target = destination(values)
                if path is None and await disk(target.exists):
                    return {"reference": "A recipe with this name already exists."}
                if path is not None or family in {
                    "merge",
                    "convert",
                    "acquisition import",
                    "acquisition register",
                }:
                    with TemporaryDirectory(prefix="marketforge-form-") as folder:
                        draft = Path(folder) / target.name
                        await disk(draft.write_text, render(candidate))
                        with database_environment(testing=testing), pinned_registry(registry):
                            await disk(validate_recipe_draft, self.context.paths, family, draft)
                if removal_review is not None and (
                    family == "normalize"
                    and "archive_pins" in candidate
                    or family == "merge"
                    and "publication_ids" in candidate
                ):
                    if registry is None:
                        return {
                            "": "Select the recipe's registry to validate the remaining inputs."
                        }
                    with database_environment(testing=testing), pinned_registry(registry):
                        await data_recipes.validate_sources(
                            self.context.paths, family, candidate, registry
                        )
                if family == "convert" and (removal_review is not None or verify_conversion_plan):
                    from forge.historical.conversion.service import prepare

                    with TemporaryDirectory(prefix="marketforge-conversion-review-") as folder:
                        draft = Path(folder) / target.name
                        await disk(draft.write_text, render(candidate))
                        with database_environment(testing=testing), pinned_registry(registry):
                            result = await prepare(self.context.paths, draft, dry_run=True)
                        blockers = result.get("blockers", []) + result.get(
                            "compatibility_blockers", []
                        )
                        if blockers:
                            return {"": "\n".join(blockers)}
                if source_review is not None:
                    assert registry is not None
                    with database_environment(testing=testing), pinned_registry(registry):
                        await data_recipes.validate_sources(
                            self.context.paths, family, candidate, registry
                        )
            except Exception as error:
                return {"" if path else "reference": str(error)}
            return {}

        def review(values: Values) -> str:
            candidate, _ = adapter.candidate(values)
            stages = "Acquisition → Normalization → Merge"
            if candidate.get("convert") is not None:
                stages += " → Conversion"
            summary = summarize(candidate, "Configuration validated", str(destination(values)))
            summary["Environment"] = (
                f"{self.database_environment} · {self.database_id or 'registry identity unavailable'}"
            )
            if family == "pipeline":
                summary["Stages"] = stages
            summary["Action"] = "Save recipe configuration"
            if removal_review is not None:
                summary["Removing"] = removal_review
                summary["Recipe scope"] = (
                    "Shared workspace configuration for Testing and Production"
                )
            if source_review is not None:
                summary["Reviewed source changes"] = source_review
            heading = "\n".join(f"{key}  {value}" for key, value in summary.items())
            return heading + "\n\nConfigured values\n" + recipe_configuration(candidate).plain

        guide = {
            "acquisition": "acquisition/historical-data/#1-create-a-recipe",
            "normalize": "normalization/#2-create-a-normalization-recipe",
            "merge": "merge/#2-create-and-edit-the-recipe",
            "convert": "conversion/#2-create-and-inspect-a-recipe",
            "pipeline": "pipeline/#recipe",
            "acquisition import": "acquisition/external-data/",
            "acquisition register": "acquisition/external-data/",
            "markets rank": "forge-tui/",
        }[family]
        locked = frozenset(
            field.key
            for field in fields
            if (
                family == "normalize"
                and "archive_pins" in data
                and field.key in {"archive_pins", "registry_id"}
                or family == "merge"
                and data.get("selection_sha256")
                and field.key
                in {
                    "dataset_ids",
                    "publication_ids",
                    "registry_id",
                    "selection_sha256",
                    "file_subset",
                }
                or family == "convert"
                and (
                    field.key == "source"
                    or (
                        document(data.get("source")).get("kind") == "publication"
                        and (
                            field.key.startswith("interval--") or field.key.startswith("selection")
                        )
                    )
                )
            )
        )
        values = await self.push_screen_wait(
            Form(
                Action(title, "Workflows", fields),
                preset,
                root=self.context.paths.root,
                local=True,
                submit_label="Review changes" if row else "Review recipe",
                documentation=partial(self.catalogue_docs, f"{SITE_URL}workflows/{guide}"),
                validate=validate,
                review=review,
                commit_label="Save recipe",
                expand=adapter.add if adapter.can_add else None,
                expand_label="Add Request" if "requests" in model.model_fields else "Add Input",
                initial_focus=f"requests--{adapter.count - 1}--exchange"
                if add_entry
                else "reference",
                locked=locked,
                search_fields={
                    "acquisition": frozenset({"identifier"}),
                    "pipeline": frozenset({"identifier"}),
                    "normalize": frozenset({"identifier"}),
                    "acquisition import": frozenset({"exchange", "format_profile"}),
                    "acquisition register": frozenset({"instrument", "profile", "specification"}),
                    "merge": frozenset({"dataset_ids", "selection--publication_ids"}),
                }.get(family, frozenset()),
                multiple_entities=frozenset({"dataset_ids", "selection--publication_ids"})
                if family == "merge"
                else frozenset(),
                entity_search=self.search_entities,
            )
        )
        if values is None or self.controller.shutdown.emergency:
            return
        if (
            registry is not None and registry != self.database_id
        ) or testing != self.context.testing:
            raise ValueError("The selected environment changed during review.")
        target = destination(values)
        candidate, _ = adapter.candidate(values)
        if source_review is not None:
            assert registry is not None
            with database_environment(testing=testing), pinned_registry(registry):
                await data_recipes.validate_sources(self.context.paths, family, candidate, registry)
        with WorkspaceOwnership(self.context.paths), pinned_registry(registry):
            if original is None:
                await disk(publish_new, target, dumps(candidate))
            elif candidate != before:
                updated = render(candidate)
                with recipe_writer(target):
                    await disk(replace_reviewed, target, original, updated)
        name = target.stem
        if self.area != "Data":
            self.state.selection = name
            self.request_query()
        self.push_screen(
            Inspection(
                f"Recipe saved · {name}"
                if source_review is not None
                else f"{family.title()} · {name}",
                candidate,
                sections=recipe_sections(family, candidate, "Saved", str(target)),
                actions=self.saved_recipe_actions(family, name, target),
            )
        )

    @work(exit_on_error=False)
    async def edit_recipe_form(self, row: Row) -> None:
        if self.recipe_activation or row.path is None:
            return
        self.recipe_activation = True
        try:
            await self.author_recipe(row.kind, row)
        except Exception as error:
            self.push_screen(
                Inspection(
                    "Recipe form unavailable",
                    {"Error": str(error)},
                    actions={"Open TOML editor": partial(self.edit_selected, row)},
                )
            )
        finally:
            self.recipe_activation = False

    @work(exit_on_error=False)
    async def edit_selected(self, row: Row) -> None:
        if row.path is None:
            return
        path = row.path
        paths, testing, registry = self.context.paths, self.context.testing, self.database_id

        async def validate(draft: Path) -> object:
            with database_environment(testing=testing), pinned_registry(registry):
                return await disk(validate_recipe_draft, paths, row.kind, draft)

        async def apply(original: bytes, updated: bytes) -> None:
            self.check_recipe_environment(registry, testing)
            with WorkspaceOwnership(paths), recipe_writer(path):
                await disk(replace_reviewed, path, original, updated.decode())
            if self.area == "Workflows":
                self.request_query()

        self.queue_editor(path, validate, apply, None)

    def render_operation(self) -> None:
        if self.root_screen is None:
            return
        op = self.controller.current
        if op is None:
            return
        if self.area == "Overview":
            if self.controller.active():
                self.paint_overview()
            if not self.controller.active() and op.id != self.overview_completed:
                self.overview_completed = op.id
                self.request_query()
        try:
            widget = self.root_screen.query_one("#operation", Static)
        except NoMatches:
            return
        widget.remove_class("-idle")
        label = f"{op.title} · {op.state}"
        phase = op.snapshot()
        if self.controller.active() and isinstance(phase, PipelineSnapshot):
            current = next(
                (stage for stage in phase.stages if stage.name == phase.active_stage), None
            )
            if current:
                label += f" · {current.name} / {current.phase}"
        elif self.controller.active() and isinstance(phase, ProgressSnapshot):
            label += f" · {phase.requests.finished}/{phase.total_requests} requests"
        elif self.controller.active() and isinstance(phase, str):
            label += f" · {phase}"
        widget.update(label)
        for indicator in self.screen.query("#inspection-operation").results(Static):
            indicator.update(label)
            indicator.display = True
        widget.set_class(op.state == "failed", "failed")
        widget.set_class(op.state in {"complete", "registered"}, "complete")
        widget.set_class(op.state in {"stopped", "blocked", "interrupted"}, "warning")

    def show_operation(
        self, op: Operation, action: Action | None = None, values: Values | None = None
    ) -> None:
        if action is not None and read_only(action, values or {}):
            result = {"error": op.error} if op.error else op.result
            self.push_screen(
                Inspection(op.title, result, actions=self.operation_links(op, action, values))
            )
        else:
            self.push_screen(
                OperationView(
                    op,
                    partial(self.operation_links, op, action, values),
                    self.action_interrupt
                    if self.controller.current is op and self.controller.active()
                    else None,
                    errors=partial(self.inspect_operation_result, op, show_errors=True)
                    if op.title in EXECUTION_ACTIONS or self.owned_run_reference(op)
                    else None,
                )
            )

    def operation_links(
        self, op: Operation, action: Action | None, values: Values | None
    ) -> dict[str, Callable[[], object]]:
        result = document(op.result)
        links: dict[str, Callable[[], object]] = {}
        if (
            result.get("run_id")
            and result.get("kind") == "historical_pipeline"
            and op.state in {"stopped", "blocked", "interrupted", "failed"}
        ):
            identity = str(result["run_id"])
            links["Resume retained pipeline"] = partial(
                self.open_action, BY_KEY["pipeline resume"], None, {"reference": identity}
            )
        if op.title.startswith("pipeline ") and isinstance(
            snapshot := op.snapshot(), PipelineSnapshot
        ):
            links["View run logs"] = partial(
                self.open_action, BY_KEY["runs logs"], None, {"reference": snapshot.run_id}
            )
        if isinstance(snapshot := op.snapshot(), PipelineSnapshot):
            for stage in snapshot.stages:
                links[f"Inspect {stage.name} stage"] = partial(self.inspect_pipeline_stage, stage)
        if result and (action is None or not read_only(action, values or {})):
            links["Inspect result"] = partial(self.inspect_operation_result, op)
        resolutions = records(result, "reference_resolutions")
        if (
            op.title == "convert prepare"
            and resolutions
            and isinstance(source_path := result.get("recipe_path"), str)
        ):
            links["Inspect reference selection"] = lambda: self.push_screen(
                Inspection("Conversion reference selection", {"Selections": resolutions})
            )
            recipe_path = Path(source_path)
            library = self.context.paths.workflow_dir("conversion")
            if (
                recipe_path.parent == library.resolve()
                and recipe_path.is_file()
                and not recipe_path.is_symlink()
                and all(item.get("mode") == "automatic" for item in resolutions)
            ):
                selected = records(resolutions[0], "selected") if len(resolutions) == 1 else []
                links["Switch to manual references"] = partial(
                    self.edit_conversion_options,
                    Row(recipe_path.stem, recipe_path.stem, "Recipe", "", {}, path=recipe_path),
                    manual_draft=tuple(str(item["stream_id"]) for item in selected),
                )
        if result.get("kind") == "normalization_prepare_report":
            if result.get("failed_stage") == "Engine binary resolution":
                links["Build normalization Engine"] = partial(
                    self.open_action, BY_KEY["engine build"], None, {"reference": "normalize"}
                )
            if op.reference and op.state in {"failed", "blocked"}:
                try:
                    path = resolve_config_file(
                        self.context.paths.normalization_recipe_dir, op.reference
                    )
                except ValueError:
                    pass
                else:
                    links["Edit recipe"] = self.saved_recipe_actions("normalize", path.stem, path)[
                        "Edit"
                    ]
            diagnostics = document(result.get("diagnostic_summary"))
            if any(
                group.get("category") == "missing_historical_specifications"
                for group in records(diagnostics, "groups")
            ):
                links["Review historical specification declaration"] = partial(
                    self.open_action, BY_KEY["catalog specs add"]
                )
        if op.error:
            links["Copy diagnostic"] = partial(self.copy_value, op.error)
        if op.run_id:
            links["Open producing run"] = partial(self.open_indexed_run, op.run_id)
            links["Open retained logs"] = partial(
                self.open_action, BY_KEY["runs logs"], None, {"reference": op.run_id}
            )
        if report_run := result.get("logical_run_id") or op.run_id or result.get("run_id"):
            ref = str(report_run)
            links["Open report"] = lambda: self.open_action(
                BY_KEY["runs report"], Row(ref, ref, "Run", "", {}, ref)
            )
        if (
            result.get("execution_receipt")
            and result.get("registration") != "registered"
            and overview.execution_key(result) not in self.overview_recovered
            and (key := op.title.split()[0] + " register") in BY_KEY
        ):
            ref = str(result["execution_receipt"])
            links["Recover registration"] = lambda: self.open_action(
                BY_KEY[key], Row(ref, ref, "Intent", "", {}, ref)
            )
        execution = result.get("execution_id")
        if isinstance(execution, str) and MANAGED_RUN.fullmatch(execution):
            links["Open execution report"] = partial(self.open_managed_run, execution)
            links["View engine logs"] = partial(self.open_overview_logs, execution)
        self.add_registry_links(links, result)
        if (
            action is not None
            and values is not None
            and (op.error or op.state in {"failed", "blocked"})
        ):
            links["Edit request"] = partial(self.open_action, action, None, values)

        return links

    def owned_run_reference(self, operation: Operation) -> str | None:
        result = document(operation.result)
        snapshot = operation.snapshot()
        for candidate in (
            result.get("logical_run_id"),
            operation.run_id,
            result.get("run_id"),
            snapshot.run_id if isinstance(snapshot, PipelineSnapshot) else None,
            result.get("execution_id"),
        ):
            if isinstance(candidate, str):
                try:
                    return safe_reference(candidate)[0]
                except MarketForgeError:
                    continue
        return None

    def inspect_operation_result(self, operation: Operation, *, show_errors: bool = False) -> None:
        if identity := self.owned_run_reference(operation):
            if operation.environment and operation.environment != self.database_environment:
                self.notify(
                    "Select the registry environment that executed this run.", severity="warning"
                )
                return
            if MANAGED_RUN.fullmatch(identity):
                self.open_managed_run(identity, show_errors=show_errors)
            else:
                self.open_indexed_run(identity, show_errors=show_errors)
        elif show_errors:
            self.push_screen(
                Detail("Run errors", "This operation has no retained run identity yet.", raw=True)
            )
        else:
            self.push_screen(Inspection(operation.title, operation.result))

    def inspect_pipeline_stage(self, stage: StageSnapshot) -> None:
        self.push_screen(Inspection(f"Pipeline stage · {stage.name}", asdict(stage)))

    def action_operation(self) -> None:
        if (
            isinstance(self.screen, OperationView)
            and self.screen.operation is self.controller.current
        ):
            return
        if self.controller.current:
            self.show_operation(self.controller.current)
        else:
            self.notify("No owned operations.")

    async def mount_emergency_button(self, screen: Screen[object]) -> None:
        if (
            screen is not self.screen
            or self.emergency_confirmation
            or screen.query("#emergency-stop")
        ):
            return
        screen.add_class("has-emergency-stop")
        await screen.mount(
            Button(
                "STOP ALL",
                id="emergency-stop",
                variant="error",
                compact=True,
                classes="screen-stop",
                action="app.emergency_stop",
            )
        )

    @work(group="emergency-stop", exit_on_error=False)
    async def action_emergency_stop(self) -> None:
        if self.emergency_confirmation or self.controller.shutdown.emergency:
            return
        self.emergency_confirmation = True
        try:
            confirmed = await self.push_screen_wait(
                Confirm(
                    "Emergency Stop",
                    "Stop all processing owned by this MarketForge session?\n\n"
                    f"Active operations: {int(self.controller.active())}\n"
                    f"Observed managed processes: {self.controller.shutdown.process_count()}\n\n"
                    "Processing will be interrupted. Existing committed datasets are kept. "
                    "The MarketForge TUI will exit after shutdown. "
                    "Editable Neovim windows stay open. Their private drafts can be recovered "
                    "from the editor buffer path.",
                    destructive=True,
                    confirm_label="STOP ALL & EXIT",
                    back_label="Cancel",
                )
            )
            if confirmed:
                self.controller.shutdown.emergency = True
                stopped = await self.controller.emergency_stop()
                for worker in self.workers:
                    if worker.group != "emergency-stop":
                        worker.cancel()
                self.exit(
                    message=None
                    if stopped
                    else (
                        "Emergency shutdown could not verify that all owned work stopped. "
                        "Inspect retained runs and logs in the same registry before resuming."
                    )
                )
        finally:
            self.emergency_confirmation = False

    @work(exit_on_error=False)
    async def action_interrupt(self) -> None:
        if self.controller.active() and await self.push_screen_wait(
            Confirm(
                "Interrupt owned operation",
                "Forge will wait for service cleanup and retain the result.",
                destructive=True,
            )
        ):
            await self.controller.cancel()

    @work(exit_on_error=False)
    async def action_quit_forge(self) -> None:
        if self.editor_open:
            self.notify("Close Neovim before quitting Forge.", severity="warning")
            return
        if self.editor_sessions and not await self.push_screen_wait(
            Confirm(
                "Quit with editor drafts?",
                "Editor drafts can be recovered from these paths:\n\n"
                + "\n".join(
                    str(item.draft) for item in self.editor_sessions.values() if item.draft
                ),
                confirm_label="Keep drafts and quit",
            )
        ):
            return
        if any(isinstance(screen, Form) and screen.has_pending() for screen in self.screen_stack):
            if not await self.push_screen_wait(
                Confirm(
                    "Discard form and quit?",
                    "Back keeps the current fields and returns to the workspace.",
                    destructive=True,
                    confirm_label="Discard and quit",
                )
            ):
                return
        if self.query_one(ConfigurationEditor).editing:
            self.notify("Wait for the current configuration action to finish.", severity="warning")
            return
        if not await self.confirm_discard_configs():
            return
        if self.controller.active():
            if not await self.push_screen_wait(
                Confirm(
                    "Cancel operation and quit",
                    "Choose Back to keep Forge open. Confirm to interrupt and wait for cleanup.",
                )
            ):
                return
            await self.controller.cancel()
        self.exit()


def read_source(path: Path) -> str:
    with path.open("rb") as stream:
        return stream.read(256 * 1024).decode("utf-8", errors="replace")


def review_text(values: Values, environment: str = "testing") -> str:
    selected = "\n".join(
        f"{key.replace('_', ' ').capitalize()}  {value}"
        for key, value in values.items()
        if value not in ("", False)
    )
    return f"Environment  {environment.upper()}" + ("\n" + selected if selected else "")


def update_static(widget: Static, value: Text | str) -> None:
    if widget.content != value:
        widget.update(value, layout=False)


def reference_label(entry: Mapping[str, object]) -> str:
    memberships = document(entry.get("reference_memberships"))
    members = [member for file in memberships for member in records(memberships, file)]
    names = tuple(
        dict.fromkeys(
            ".".join(value_at(stream, key) for key in ("exchange", "market_category", "symbol"))
            for stream in members or records(entry, "streams")
        )
    )
    identity = ", ".join(names[:3]) or "trade streams"
    if len(names) > 3:
        identity += f" +{len(names) - 3}"
    return (
        f"Registry {value_at(entry, 'registry_publication_id')} · {identity} · trades · "
        f"ns {value_at(entry, 'first_observed_ns')}–{value_at(entry, 'last_observed_ns')} · "
        f"integrity {value_at(entry, 'integrity', 'recorded')}"
    )
