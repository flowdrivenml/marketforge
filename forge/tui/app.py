"""Persistent Textual workspace for the Forge service clients."""

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from functools import partial
from pathlib import Path
from typing import cast

from textual import on, work
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.command import Hit, Hits, Provider
from textual.containers import Horizontal, Vertical, VerticalScroll
from textual.events import Resize
from textual.widgets import Button, DataTable, Footer, Input, Static

from forge.datasets.query import publications
from forge.historical.archives.progress import ProgressSnapshot
from forge.historical.archives.storage.files import disk
from forge.historical.conversion.recipes import library as conversion_library
from forge.historical.pipelines.progress import PipelineSnapshot
from forge.historical.profiles.service import discover
from forge.orchestration.inspection import list_runs, read_managed_report
from forge.shared.config.library import list_config_names
from forge.shared.terminal.editor import edit_file
from forge.startup.context import ApplicationContext
from forge.tui.actions import ACTIONS, BY_KEY, Action, Values, read_only
from forge.tui.controller import Controller, Invoke, Operation, document, outcome
from forge.tui.screens import Confirm, Detail, Form, LiveDetail, RunDetail
from forge.tui.services import apply_cleanup, execute, ranks

AREAS = (
    "Overview",
    "Workflows",
    "Data",
    "Markets",
    "Catalog",
    "Recipes & Profiles",
    "Runs",
    "System",
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
    "Data": ("Publications", "Primary files", "Raw storage"),
    "Markets": ("Instruments", "Rankings", "Policies"),
    "Catalog": ("Exchanges", "Instruments", "Specifications"),
    "Recipes & Profiles": ("Recipes", "Profiles"),
    "Runs": ("Retained", "Owned"),
    "System": ("Engines", "Database", "Settings"),
}
FAMILIES = {
    "Pipeline": "pipeline",
    "Acquisition": "acquisition",
    "Normalization": "normalize",
    "Merge": "merge",
    "Conversion": "convert",
    "Import": "acquisition import",
    "Registration": "acquisition register",
    "Policies": "markets rank",
}
CATALOG = {"Exchanges": "exchange", "Instruments": "instrument", "Specifications": "specs"}


@dataclass(frozen=True, slots=True)
class Row:
    id: str
    name: str
    kind: str
    state: str
    value: object
    reference: str = ""
    path: Path | None = None


@dataclass(slots=True)
class ViewState:
    filter: str = ""
    offset: int = 0
    selection: str | None = None
    rows: dict[str, Row] = field(default_factory=dict[str, Row])
    more: bool = False
    scroll: tuple[float, float] = (0, 0)


def records(value: object, key: str) -> list[Mapping[str, object]]:
    items = document(value).get(key)
    return (
        [document(item) for item in cast(list[object], items) if document(item)]
        if isinstance(items, list)
        else []
    )


def value_at(item: Mapping[str, object], key: str, default: str = "—") -> str:
    value = item.get(key)
    return str(value) if value is not None else default


class Commands(Provider):
    async def search(self, query: str) -> Hits:
        app = cast(ForgeApp, self.app)
        matcher = self.matcher(query)
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


class Results(DataTable[str]):
    """Table with stable textual cell values."""


class ForgeApp(App[None]):
    TITLE = "MarketForge / Forge"
    CSS_PATH = "workspace.tcss"
    COMMANDS = {Commands}
    BINDINGS = [
        Binding("ctrl+p", "command_palette", "Commands", show=False),
        Binding("ctrl+n", "navigation", "Navigation"),
        Binding("/", "filter", "Filter"),
        Binding("ctrl+r", "reload", "Reload"),
        Binding("pageup", "previous", "Prev page", show=False),
        Binding("pagedown", "next", "Next page", show=False),
        Binding("ctrl+o", "operation", "Run details"),
        Binding("ctrl+c", "interrupt", "Interrupt", priority=True),
        Binding("ctrl+q", "quit_forge", "Quit", priority=True),
    ]

    def __init__(self, context: ApplicationContext, *, invoke: Invoke = execute) -> None:
        super().__init__()
        self.context = context
        self.controller = Controller(context, self.edit_recipe, invoke)
        self.area = AREAS[0]
        self.view = VIEWS[self.area][0]
        self.states: dict[tuple[str, str], ViewState] = {}
        self.version = 0
        self.root_screen = self.screen_stack[0] if self.screen_stack else None
        self.editor_open = False

    @property
    def state(self) -> ViewState:
        return self.states.setdefault((self.area, self.view), ViewState())

    def compose(self) -> ComposeResult:
        yield Static(
            f"MarketForge / Forge    {self.context.paths.root.name}    TESTING",
            id="workspace-header",
            markup=False,
        )
        with Horizontal(id="workspace-body"):
            with VerticalScroll(id="navigation"):
                for index, area in enumerate(AREAS):
                    yield Button(area, id=f"area-{index}")
            with Vertical(id="main"):
                yield Static("", id="section-title", markup=False)
                yield Horizontal(id="views")
                yield Input(
                    placeholder="Filter visible results or enter an exact registry symbol",
                    id="filter",
                )
                yield Static("", id="query-state", markup=False)
                yield Results(cursor_type="row", id="results")
                yield Horizontal(id="context-actions")
        yield Static("Idle", id="operation", markup=False)
        yield Footer()

    async def on_mount(self) -> None:
        self.root_screen = self.screen
        self.query_one("#results", Results).add_columns("Identity", "Kind", "State", "Reference")
        self.set_interval(0.25, self.render_operation)
        await self.switch(AREAS[0])
        self.query_one("#results", Results).focus()

    def on_resize(self, event: Resize) -> None:
        if self.root_screen is not None:
            self.root_screen.set_class(event.size.width < 83, "narrow")
            self.root_screen.set_class(event.size.height < 18, "short")

    def action_navigation(self) -> None:
        if self.root_screen is not None:
            if self.size.width < 83:
                self.root_screen.toggle_class("nav-open")
                visible = self.root_screen.has_class("nav-open")
            else:
                self.root_screen.toggle_class("nav-hidden")
                visible = not self.root_screen.has_class("nav-hidden")
            if visible:
                self.root_screen.query_one("#area-0", Button).focus()

    def action_filter(self) -> None:
        if self.screen == self.root_screen and not isinstance(self.focused, Input):
            self.query_one("#filter", Input).focus()

    def action_reload(self) -> None:
        self.request_query()

    def action_next(self) -> None:
        if self.screen == self.root_screen and self.state.more:
            self.state.offset += 50
            self.request_query()

    def action_previous(self) -> None:
        if self.screen == self.root_screen and self.state.offset:
            self.state.offset = max(0, self.state.offset - 50)
            self.request_query()

    async def switch(self, area: str, view: str | None = None) -> None:
        results = self.query_one("#results", Results)
        self.state.scroll = (results.scroll_x, results.scroll_y)
        self.area, self.view = area, view or VIEWS[area][0]
        self.query_one("#section-title", Static).update(
            f"{area} / {self.view}" if len(VIEWS[area]) > 1 else area
        )
        field = self.query_one("#filter", Input)
        field.value = self.state.filter
        group = self.query_one("#views", Horizontal)
        await group.remove_children()
        await group.mount(
            *(
                Button(
                    label,
                    id=f"view-{index}",
                    variant="primary" if label == self.view else "default",
                )
                for index, label in enumerate(VIEWS[area])
            )
        )
        self.request_query()

    def request_query(self) -> None:
        self.version += 1
        self.query_view(self.version, self.area, self.view, self.state.filter, self.state.offset)

    @work(exclusive=True, group="query", exit_on_error=False)
    async def query_view(
        self, version: int, area: str, view: str, search: str, offset: int
    ) -> None:
        state = self.query_one("#query-state", Static)
        state.remove_class("-error")
        state.update("Loading retained evidence…")
        try:
            await asyncio.sleep(0.16)
            rows, more = await self.load_rows(area, view, search, offset)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            if version == self.version:
                self.state.rows = {}
                self.query_one("#results", Results).clear()
                state.add_class("-error")
                state.update(f"Query failed · {error}")
                await self.refresh_context()
            return
        if version != self.version:
            return
        page = self.state
        page.rows = {row.id: row for row in rows}
        page.more = more
        table = self.query_one("#results", Results)
        table.clear()
        for row in rows:
            table.add_row(row.name, row.kind, row.state, row.reference, key=row.id)
        if page.selection in page.rows:
            table.move_cursor(row=tuple(page.rows).index(page.selection), scroll=False)
            table.scroll_to(*page.scroll, animate=False, immediate=True)
        state.update(
            f"{len(rows)} shown · offset {offset}" + (" · PgDn next page" if more else "")
            if rows
            else "No matching records."
        )
        await self.refresh_context()

    @on(Input.Changed, "#filter")
    def filter_changed(self, event: Input.Changed) -> None:
        if self.state.filter != event.value:
            self.state.filter = event.value
            self.state.offset = 0
            self.request_query()

    async def load_rows(
        self, area: str, view: str, search: str, offset: int
    ) -> tuple[list[Row], bool]:
        context = self.context
        paths = context.paths
        normalized = search.casefold().strip()
        rows: list[Row] = []
        more = False

        async def inspect(key: str, values: Values) -> object:
            return await self.controller.invoke(
                key, values, context, lambda _: None, self.edit_recipe
            )

        if area == "Overview":
            rows = [
                Row(
                    "project",
                    paths.root.name,
                    "Workspace",
                    "Local",
                    {
                        "workspace": str(paths.root),
                        "storage": str(paths.data_root),
                        "registry": "Explicit testing route",
                    },
                ),
                Row(
                    "doctor",
                    "Check prerequisites",
                    "Diagnostics",
                    "Unchecked",
                    {"action": "doctor"},
                ),
            ]
            for index, item in enumerate(await disk(list_runs, paths.runs_dir, 8)):
                run_id = value_at(item, "run_id", str(index))
                state = value_at(item, "state", value_at(item, "status"))
                rows.append(
                    Row(
                        run_id,
                        value_at(item, "name", run_id[:8]),
                        "Retained run",
                        "Last recorded " + state,
                        item,
                        run_id,
                    )
                )
        elif (family := FAMILIES.get(view)) is not None and area in {"Workflows", "Markets"}:
            directory = self.library(family)
            rows = [
                Row(
                    name,
                    name,
                    family,
                    "Saved",
                    {"path": str(directory / f"{name}.toml")},
                    name,
                    directory / f"{name}.toml",
                )
                for name in await disk(list_config_names, directory)
            ]
        elif area == "Recipes & Profiles":
            if view == "Profiles":
                for item in await disk(discover, paths):
                    row = str(item["id"])
                    rows.append(
                        Row(row, row, "Profile", "Installed", item, row, Path(item["location"]))
                    )
            else:
                for family in FAMILIES.values():
                    directory = self.library(family)
                    for name in await disk(list_config_names, directory):
                        path = directory / f"{name}.toml"
                        rows.append(
                            Row(
                                f"{family}:{name}",
                                name,
                                family,
                                "Saved",
                                {"path": str(path)},
                                name,
                                path,
                            )
                        )
        elif area == "Data":
            if view == "Raw storage":
                result = await inspect(
                    "storage list",
                    {"limit": 50, "offset": offset, **({"match": search} if search else {})},
                )
                for item in records(result, "archives"):
                    ident = value_at(item, "id", value_at(item, "local_id"))
                    rows.append(
                        Row(
                            ident,
                            value_at(item, "filename", ident),
                            value_at(item, "data_type"),
                            value_at(item, "status"),
                            item,
                            ident,
                        )
                    )
                more = len(rows) == 50
            elif view == "Publications":
                result = await publications(paths, instrument=search or None, offset=offset)
                for item in records(result, "publications"):
                    ident = value_at(item, "registry_publication_id")
                    rows.append(
                        Row(
                            ident,
                            value_at(item, "engine_dataset_id"),
                            "Publication",
                            value_at(item, "operation"),
                            item,
                            value_at(item, "representative_file_id"),
                        )
                    )
                more = bool(result["has_more"])
            else:
                result = await inspect(
                    "datasets list",
                    {"limit": 50, "offset": offset, **({"instrument": search} if search else {})},
                )
                for item in records(result, "datasets"):
                    ident = value_at(item, "id")
                    rows.append(
                        Row(
                            ident,
                            ident,
                            value_at(item, "data_type"),
                            value_at(item, "status"),
                            item,
                            ident,
                        )
                    )
                more = bool(document(result).get("has_more"))
            return rows, more
        elif area == "Markets":
            key = "markets rank list" if view == "Rankings" else "markets list"
            payload: Values = {"limit": 50}
            if search and view == "Instruments":
                payload["match"] = search
            result = await inspect(key, payload)
            for index, item in enumerate(
                records(result, "rankings" if view == "Rankings" else "instruments")
            ):
                ident = value_at(item, "instrument_reference", value_at(item, "id", str(index)))
                rows.append(
                    Row(
                        ident,
                        value_at(item, "symbol", value_at(item, "policy_name", ident)),
                        view,
                        value_at(item, "status"),
                        item,
                        ident,
                    )
                )
        elif area == "Catalog":
            entity = CATALOG[view]
            payload: Values = {"limit": 51, "offset": offset, "search": search}
            result = document(await inspect(f"catalog {entity} list", payload))
            for key in result:
                entries = records(result, key)
                more |= len(entries) > 50
                for index, item in enumerate(entries[:50]):
                    ident = value_at(item, "id", str(index))
                    reference = value_at(item, "code", value_at(item, "identity_key", "")).replace(
                        "/", "."
                    )
                    if not reference:
                        reference = ".".join(
                            value_at(item, field)
                            for field in (
                                "exchange",
                                "instrument_type",
                                "market_category",
                                "symbol",
                            )
                        )
                    rows.append(
                        Row(
                            f"{key}:{ident}",
                            value_at(item, "symbol", reference),
                            entity,
                            value_at(item, "status"),
                            item,
                            reference,
                        )
                    )
            return rows, more
        elif area == "Runs":
            if view == "Owned":
                for op in [
                    *self.controller.history,
                    *([self.controller.current] if self.controller.active() else []),
                ]:
                    if op is not None:
                        rows.append(Row(op.id, op.title, "Owned", op.state, op, op.id))
            else:
                for index, item in enumerate(await disk(list_runs, paths.runs_dir, 50)):
                    ident = value_at(item, "run_id", str(index))
                    rows.append(
                        Row(
                            ident,
                            value_at(item, "name", ident[:8]),
                            value_at(item, "kind", "Retained"),
                            "Last recorded " + value_at(item, "state", value_at(item, "status")),
                            item,
                            ident,
                        )
                    )
        elif area == "System":
            key = {
                "Engines": "engine status",
                "Database": "database status",
                "Settings": "config show",
            }[view]
            if view == "Engines":
                for item in records(await inspect(key, {}), "targets"):
                    ident = value_at(item, "target")
                    rows.append(
                        Row(ident, ident, "Managed engine", value_at(item, "status"), item, ident)
                    )
            else:
                result = await inspect(key, {})
                for name, item in document(result).items():
                    rows.append(Row(name, name, view, "Retained", item))
        return [
            row for row in rows if normalized in f"{row.name} {row.kind} {row.reference}".casefold()
        ][:100], more

    def library(self, family: str) -> Path:
        paths = self.context.paths
        return {
            "pipeline": paths.pipeline_config_dir,
            "acquisition": paths.acquisition_config_dir,
            "normalize": paths.normalization_recipe_dir,
            "merge": paths.config_dir / "historical/merge",
            "convert": conversion_library(paths),
            "acquisition import": paths.acquisition_config_dir / "import",
            "acquisition register": paths.config_dir / "historical/registration",
            "markets rank": paths.config_dir / "markets/rankings",
        }[family]

    def related(self, row: Row | None) -> tuple[str, ...]:
        if self.area == "Overview":
            return ("doctor", "pipeline run", "runs list")
        if self.area == "Data":
            return (
                ("storage show", "storage verify", "storage coverage", "storage delete")
                if self.view == "Raw storage"
                else (
                    "datasets show",
                    "datasets streams",
                    "datasets lineage",
                    "datasets schema",
                    "datasets head",
                    "datasets verify",
                    "datasets coverage",
                )
            )
        if self.area == "Runs":
            if row is not None and isinstance(row.value, Operation):
                return ()
            if row is not None and document(row.value).get("kind") == "historical_pipeline":
                return ("runs report", "runs logs", "pipeline resume")
            return ("runs report", "runs logs")
        if self.area == "Markets" and self.view != "Policies":
            return (
                ("markets rank show", "markets rank update", "markets rank delete")
                if self.view == "Rankings"
                else ("markets show", "markets list", "markets refresh", "markets sync")
            )
        if self.area == "Catalog":
            return tuple(f"catalog {CATALOG[self.view]} {x}" for x in ("show", "validate", "add"))
        if self.area == "System":
            return (
                ("engine build", "doctor")
                if self.view == "Engines"
                else ("database status", "database migrate", "database backup", "database restore")
                if self.view == "Database"
                else ("config show",)
            )
        if self.view == "Profiles":
            return tuple(
                f"acquisition profiles {x}" for x in ("inspect", "validate", "sample", "register")
            )
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

    async def refresh_context(self) -> None:
        group = self.query_one("#context-actions", Horizontal)
        await group.remove_children()
        row = self.state.rows.get(self.state.selection or "")
        keys = self.related(row)
        await group.mount(
            *(
                Button(key.rsplit(" ", 1)[-1].title(), id=f"context-{index}")
                for index, key in enumerate(keys)
            ),
            *([Button("Edit TOML", id="edit-source")] if row and row.path else []),
        )

    @on(DataTable.RowHighlighted, "#results")
    async def highlighted(self, event: DataTable.RowHighlighted) -> None:
        if event.row_key.value in self.state.rows:
            self.state.selection = event.row_key.value
            await self.refresh_context()

    @on(DataTable.RowSelected, "#results")
    async def selected(self, event: DataTable.RowSelected) -> None:
        row = self.state.rows.get(event.row_key.value or "")
        if row is None:
            return
        if isinstance(row.value, Operation):
            self.show_operation(row.value)
            return
        if row.id == "doctor" and self.area == "Overview":
            self.open_action(BY_KEY["doctor"])
            return
        actions: dict[str, Callable[[], object]] = {
            "Copy ID": lambda: self.copy_value(row.reference or row.id)
        }
        if row.path:
            actions["Copy path"] = lambda: self.copy_value(str(row.path))
        for key in self.related(row):
            actions[key.rsplit(" ", 1)[-1].title()] = lambda key=key: self.open_action(
                BY_KEY[key], row
            )
        detail = row.value
        if row.kind == "Publication":
            actions.pop("Copy ID", None)
            actions["Copy registry publication ID"] = partial(self.copy_value, row.id)
            actions["Copy engine publication ID"] = partial(self.copy_value, row.name)
            try:
                publication = await publications(self.context.paths, identity=int(row.id))
                rows = records(publication, "publications")
                detail = rows[0] if rows else {"state": "unavailable", "retained": row.value}
                actions["Browse primary files"] = partial(self.open_publication_files, row.name)
                execution = document(document(detail).get("execution"))
                if execution.get("execution_id"):
                    actions["Related execution"] = partial(
                        self.open_managed_run, str(execution["execution_id"])
                    )
            except Exception as error:
                detail = {"error": str(error), "retained": row.value}
        if row.path:
            try:
                detail = {**document(row.value), "source_toml": await disk(read_source, row.path)}
            except OSError as error:
                detail = {"error": str(error), "path": str(row.path)}
        screen = RunDetail if self.area == "Runs" else Detail
        self.push_screen(screen(row.name, detail, actions=actions))

    @work(exit_on_error=False)
    async def open_publication_files(self, engine_id: str) -> None:
        values: Values = {"publication": engine_id, "limit": 50, "offset": 0}
        await self.start_action(BY_KEY["datasets list"], values)

    @work(exit_on_error=False)
    async def open_managed_run(self, identity: str) -> None:
        try:
            item = await disk(read_managed_report, self.context.paths.runs_dir, identity)
            self.push_screen(Detail(identity, item))
        except Exception as error:
            self.push_screen(Detail(identity, str(error), raw=True))

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
        elif key.startswith("context-"):
            row = self.state.rows.get(self.state.selection or "")
            keys = self.related(row)
            index = int(key.split("-")[1])
            if index < len(keys):
                self.open_action(BY_KEY[keys[index]], row)
        elif key == "edit-source":
            row = self.state.rows.get(self.state.selection or "")
            if row and row.path:
                self.edit_selected(row)

    def preset(self, action: Action, row: Row | None) -> Values:
        if row is None or not action.fields:
            return {}
        name = action.fields[0].key
        if name == "id" and row.reference.isdecimal():
            return {"id": int(row.reference)}
        if name in {"reference", "archive"}:
            return {name: row.reference}
        return {}

    @work(exit_on_error=False)
    async def open_action(
        self, action: Action, row: Row | None = None, preset: Values | None = None
    ) -> None:
        choices: tuple[str, ...] = ()
        family = action.key.rsplit(" ", 1)[0]
        if family in FAMILIES.values() and not action.key.endswith((" new", " register")):
            choices = await disk(list_config_names, self.library(family))
        if action.fields:
            values = await self.push_screen_wait(
                Form(action, preset or self.preset(action, row), choices, self.context.paths.root)
            )
            if values is None:
                return
        else:
            values = {}
        if action.effect in {"engine", "network", "write"} and not read_only(action, values):
            if not await self.push_screen_wait(Confirm(action.key, action.command(values))):
                return
        await self.start_action(action, values)

    async def start_action(self, action: Action, values: Values) -> None:
        if action.effect == "editor":
            try:
                result = await self.controller.invoke(
                    action.key, values, self.context, lambda _: None, self.edit_recipe
                )
                self.push_screen(Detail(action.key, result))
                self.request_query()
            except Exception as error:
                self.push_screen(Detail(action.key, str(error), raw=True))
            return
        if action.key == "normalize prepare" and values.get("from_acquisition"):
            try:
                await self.controller.invoke(
                    "normalize new", values, self.context, lambda _: None, self.edit_recipe
                )
            except Exception as error:
                self.push_screen(Detail(action.key, str(error), raw=True))
                return
            # Editing has returned before preparation acquires workspace ownership.
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
            self.show_operation(op, action, values)
            return
        try:
            op = await asyncio.shield(self.controller.launch(action, values))
        except ValueError as error:
            self.notify(str(error), severity="warning")
            return
        if action.effect == "guarded" and op.error is None:
            await self.review_action(action, values, op)
        elif self.screen == self.root_screen:
            self.show_operation(op, action, values)
        else:
            self.notify(f"{op.title} · {op.state}. Open owned runs for details.")

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
            Detail(
                "Review exact selection",
                op.result,
                actions={"Apply reviewed selection": apply} if apply else None,
            )
        )

    @work(exit_on_error=False)
    async def apply_review(
        self, action: Action, values: Values, callback: Callable[[], Awaitable[object]]
    ) -> None:
        if not await self.push_screen_wait(Confirm("Apply reviewed selection", action.key)):
            return
        op = await asyncio.shield(self.controller.launch(action, values, callback))
        self.show_operation(op)
        self.request_query()

    async def edit_recipe(self, path: Path) -> None:
        if self.editor_open:
            raise ValueError("An editor is already open.")
        self.editor_open = True
        try:
            with self.suspend():
                await disk(
                    edit_file, path, editor=self.context.settings.presentation.editor or "nvim"
                )
        finally:
            self.editor_open = False

    @work(exit_on_error=False)
    async def edit_selected(self, row: Row) -> None:
        assert row.path is not None
        try:
            await self.edit_recipe(row.path)
            key = (
                "acquisition profiles validate" if row.kind == "Profile" else f"{row.kind} validate"
            )
            if key in BY_KEY:
                result = await self.controller.invoke(
                    key,
                    {"reference": row.reference},
                    self.context,
                    lambda _: None,
                    self.edit_recipe,
                )
            else:
                result = await disk(read_source, row.path)
            self.push_screen(Detail("Saved source", result))
        except Exception as error:
            self.push_screen(Detail("Editor or validation error", str(error), raw=True))
        self.request_query()

    def render_operation(self) -> None:
        if self.root_screen is None:
            return
        op = self.controller.current
        if op is None:
            return
        widget = self.root_screen.query_one("#operation", Static)
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

    def show_operation(
        self, op: Operation, action: Action | None = None, values: Values | None = None
    ) -> None:
        result = document(op.result)
        links: dict[str, Callable[[], object]] = {}
        if result.get("run_id"):
            ref = str(result["run_id"])
            links["Open report"] = lambda: self.open_action(
                BY_KEY["runs report"], Row(ref, ref, "Run", "", {}, ref)
            )
        if result.get("execution_receipt") and (key := op.title.split()[0] + " register") in BY_KEY:
            ref = str(result["execution_receipt"])
            links["Recover registration"] = lambda: self.open_action(
                BY_KEY[key], Row(ref, ref, "Intent", "", {}, ref)
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
        if (
            action is not None
            and values is not None
            and (op.error or op.state in {"failed", "blocked"})
        ):
            links["Edit request"] = partial(self.open_action, action, None, values)

        def display() -> object:
            from dataclasses import asdict

            phase = op.snapshot()
            if isinstance(phase, (PipelineSnapshot, ProgressSnapshot)):
                phase = asdict(phase)
            return {"state": op.state, "activity": phase, "error": op.error, "result": op.result}

        if op.state in {"running", "interrupting"}:
            self.push_screen(LiveDetail(op.title, display))
        else:
            detail = RunDetail if result.get("run_id") or result.get("execution_id") else Detail
            self.push_screen(detail(op.title, display(), actions=links))

    def action_operation(self) -> None:
        if self.controller.current:
            self.show_operation(self.controller.current)
        else:
            self.notify("No owned operations.")

    @work(exit_on_error=False)
    async def action_interrupt(self) -> None:
        if self.controller.active() and await self.push_screen_wait(
            Confirm(
                "Interrupt owned operation",
                "Forge will wait for service cleanup and retain the result.",
            )
        ):
            await self.controller.cancel()

    @work(exit_on_error=False)
    async def action_quit_forge(self) -> None:
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
