"""Persistent Textual workspace for the Forge service clients."""

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from functools import partial
from pathlib import Path
from typing import cast

from rich.text import Text
from textual import on, work
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.command import Hit, Hits, Provider
from textual.containers import Horizontal, Vertical, VerticalScroll
from textual.events import Resize
from textual.widgets import Button, DataTable, Footer, Input, Static

from forge.datasets.query import publications, utc
from forge.historical.archives.progress import ProgressSnapshot
from forge.historical.archives.storage.files import disk
from forge.historical.pipelines.progress import PipelineSnapshot
from forge.orchestration.inspection import read_managed_report, run_page
from forge.shared.config.library import list_config_names
from forge.shared.terminal.editor import edit_file
from forge.startup.context import ApplicationContext
from forge.tui.actions import ACTIONS, BY_KEY, Action, Values, read_only
from forge.tui.controller import Controller, Invoke, Operation, document, outcome
from forge.tui.inspection import ActionMenu, Inspection, fields
from forge.tui.operation_view import OperationView
from forge.tui.presentation import Results, Selection, bind, cell_text, columns, membership, visible
from forge.tui.resources import CATALOG, FAMILIES, Row, library, load_rows, records, value_at
from forge.tui.screens import Confirm, Detail, Form
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


@dataclass(slots=True)
class ViewState:
    filter: str = ""
    offset: int = 0
    selection: str | None = None
    rows: dict[str, Row] = field(default_factory=dict[str, Row])
    more: bool = False
    scroll: tuple[float, float] = (0, 0)


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


class ForgeApp(App[None]):
    TITLE = "MarketForge / Forge"
    CSS_PATH = "workspace.tcss"
    COMMANDS = {Commands}
    BINDINGS = [
        Binding("ctrl+p", "command_palette", "Commands", show=False),
        Binding("ctrl+n", "navigation", "Navigation"),
        Binding("/", "filter", "Filter"),
        Binding("ctrl+r", "reload", "Reload"),
        Binding("pageup", "previous", "Prev page", show=False, priority=True),
        Binding("pagedown", "next", "Next page", show=False, priority=True),
        Binding("ctrl+o", "operation", "Run"),
        Binding("f2", "context", "Actions"),
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
                yield Horizontal(id="views")
                yield Input(
                    placeholder="Filter visible results or enter an exact registry symbol",
                    id="filter",
                )
                yield Static("", id="query-state", markup=False)
                with Horizontal(id="resource-body"):
                    yield Results(cursor_type="row", id="results")
                    yield Static("Select a resource", id="inspector")
                with Horizontal(id="context-actions"):
                    yield Button("Enter Inspect", id="inspect-selected", classes="compact")
                    yield Button("F2 Actions", id="context-menu", classes="compact")
                    yield Button("Edit TOML", id="edit-source", classes="compact")
        yield Static("Idle", id="operation", markup=False)
        yield Footer()

    async def on_mount(self) -> None:
        self.root_screen = self.screen
        self.set_interval(0.25, self.render_operation)
        await self.switch(AREAS[0])
        self.query_one("#results", Results).focus()

    def on_resize(self, event: Resize) -> None:
        if self.root_screen is not None:
            self.root_screen.set_class(event.size.width < 93, "narrow")
            self.root_screen.set_class(event.size.width >= 146, "wide")
            if self.screen == self.root_screen and self.state.rows:
                self.call_later(self.render_rows)
            self.root_screen.set_class(event.size.height < 18, "short")

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
        if self.screen == self.root_screen and not isinstance(self.focused, Input):
            self.query_one("#filter", Input).focus()

    def action_reload(self) -> None:
        self.request_query()

    def check_action(self, action: str, parameters: tuple[object, ...]) -> bool | None:
        if action in {"previous", "next"}:
            return self.screen == self.root_screen and not isinstance(self.focused, Input)
        return super().check_action(action, parameters)

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
        self.query_one("#workspace-header", Static).update(
            f"MarketForge / Forge  ·  {self.context.paths.root.name}  ·  TESTING  /  {area}"
        )
        for index, name in enumerate(AREAS):
            self.query_one(f"#area-{index}", Button).set_class(name == area, "active")
        field = self.query_one("#filter", Input)
        field.value = self.state.filter
        field.placeholder = self.search_scope() + " · enter a filter"
        group = self.query_one("#views", Horizontal)
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
        self.render_rows()
        state.update(
            f"{self.search_scope()}  ·  {len(rows)} shown  ·  offset {offset}"
            + ("  ·  PgDn next page" if more else "")
            if rows
            else "No matching records."
        )
        await self.refresh_context()

    def search_scope(self) -> str:
        if self.area == "Runs" and self.view == "Retained":
            return "Retained artifacts"
        if self.area in {"Data", "Catalog"} or self.view in {"Instruments", "Rankings"}:
            return "Registry search"
        return "Loaded resources"

    def render_rows(self) -> None:
        table = self.query_one("#results", Results)
        table.border_title = self.view
        page = self.state
        table.clear(columns=True)
        first = next(iter(page.rows.values()), None)
        if first is None:
            return
        layout = visible(
            columns(self.view, first.name, first.kind, first.state, first.value),
            table.size.width or self.size.width - 4,
        )
        table.add_column("", width=1, key="mark")
        for cell in layout:
            table.add_column(cell.label, width=cell.width, key=cell.label)
        if page.selection not in page.rows:
            page.selection = first.id
        for row in page.rows.values():
            by_name = {
                cell.label: cell
                for cell in columns(self.view, row.name, row.kind, row.state, row.value)
            }
            table.add_row(
                Text("›" if page.selection == row.id else "", style="#7dcfff"),
                *(cell_text(by_name[c.label]) if c.label in by_name else Text("—") for c in layout),
                key=row.id,
            )
        table.move_cursor(row=tuple(page.rows).index(page.selection), scroll=False)
        table.scroll_to(*page.scroll, animate=False, immediate=True)

    @on(Input.Changed, "#filter")
    def filter_changed(self, event: Input.Changed) -> None:
        if self.state.filter != event.value:
            self.state.filter = event.value
            self.state.offset = 0
            self.request_query()

    async def load_rows(
        self, area: str, view: str, search: str, offset: int
    ) -> tuple[list[Row], bool]:
        return await load_rows(self.controller, area, view, search, offset)

    def related(self, row: Row | None) -> tuple[str, ...]:
        if self.area == "Overview":
            return ("doctor", "pipeline run", "runs list")
        if self.area == "Data":
            if self.view == "Publications":
                return ()
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
        row = self.state.rows.get(self.state.selection or "")
        self.query_one("#inspect-selected", Button).disabled = row is None
        self.query_one("#edit-source", Button).display = bool(row and row.path)
        self.query_one("#context-menu", Button).disabled = not bool(self.related(row))
        inspector = self.query_one("#inspector", Static)
        inspector.border_title = row.name if row else "Selection"
        inspector.update(fields(row.value) if row else Text("Select a resource"))

    @on(DataTable.RowHighlighted, "#results")
    async def highlighted(self, event: DataTable.RowHighlighted) -> None:
        if event.row_key.value in self.state.rows:
            old = self.state.selection
            self.state.selection = event.row_key.value
            self.query_one("#results", Results).mark(old, self.state.selection)
            await self.refresh_context()

    @on(DataTable.RowSelected, "#results")
    async def selected(self, event: DataTable.RowSelected) -> None:
        row = self.state.rows.get(event.row_key.value or "")
        if row is not None:
            await self.inspect_row(row)

    async def inspect_row(self, row: Row) -> None:
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
            await self.inspect_publication(int(row.id))
            return
        if row.path:
            try:
                detail = {**document(row.value), "source_toml": await disk(read_source, row.path)}
            except OSError as error:
                detail = {"error": str(error), "path": str(row.path)}
        selected = self.selection(row)
        if selected.kind == "file":
            actions.update(self.file_links(int(selected.identity)))
        self.push_screen(Inspection(row.name, detail, actions=actions))

    async def inspect_publication(
        self, identity: int, offset: int = 0, *, section: str = "Summary"
    ) -> None:
        try:
            value = await publications(self.context.paths, identity=identity, file_offset=offset)
            matches = records(value, "publications")
            if not matches:
                raise ValueError("The retained publication is unavailable.")
            publication = matches[0]
        except Exception as error:
            self.push_screen(Inspection("Publication evidence", {"error": str(error)}))
            return
        engine = value_at(publication, "engine_dataset_id")

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
            "Copy registry publication ID": partial(self.copy_value, str(identity)),
            "Copy engine publication ID": partial(self.copy_value, engine),
        }
        if publication.get("primary_files_truncated"):
            links["Next file page"] = partial(self.publication_page, identity, offset + 50)
        if offset:
            links["Previous file page"] = partial(
                self.publication_page, identity, max(0, offset - 50)
            )
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
        summary["Instruments"] = membership(streams, "symbol")
        summary["Stream count"] = len(streams) if publication.get("streams") is not None else None
        requested = document(publication.get("requested_interval"))
        for key, label in (
            ("start_ns", "Requested start UTC"),
            ("end_ns", "Requested end UTC (exclusive)"),
        ):
            bound = requested.get(key)
            summary[label] = utc(bound) if isinstance(bound, int) else bound
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
                    "Streams": publication.get("streams"),
                    "Files": files,
                    "Evidence": {
                        "manifest_path": publication.get("manifest_path"),
                        "manifest_sha256": publication.get("manifest_sha256"),
                    },
                },
                choose=choose,
                section=section,
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

    def file_links(self, identity: int) -> dict[str, Callable[[], object]]:
        selection = Row(str(identity), str(identity), "File", "", {}, str(identity))
        return {
            label: partial(self.open_action, BY_KEY[key], selection)
            for label, key in (
                ("File schema", "datasets schema"),
                ("Bounded file sample", "datasets head"),
                ("File lineage", "datasets lineage"),
                ("Verify primary file", "datasets verify"),
            )
        }

    async def inspect_request(
        self,
        key: str,
        values: Values,
        label: str,
        links: dict[str, Callable[[], object]] | None = None,
    ) -> None:
        try:
            result = await self.controller.invoke(
                key, values, self.context, lambda _: None, self.edit_recipe
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
            "storage show", {"archive": identity}, f"Local archive {identity}"
        )

    @work(exit_on_error=False)
    async def open_managed_run(self, identity: str) -> None:
        try:
            item = await disk(read_managed_report, self.context.paths.runs_dir, identity)
            self.push_screen(Inspection(identity, item))
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
        elif key == "context-menu":
            self.action_context()
        elif key == "inspect-selected":
            row = self.state.rows.get(self.state.selection or "")
            if row:
                await self.inspect_row(row)
        elif key == "edit-source":
            row = self.state.rows.get(self.state.selection or "")
            if row and row.path:
                self.edit_selected(row)

    def action_context(self) -> None:
        if self.screen != self.root_screen:
            return
        row = self.state.rows.get(self.state.selection or "")
        keys = self.related(row)
        if keys:

            def choose(key: str | None) -> None:
                if key is not None:
                    self.open_action(BY_KEY[key], row)

            self.push_screen(ActionMenu(row.name if row else self.view, keys), choose)

    def selection(self, row: Row) -> Selection:
        item = document(row.value)
        if row.kind == "Publication":
            return Selection("publication", row.id)
        if row.reference.isdecimal() and (row.kind == "File" or self.view == "Primary files"):
            return Selection("file", row.reference)
        if row.reference.isdecimal() and self.view == "Raw storage":
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
        if action.fields:
            values = await self.push_screen_wait(
                Form(action, values_from_context, choices, self.context.paths.root)
            )
            if values is None:
                return
        else:
            values = {}
        if action.effect in {"engine", "network", "write"} and not read_only(action, values):
            if not await self.push_screen_wait(
                Confirm(action.key, await self.review_scope(action, values))
            ):
                return
        await self.start_action(action, values)

    async def review_scope(self, action: Action, values: Values) -> str:
        text = review_text(values)
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
        elif read_only(action, values) and self.screen == self.root_screen:
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
            self.push_screen(OperationView(op, partial(self.operation_links, op, action, values)))

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
        if result.get("run_id"):
            ref = str(result["run_id"])
            links["Open report"] = lambda: self.open_action(
                BY_KEY["runs report"], Row(ref, ref, "Run", "", {}, ref)
            )
        if (
            result.get("execution_receipt")
            and result.get("registration") != "registered"
            and (key := op.title.split()[0] + " register") in BY_KEY
        ):
            ref = str(result["execution_receipt"])
            links["Recover registration"] = lambda: self.open_action(
                BY_KEY[key], Row(ref, ref, "Intent", "", {}, ref)
            )
            links["Run logs"] = partial(
                self.open_action, BY_KEY["runs logs"], None, {"reference": ref}
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

        return links

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


def review_text(values: Values) -> str:
    selected = "\n".join(
        f"{key.replace('_', ' ').capitalize()}  {value}"
        for key, value in values.items()
        if value not in ("", False)
    )
    return "Environment  Testing" + ("\n" + selected if selected else "")
