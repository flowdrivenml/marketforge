"""Textual configuration explorer, using the existing application editor and clipboard."""

import difflib
import os
import webbrowser
from collections.abc import Callable
from copy import copy as copy_draft
from dataclasses import asdict, dataclass
from functools import partial
from pathlib import Path
from typing import Any, cast
from urllib.parse import urlsplit

from rich.text import Text
from textual import on, work
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical, VerticalScroll
from textual.events import Click
from textual.message import Message
from textual.screen import ModalScreen
from textual.widgets import Button, DataTable, Input, Select, Static, Switch
from textual.worker import Worker

from forge.historical.archives.storage.files import disk
from forge.historical.engine.pipeline_runtime import PipelineChange
from forge.historical.engine.runtime_config import UNITS, Change
from forge.shared.config.models import AppSettings
from forge.shared.database.status import DatabaseInspectionError, inspect_database
from forge.shared.documentation import SITE_URL
from forge.shared.terminal.palette import BLUE, CYAN, GREEN, MUTED, RED, YELLOW
from forge.startup.context import ApplicationContext
from forge.tui.actions import Action, Field
from forge.tui.clipboard import plain
from forge.tui.configuration import (
    BASIC,
    EDITOR_KINDS,
    PAGES,
    ApplicationChange,
    ConfigurationDraft,
    documentation,
    editable,
    retained_pipeline_settings,
)
from forge.tui.design import SECONDARY, success_message
from forge.tui.editor_inspection import OpenInspection, StructuredValue
from forge.tui.editor_sessions import QueueEditor
from forge.tui.inspection import ActionMenu
from forge.tui.screens import Confirm, Detail, Form

COMPONENTS = {
    "application": "General · Forge application settings",
    "normalize": "Engine · Normalization",
    "merge": "Engine · Merge",
    "convert": "Engine · Conversion",
    "pipeline": "Pipeline Runtime",
}


STATUS_COLORS = {
    "valid": GREEN,
    "warning": YELLOW,
    "error": RED,
    "group": CYAN,
    "muted": MUTED,
}


def field_group(path: str) -> str:
    if "." not in path:
        return "general"
    parts = path.split(".")
    if parts[0] == "resources" and len(parts) > 2:
        return parts[1]
    return parts[0]


class FieldEditor(ModalScreen[str | None]):
    BINDINGS = [Binding("escape", "cancel", "Back")]

    def __init__(self, field: dict[str, Any]) -> None:
        super().__init__()
        self.field = field

    def compose(self) -> ComposeResult:
        field = self.field
        value = field.get("configured", field.get("effective"))
        if value is None:
            value = field.get("effective") if field.get("effective_known") else field.get("default")
        with Vertical(classes="dialog dialog-compact field-editor"):
            yield Static(str(field["path"]), markup=False, classes="dialog-title")
            with VerticalScroll(classes="dialog-body"):
                yield Static(plain(field.get("constraint")), markup=False)
                kind = field["kind"]
                if kind == "enum":
                    yield Select(
                        [(choice, choice) for choice in field["choices"]],
                        value=value if value in field["choices"] else Select.BLANK,
                        id="setting-enum",
                        compact=True,
                    )
                elif kind == "boolean":
                    yield Switch(value is True, id="setting-switch")
                else:
                    yield Input("" if value is None else str(value), id="setting-input")
                    if kind in {"bytes", "mib"}:
                        yield Select(
                            [(name or "Bytes", name) for name in UNITS],
                            value="MiB" if kind == "mib" else "",
                            allow_blank=False,
                            id="setting-unit",
                            compact=True,
                        )
                    elif kind == "milliseconds":
                        yield Static("Milliseconds")
            with Horizontal(classes="dialog-buttons"):
                yield Button("Cancel", id="setting-cancel")
                yield Button("Update draft", id="setting-update", variant="primary")

    def action_cancel(self) -> None:
        self.dismiss(None)

    @on(Button.Pressed)
    def choose(self, event: Button.Pressed) -> None:
        event.stop()
        if event.button.id == "setting-cancel":
            self.dismiss(None)
        else:
            self.submit()

    @on(Input.Submitted)
    def submit(self) -> None:
        kind = self.field["kind"]
        if kind == "enum":
            value = cast(Select[str], self.query_one("#setting-enum", Select)).value
            if not isinstance(value, str):
                self.notify("Select an allowed value.")
                return
            self.dismiss(str(value))
        elif kind == "boolean":
            self.dismiss(str(self.query_one(Switch).value).lower())
        else:
            raw = self.query_one(Input).value
            if kind in {"bytes", "mib"}:
                raw += str(cast(Select[str], self.query_one("#setting-unit", Select)).value)
            self.dismiss(raw)


CONFIG_TABS = {
    "General": "application",
    "Normalize": "normalize",
    "Merge": "merge",
    "Convert": "convert",
    "Pipeline": "pipeline",
}


@dataclass
class EditorView:
    search: str = ""
    basic: bool = False
    scroll: tuple[float, float] = (0, 0)


class DraftsChanged(Message):
    def __init__(self, counts: dict[str, int]) -> None:
        super().__init__()
        self.counts = counts


class ConfigurationTable(DataTable[Text]):
    class GroupPicked(Message):
        def __init__(self, key: str) -> None:
            super().__init__()
            self.key = key

    def on_click(self, event: Click) -> None:
        row = event.style.meta.get("row")
        if not isinstance(row, int) or not 0 <= row < self.row_count:
            return
        key = self.ordered_rows[row].key.value
        if key is not None and key.startswith("group:"):
            self.move_cursor(row=row)
            event.stop()
            event.prevent_default()
            self.post_message(self.GroupPicked(key))


class ConfigurationEditor(Vertical):
    BINDINGS = [
        Binding("f1", "field_help", "Field help"),
    ]
    DEFAULT_CSS = """
    ConfigurationEditor { height: 1fr; width: 1fr; }
    ConfigurationExplorer { align: center middle; }
    #configuration-explorer { height: 1fr; }
    #configuration-title, #configuration-status { height: auto; min-height: 1; }
    #configuration-fields { height: 1fr; min-height: 4; }
    .configuration-actions { height: auto; max-height: 5; overflow-x: auto; }
    .configuration-actions Button { min-width: 12; }
    """

    def __init__(
        self,
        context: ApplicationContext,
        queue_editor: QueueEditor,
        copy: Callable[[str], None],
        reload_application: Callable[[AppSettings, Path], None],
        environment: Callable[[], dict[str, object]],
        initial: str = "application",
        *,
        embedded: bool = False,
    ) -> None:
        super().__init__()
        self.context = context
        self.queue_editor = queue_editor
        self.copy = copy
        self.reload_application = reload_application
        self.environment = environment
        self.component = initial
        self.embedded = embedded
        self.views: dict[str, EditorView] = {}
        self.drafts: dict[str, ConfigurationDraft] = {}
        self.errors: dict[str, str] = {}
        self.database_status: dict[str, object] = {}
        self.selected: dict[str, str] = {}
        self.visible_fields: dict[str, tuple[str, dict[str, Any]]] = {}
        self.field: dict[str, Any] | None = None
        self.loading = False
        self.basic = False
        self.collapsed: dict[str, set[str]] = {}
        self.applied: set[str] = set()
        self.editing = False

    @property
    def host(self) -> App[object]:
        return cast(App[object], self.app)  # pyright: ignore[reportUnknownMemberType]

    def compose(self) -> ComposeResult:
        with Vertical(id="configuration-explorer"):
            yield Static("", id="configuration-title", markup=False)
            yield Static("", id="configuration-status", markup=False)
            if self.component == "environment":
                with VerticalScroll():
                    yield Static("", id="configuration-environment", markup=False)
            else:
                yield Input(placeholder="Search settings…", id="configuration-search")
                yield ConfigurationTable(
                    id="configuration-fields",
                    cursor_type="row",
                    zebra_stripes=True,
                    cursor_foreground_priority="renderable",
                )
                with Horizontal(classes="configuration-actions"):
                    yield Button("Edit field", id="edit")
                    yield Button("Help", id="help")
                    yield Button("Review & apply", id="apply", variant="primary")
                    yield Button("Documentation", id="category-docs")
                    yield Button("More", id="more")
                    yield OpenInspection(
                        StructuredValue({}), "Open effective", id="configuration-open-effective"
                    )
            if not self.embedded:
                with Horizontal(classes="configuration-actions"):
                    yield Button("Unset / Inherit", id="inherit")
                    yield Button("Copy link", id="copy-link")
                    yield Button("Back", id="close")

    @property
    def dirty(self) -> bool:
        return any(draft.dirty for draft in self.drafts.values())

    def counts(self) -> dict[str, int]:
        return {
            key: sum(self.modified(draft, field) for field in draft.fields)
            for key, draft in self.drafts.items()
            if draft.dirty
        }

    def modified(self, draft: ConfigurationDraft, field: dict[str, Any]) -> bool:
        previous = draft.baseline.get(field["path"], {})
        return any(previous.get(key) != field.get(key) for key in ("configured", "configured_path"))

    def activate(self, component: str) -> None:
        if not self.is_mounted:
            self.component = component
            return
        if component != self.component:
            search = self.query_one("#configuration-search", Input)
            table = cast(DataTable[Text], self.query_one(DataTable))
            self.views[self.component] = EditorView(
                search.value,
                self.basic,
                (table.scroll_x, table.scroll_y),
            )
            self.component = component
            state = self.views.get(component, EditorView())
            with search.prevent(Input.Changed):
                search.value = state.search
            self.basic = state.basic
        worker = self.load(component)
        if component in self.views:
            self.restore_scroll(component, worker)

    @work(exclusive=True, group="configuration-scroll", exit_on_error=False)
    async def restore_scroll(self, component: str, worker: Worker[None]) -> None:
        await worker.wait()
        if self.component == component:
            x, y = self.views[component].scroll
            self.query_one(DataTable).scroll_to(x=x, y=y, animate=False)

    def discard(self) -> None:
        self.workers.cancel_group(self, "configuration-load")  # pyright: ignore[reportUnknownMemberType]
        self.drafts.clear()
        self.applied.clear()
        self.post_message(DraftsChanged({}))

    def navigate_selected(self, row_key: str) -> None:
        if row_key.startswith("group:"):
            if self.query_one("#configuration-search", Input).value.strip():
                return
            groups = self.collapsed.setdefault(self.component, set())
            group = row_key.removeprefix("group:")
            if group in groups:
                groups.remove(group)
            else:
                groups.add(group)
            self.selected[self.component] = row_key
            self.render_fields()
        elif row_key in self.visible_fields:
            self.show_field(row_key)
            self.interact("edit")

    def on_mount(self) -> None:
        if self.component != "environment":
            table = cast(DataTable[Text], self.query_one(DataTable))
            table.add_column("Setting", width=26)
            table.add_column("Configured", width=17)
            table.add_column("Effective", width=12)
            table.add_column("Default", width=11)
        if not self.embedded:
            self.load(self.component)

    @work(exclusive=True, group="configuration-load", exit_on_error=False)
    async def load(self, component: str, *, validate: bool = False) -> None:
        self.loading = True
        self.render_fields()
        try:
            if component == "environment":
                try:
                    result = await disk(
                        inspect_database,
                        project_root=self.context.paths.root,
                        testing=self.context.testing,
                    )
                except DatabaseInspectionError as error:
                    self.database_status = {
                        "Inspection state": str(error),
                        "Inspection stage": error.stage,
                        "Connected": error.connected,
                    }
                    if error.status:
                        self.database_status["Observed registry"] = asdict(error.status)
                else:
                    self.database_status = {"Connected": True, **asdict(result)}
                return
            for key in tuple(CONFIG_TABS.values()) if component == "coverage" else (component,):
                if key not in COMPONENTS:
                    continue
                try:
                    if key not in self.drafts:
                        self.drafts[key] = await disk(ConfigurationDraft, self.context, key)
                    elif validate:
                        if not self.drafts[key].dirty:
                            self.drafts[key] = await disk(ConfigurationDraft, self.context, key)
                        else:
                            await disk(self.drafts[key].refresh)
                    self.errors.pop(key, None)
                except Exception as error:
                    self.errors[key] = plain(str(error))
        finally:
            self.loading = False
            self.render_fields()

    @on(Input.Changed, "#configuration-search")
    def search(self) -> None:
        self.render_fields()

    @on(Input.Submitted, "#configuration-search")
    def focus_results(self) -> None:
        table = cast(DataTable[Text], self.query_one(DataTable))
        if table.row_count:
            table.focus()
            table.move_cursor(row=min(1, table.row_count - 1))

    def render_fields(self) -> None:
        if not self.is_mounted:
            return
        self.post_message(DraftsChanged(self.counts()))
        self.query_one("#configuration-title", Static).update(
            COMPONENTS.get(self.component, "Registry settings")
        )
        if self.component == "environment":
            self.query_one("#configuration-environment", Static).update(
                plain({**self.environment(), **self.database_status})
            )
            return

        key = self.component
        draft = self.drafts.get(key)
        status = self.query_one("#configuration-status", Static)
        source = (
            draft.path
            if draft
            else (
                (self.context.settings_file or self.context.paths.settings_file)
                if key == "application"
                else self.context.paths.engine_config_dir / f"{key}.toml"
            )
        )
        if source.is_relative_to(self.context.paths.root):
            source = source.relative_to(self.context.paths.root)
        error = self.errors.get(key)
        if self.loading:
            label = "VALIDATION REQUIRED" if draft and draft.dirty else "LOADING"
            message = ""
            style = "warning"
        elif error or (draft and draft.source_error):
            label, message, style = (
                "UNAVAILABLE",
                error or (draft.source_error if draft else "") or "",
                "error",
            )
        elif draft and draft.status != "Validated":
            label, message, style = "INVALID", draft.status, "error"
        elif draft:
            label = "APPLIED · VALID" if key in self.applied and not draft.dirty else "VALID"
            message, style = "", "valid"
        else:
            label, message, style = "UNAVAILABLE", "", "warning"
        text = Text(str(source) + " · Shared workspace · ")
        text.append(label, STATUS_COLORS[style])
        if draft and draft.dirty:
            text.append(" · MODIFIED", STATUS_COLORS["warning"])
        if message:
            text.append(" · " + plain(message), STATUS_COLORS[style])
        status.update(text)

        table = cast(DataTable[Text], self.query_one(DataTable))
        table.clear()
        self.visible_fields.clear()
        self.field = None
        open_effective = self.query_one("#configuration-open-effective", OpenInspection)
        effective = draft.effective_document if draft else None
        open_effective.display = effective is not None and not self.loading
        open_effective.target = StructuredValue(effective)
        groups: dict[str, list[dict[str, Any]]] = {}
        query = self.query_one("#configuration-search", Input).value.casefold().split()
        for field in draft.fields if draft else ():
            path = field["path"]
            group = field_group(path)
            searchable = f"{path} {group.replace('_', ' ')} {field.get('purpose', '')}".casefold()
            if any(word not in searchable for word in query):
                continue
            if self.basic and not query and path.rsplit(".", 1)[-1] not in BASIC:
                continue
            groups.setdefault(group, []).append(field)

        saved = self.selected.get(key)
        selected_index = None
        first_field = None
        collapsed = self.collapsed.get(key, set())
        for group, fields in groups.items():
            closed = group in collapsed and not query
            header = f"group:{group}"
            if header == saved:
                selected_index = table.row_count
            title = group.replace("_", " ").title()
            if key == "pipeline" and group != "general":
                title += " overrides"
            table.add_row(
                Text(f"{'▸' if closed else '▾'} {title}", style=STATUS_COLORS["group"]),
                Text(),
                Text(),
                Text(),
                key=header,
            )
            if closed or draft is None:
                continue
            for field in fields:
                path = field["path"]
                row_key = f"{key}:{path}"
                self.visible_fields[row_key] = (key, field)
                if row_key == saved:
                    selected_index = table.row_count
                if first_field is None:
                    first_field = (table.row_count, row_key)
                table.add_row(*self.field_cells(draft, field), key=row_key, height=None)

        if not table.row_count and draft:
            table.add_row(Text("No matching settings", style=STATUS_COLORS["muted"]))
        if selected_index is not None and saved is not None:
            table.move_cursor(row=selected_index)
            self.show_field(saved)
        elif first_field:
            table.move_cursor(row=first_field[0])
            self.show_field(first_field[1])
        else:
            self.update_field_actions()

    def field_cells(self, draft: ConfigurationDraft, field: dict[str, Any]) -> tuple[Text, ...]:
        path = field["path"]
        modified = self.modified(draft, field)
        read_only = not editable(field) or bool(draft.source_error)
        inactive = bool(field.get("inactive_reasons"))
        inherited = (
            self.component == "pipeline"
            and path != "schema_version"
            and not field.get("configured_path")
        )
        marker = "* " if modified else "– " if read_only or inactive else "  "
        color = (
            STATUS_COLORS["warning"]
            if modified
            else (STATUS_COLORS["muted"] if read_only or inactive else "")
        )
        configured = "Inherited" if inherited else plain(field.get("configured"))
        if modified:
            previous = draft.baseline.get(path, {})
            old = (
                "Inherited"
                if self.component == "pipeline" and not previous.get("configured_path")
                else plain(previous.get("configured"))
            )
            configured = f"{old} → {configured}"
        configured_color = color or (CYAN if inherited else MUTED if configured == "—" else BLUE)
        effective_color = color or ("" if field.get("effective_known") else SECONDARY)
        return (
            Text(marker + path, style=color, overflow="fold"),
            Text(configured, style=configured_color, overflow="fold", justify="right"),
            Text(
                plain(field.get("effective")) if field.get("effective_known") else "Unvalidated",
                style=effective_color,
                overflow="fold",
                justify="right",
            ),
            Text(
                plain(field.get("default")),
                style=STATUS_COLORS["muted"],
                overflow="fold",
                justify="right",
            ),
        )

    @on(ConfigurationTable.GroupPicked)
    def toggle_group(self, event: ConfigurationTable.GroupPicked) -> None:
        self.navigate_selected(event.key)

    @on(DataTable.RowHighlighted)
    def highlight(self, event: DataTable.RowHighlighted) -> None:
        if event.row_key.value is not None:
            self.show_field(event.row_key.value)

    @on(DataTable.RowSelected)
    def select_field(self, event: DataTable.RowSelected) -> None:
        if event.row_key.value is not None:
            self.navigate_selected(event.row_key.value)

    def show_field(self, row_key: str) -> None:
        active = self.visible_fields.get(row_key)
        self.selected[self.component] = row_key
        self.field = active[1] if active else None
        self.update_field_actions()

    def update_field_actions(self) -> None:
        field = self.field
        draft = self.drafts.get(self.component)
        self.query_one("#help", Button).disabled = field is None
        self.query_one("#edit", Button).disabled = (
            field is None or not editable(field) or draft is None or bool(draft.source_error)
        )

    def field_help(self, key: str, field: dict[str, Any], draft: ConfigurationDraft) -> Detail:
        path = field["path"]
        reason = (
            "Editable"
            if editable(field)
            else "Unsupported editor kind"
            if field["mutable"]
            else "Immutable schema identity"
            if path in {"schema_version", "version"}
            else "The authoritative schema marks this field read-only"
        )
        if draft.source_error:
            reason = f"Read-only: {draft.source_error}"
        modified = self.modified(draft, field)
        detail = {
            "Purpose": field.get("purpose"),
            "Field": path,
            "Draft": "Modified" if modified else "Unchanged",
            "Name": path.rsplit(".", 1)[-1].replace("_", " ").title(),
            "Category": path.rsplit(".", 1)[0] if "." in path else "General",
            "Component": COMPONENTS[key],
            "Source": str(draft.path),
            "Configured": field.get("configured"),
            "Configured at": field.get("configured_path")
            or ("Inherited" if key == "pipeline" else "Built-in default"),
            "Effective source": field.get("effective_source"),
            "Effective": field.get("effective") if field.get("effective_known") else "Unvalidated",
            "Default": field["default"],
            "Type": field["kind"],
            "Allowed": field.get("choices"),
            "Constraints": field.get("constraint"),
            "Units": field.get("units")
            or {"bytes": "Bytes", "mib": "MiB", "milliseconds": "Milliseconds"}.get(field["kind"]),
            "Required in file": field["required"]
            and (key != "pipeline" or path == "schema_version"),
            "Dependencies": field.get("dependencies"),
            "Active": field.get("active"),
            "Inactive reasons": field.get("inactive_reasons"),
            "Risk": field.get("risk"),
            "Editor": reason,
            "Documentation": self.link(),
        }
        if modified:
            detail["Originally configured"] = draft.baseline.get(path, {}).get("configured")
        if "base" in field:
            detail["Base effective"] = field["base"]
        if field.get("resolved_path"):
            detail["Resolved storage root"] = field["resolved_path"]
        return Detail(
            f"Setting help · {path}",
            detail,
            actions={
                "Full documentation": partial(self.interact, "docs"),
                "Copy field path": partial(self.copy, path),
            },
        )

    def coverage(self) -> str:
        sections: list[str] = []
        for name, key in CONFIG_TABS.items():
            item = self.drafts.get(key)
            if not item:
                sections.append(f"{name}\n{self.errors.get(key, 'Not loaded')}")
                continue
            unknown = [field["path"] for field in item.fields if field["kind"] not in EDITOR_KINDS]
            missing = item.documentation_missing
            docs = (
                f"Missing documentation {len(missing)}"
                if missing is not None
                else "Documentation targets unverified (local reference unavailable)"
            )
            count = len(item.fields)
            writable = 0 if item.source_error else sum(editable(field) for field in item.fields)
            sections.append(
                f"{name}\nSupported {count} · Advanced display {count} · Editable {writable}"
                f" · Read-only {count - writable} · Unsupported {len(unknown)} · {docs}\n"
                f"{item.status} · Schema {plain(item.schema_version)}"
                + (f" · {item.executable}" if item.executable else "")
                + ("\n" + "\n".join(unknown) if unknown else "")
                + ("\nMissing targets\n" + "\n".join(missing) if missing else "")
            )
        return "\n\n".join(sections)

    def link(self, *, field_link: bool = True, global_link: bool = False) -> str:
        active = self.visible_fields.get(self.selected.get(self.component, ""))
        key, field = active if active else (self.component, None)
        if global_link:
            key, field = "", None
        base = os.environ.get("MARKETFORGE_DOCS_URL", SITE_URL)
        parsed = urlsplit(base)
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.hostname
            or parsed.username
            or parsed.query
            or parsed.fragment
        ):
            base = SITE_URL
        draft = self.drafts.get(key)
        path = field["path"] if field and field_link else ""
        if (
            draft
            and draft.documentation_missing is not None
            and path in draft.documentation_missing
        ):
            path = ""
        return documentation(key if key in PAGES else "", path, base=base)

    def action_field_help(self) -> None:
        self.interact("help")

    @on(Button.Pressed)
    def button(self, event: Button.Pressed) -> None:
        if event.button.id:
            event.stop()
            self.interact(event.button.id)

    @work(group="configuration-edit", exit_on_error=False)
    async def interact(self, action: str) -> None:
        if self.editing:
            return
        self.editing = True
        try:
            await self.perform(action)
        except Exception as error:
            self.host.push_screen(
                Detail("Configuration draft", plain(str(error)), raw=True, error=True)
            )
        finally:
            self.editing = False

    async def perform(self, action: str) -> None:
        field = self.field
        active = self.visible_fields.get(self.selected.get(self.component, ""))
        key = active[0] if active else self.component
        draft = self.drafts.get(key)
        if action == "close":
            if not any(
                item.dirty for item in self.drafts.values()
            ) or await self.host.push_screen_wait(
                Confirm(
                    "Discard unsaved settings?",
                    "This closes the explorer and discards all pending drafts.",
                )
            ):
                if isinstance(self.screen, ConfigurationExplorer):
                    self.screen.dismiss(None)
        elif action == "help" and field and draft:
            self.host.push_screen(self.field_help(key, field, draft))
        elif action == "docs":
            url = self.link()
            if not await disk(webbrowser.open, url):
                self.notify(url, title="Documentation URL")
        elif action == "global-docs":
            url = self.link(global_link=True)
            if not await disk(webbrowser.open, url):
                self.notify(url, title="Configuration documentation")
        elif action == "category-docs":
            url = self.link(field_link=False)
            if not await disk(webbrowser.open, url):
                self.notify(url, title="Component documentation")
        elif action == "copy-link":
            self.copy(self.link())
        elif action == "pinned":
            values = await self.host.push_screen_wait(
                Form(
                    Action(
                        "Inspect pinned pipeline settings",
                        "Configs",
                        (Field("reference", "Retained pipeline run ID", required=True),),
                    ),
                    local=True,
                )
            )
            if values:
                evidence = await disk(
                    retained_pipeline_settings,
                    self.context,
                    str(values["reference"]),
                    self.environment().get("Registry identity"),
                )
                self.host.push_screen(Detail("Pinned pipeline settings · Read-only", evidence))
        elif action == "more":
            options = {
                "Use default": "default",
                "Unset / Inherit": "inherit",
                "Reset pending field": "reset",
                "Copy field path": "copy-path",
                "Copy effective value": "copy-value",
                "Field documentation": "docs",
                "Copy documentation link": "copy-link",
                "Configuration guide": "global-docs",
                "Edit TOML draft": "toml",
                "Reload file": "reload",
                "Validate current draft": "validate-current",
                "Validate all and show coverage": "validate",
                "Inspect pinned run": "pinned",
                "Show advanced fields" if self.basic else "Show basic fields": "basic",
            }
            choice = await self.host.push_screen_wait(
                ActionMenu("Configuration actions", tuple(options))
            )
            if choice:
                await self.perform(options[choice])
        elif action == "validate":
            worker = self.load("coverage", validate=True)
            await worker.wait()
            self.host.push_screen(Detail("Configuration coverage", self.coverage(), raw=True))
        elif action == "validate-current" and draft:
            await disk(draft.refresh)
            self.render_fields()
        elif action == "basic":
            self.basic = not self.basic
            self.render_fields()
        elif self.loading:
            self.notify("Wait for configuration inspection to finish.")
        elif action == "reload" and key in COMPONENTS:
            if (
                not draft
                or not draft.dirty
                or await self.host.push_screen_wait(
                    Confirm(
                        "Reload configuration?", "Reloading discards this file’s pending draft."
                    )
                )
            ):
                self.applied.discard(key)
                self.drafts.pop(key, None)
                self.load(self.component)
        elif draft and action in {"apply", "toml"}:
            if draft.source_error:
                self.notify(draft.source_error, severity="error")
                return
            if action == "toml":

                async def validate(path: Path) -> object:
                    candidate = copy_draft(draft)
                    candidate.candidate = await disk(path.read_bytes)
                    return await disk(candidate.preview)

                async def accept(original: bytes, updated: bytes) -> None:
                    current = self.drafts.get(key)
                    if (
                        self.editing
                        or current is None
                        or current.path != draft.path
                        or current.initial != draft.initial
                        or current.candidate != original
                    ):
                        raise ValueError(
                            "The configuration form changed. Keep the editor draft and reload the form."
                        )
                    self.editing = True
                    try:
                        await disk(draft.check_editor_source)
                        candidate = copy_draft(current)
                        candidate.candidate = updated
                        await disk(candidate.refresh)
                        self.drafts[key] = candidate
                        self.render_fields()
                        self.notify(
                            "Validated TOML loaded. Use Review & apply in Configs to save it."
                        )
                    finally:
                        self.editing = False

                self.queue_editor(draft.path, validate, accept, draft.candidate)
            elif draft.dirty:
                change = await disk(draft.preview)
                if (
                    isinstance(change, ApplicationChange)
                    and change.settings.storage.data_root != self.context.settings.storage.data_root
                    and any(
                        item.dirty for component, item in self.drafts.items() if component != key
                    )
                ):
                    self.notify(
                        "A storage root change reloads the workspace. Finish or discard the other drafts first.",
                        severity="warning",
                    )
                    return
                risk = (
                    change.requires_acknowledgement
                    if isinstance(change, Change)
                    else change.risk
                    if isinstance(change, PipelineChange)
                    else False
                )
                fields = change.changes if isinstance(change, PipelineChange) else change.fields
                diff = "".join(
                    difflib.unified_diff(
                        draft.initial.decode().splitlines(keepends=True),
                        change.candidate.decode().splitlines(keepends=True),
                        "Original",
                        "Validated draft",
                    )
                )
                if await self.host.push_screen_wait(
                    Confirm(
                        "Acknowledge integrity risk and apply"
                        if risk
                        else "Apply validated configuration",
                        plain(
                            {
                                "File": str(draft.path),
                                "Schema": draft.schema_version,
                                "Validation": "Passed",
                                "Changes": fields,
                            }
                        )
                        + "\n\n"
                        + plain(diff),
                    )
                ):
                    await disk(draft.apply, change, acknowledged=risk)
                    self.applied.add(key)
                    self.notify(success_message("Configuration applied"), markup=True)
                    if isinstance(change, ApplicationChange):
                        self.reload_application(change.settings, draft.path)
                        from dataclasses import replace

                        self.context = replace(
                            self.context, settings=change.settings, settings_file=draft.path
                        )
                    self.drafts.pop(key)
                    self.load(self.component)
            else:
                self.notify("This file has no pending changes.")
        elif draft and field:
            if action == "copy-value":
                self.copy(plain(field.get("effective")))
                return
            if action == "copy-path":
                self.copy(field["path"])
                return
            if action not in {"edit", "default", "inherit", "reset"} or not editable(field):
                return
            if draft.source_error:
                self.notify(draft.source_error, severity="error")
                return
            if action == "edit":
                raw = await self.host.push_screen_wait(FieldEditor(field))
                if raw is None:
                    return
            elif action == "inherit":
                if field["required"] and key not in {"pipeline", "application"}:
                    self.notify("This field must be explicitly configured.")
                    return
                raw = None
            else:
                value = field.get("default")
                if action == "reset":
                    saved = draft.baseline[field["path"]]
                    value = saved.get("configured")
                if value is None and field["required"] and key not in {"pipeline", "application"}:
                    self.notify("No explicit default is available for this required field.")
                    return
                raw = (
                    str(value).lower()
                    if isinstance(value, bool)
                    else str(value)
                    if value is not None
                    else None
                )
            await disk(draft.patch, field, raw)
            await disk(draft.refresh)
            self.render_fields()


class ConfigurationExplorer(ModalScreen[None]):
    """Command-palette access to auxiliary configuration tools."""

    BINDINGS = [Binding("escape", "close_explorer", "Back")]

    def __init__(
        self,
        context: ApplicationContext,
        queue_editor: QueueEditor,
        copy: Callable[[str], None],
        reload_application: Callable[[AppSettings, Path], None],
        environment: Callable[[], dict[str, object]],
        initial: str = "application",
    ) -> None:
        super().__init__()
        self.editor = ConfigurationEditor(
            context, queue_editor, copy, reload_application, environment, initial
        )

    def compose(self) -> ComposeResult:
        yield self.editor

    def action_close_explorer(self) -> None:
        self.editor.interact("close")
