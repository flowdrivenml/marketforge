"""Readable resource details and explicit full-value evidence."""

from collections.abc import Callable, Mapping
from decimal import Decimal
from typing import Literal, cast

from rich.cells import cell_len
from rich.text import Text
from textual import events, work
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical, VerticalScroll
from textual.screen import ModalScreen, Screen
from textual.widgets import Button, DataTable, Input, Static

from forge.shared.terminal.palette import BLUE, CYAN, MUTED
from forge.tui.clipboard import copy_text, field_text, plain
from forge.tui.controller import document
from forge.tui.design import SECONDARY
from forge.tui.editor_inspection import OpenInspection, RetainedFile, StructuredValue, structured
from forge.tui.presentation import entries, status
from forge.tui.screens import Detail, WorkspaceHost
from forge.tui.text_selection import SelectableTable

SUMMARY_GROUPS = {
    "Identity": {
        "id",
        "publication_id",
        "registry_publication_id",
        "engine_publication_id",
        "engine_dataset_id",
        "output_ref",
        "local_id",
        "run_id",
        "execution_id",
        "instrument_id",
        "operation",
        "recipe",
        "environment",
        "historical_instrument_id",
    },
    "Market data": {
        "exchange",
        "symbol",
        "instrument",
        "instruments",
        "instrument_type",
        "market",
        "market_category",
        "data_type",
        "target_quote",
        "stream_count",
    },
    "Storage and coverage": {
        "file_count",
        "partition_count",
        "primary_file_count",
        "recorded_bytes",
        "size_bytes",
        "bytes_written",
        "recorded_size",
        "start_at",
        "end_at",
        "first_observed_utc",
        "last_observation_utc",
        "first_observation_utc",
        "manifest_path",
        "resolved_path",
    },
    "Timing": {
        "started_at",
        "finished_at",
        "created_at",
        "updated_at",
        "elapsed",
        "elapsed_seconds",
    },
    "Recorded outcome": {
        "status",
        "state",
        "recipe_state",
        "verification",
        "registration",
        "integrity",
        "validation",
    },
}
OUTCOME_FIELDS = SUMMARY_GROUPS["Recorded outcome"]


def detail_value(name: str, value: object) -> Text:
    key = name.lower().replace(" ", "_")
    text = field_text(name, value)
    if key in OUTCOME_FIELDS or key in {"execution", "service_outcome", "engine_compatibility"}:
        return status(text)
    color = ""
    if text == "—":
        color = MUTED
    elif key in {"symbol", "instrument", "instruments"}:
        color = CYAN
    elif key in SUMMARY_GROUPS["Identity"]:
        color = BLUE
    return Text(text, style=color)


def fields(value: object) -> Text:
    mapping = document(value)
    values = (
        {name: item for name, item in mapping.items() if not isinstance(item, (dict, list, tuple))}
        if mapping
        else {"Value": value}
    )
    groups: dict[str, list[tuple[str, object]]] = {}
    for name, item in values.items():
        key = name.lower().replace(" ", "_")
        heading = next((title for title, keys in SUMMARY_GROUPS.items() if key in keys), "Details")
        groups.setdefault(heading, []).append((name, item))
    result = Text()
    for heading in (*SUMMARY_GROUPS, "Details"):
        records = groups.get(heading, [])
        if not records:
            continue
        if result:
            result.append("\n")
        if len(groups) > 1:
            result.append(f"{heading}\n", f"bold {CYAN}")
        labels = [name.replace("_", " ").capitalize() for name, _ in records]
        width = min(30, max(map(cell_len, labels)))
        for label, (name, item) in zip(labels, records, strict=True):
            result.append(label + " " * max(2, width - cell_len(label) + 2), SECONDARY)
            result.append_text(detail_value(name, item))
            result.append("\n")
    return result


def record_cell(key: str, value: object) -> Text:
    text = detail_value(key, value)
    text.no_wrap = True
    text.overflow = "ellipsis"
    if (
        isinstance(value, (int, float, Decimal))
        and not isinstance(value, bool)
        and not key.lower().endswith("id")
    ):
        text.justify = "right"
    return text


class RecordTable(SelectableTable):
    def on_mount(self) -> None:
        self.zebra_stripes = True
        self.cursor_foreground_priority = "renderable"


class ActionMenu(ModalScreen[str | None]):
    BINDINGS = [
        Binding("escape", "back", "Back"),
        Binding("up", "app.focus_previous", "Previous action", show=False),
        Binding("down", "app.focus_next", "Next action", show=False),
    ]

    def __init__(
        self,
        title: str,
        actions: tuple[str, ...],
        *,
        primary: str = "",
        anchor: tuple[int, int] | None = None,
    ) -> None:
        super().__init__()
        self.heading, self.actions = title, actions
        self.primary = primary
        self.menu_anchor = anchor

    def compose(self) -> ComposeResult:
        with VerticalScroll(classes="menu"):
            yield Static(self.heading, classes="section-title", markup=False)
            for index, label in enumerate(self.actions):
                action = label.casefold()
                variant: Literal["error", "primary", "default"] = "default"
                if action.startswith(("delete", "stop", "request stop", "remove ")) or (
                    action.startswith("preview ") and "deletion" in action
                ):
                    variant = "error"
                elif label == self.primary or action.startswith("add to "):
                    variant = "primary"
                separated = (
                    index > 0
                    and action.startswith("inspect")
                    and self.actions[index - 1].casefold().startswith("add to ")
                )
                yield Button(
                    label,
                    id=f"menu-{index}",
                    classes="compact inspect-action" if separated else "compact",
                    variant=variant,
                )

    def on_mount(self) -> None:
        menu = self.query_one(VerticalScroll)
        menu.styles.width = min(66, max(24, max(map(cell_len, (self.heading, *self.actions))) + 6))
        if self.actions:
            self.query_one("#menu-0", Button).focus()
        if self.menu_anchor is not None:
            self.styles.align = ("left", "top")
            self.call_after_refresh(self.position_menu)

    def on_resize(self, event: events.Resize) -> None:
        if self.menu_anchor is not None:
            self.call_after_refresh(self.position_menu)

    def position_menu(self) -> None:
        menu = self.query_one(VerticalScroll)
        assert self.menu_anchor is not None
        x, row = self.menu_anchor
        width, height = menu.outer_size
        y = row + 1 if row + 1 + height <= self.size.height else row - height
        menu.styles.offset = (
            max(0, min(x, self.size.width - width)),
            max(0, min(y, self.size.height - height)),
        )

    def action_back(self) -> None:
        self.dismiss(None)

    def on_button_pressed(self, event: Button.Pressed) -> None:
        self.dismiss(self.actions[int((event.button.id or "").removeprefix("menu-"))])


class Inspection(Screen[None]):
    BINDINGS = [
        Binding("escape", "back", "Back"),
        Binding("f2", "actions", "Actions"),
        Binding("f3", "evidence", "Evidence"),
        Binding("y", "copy_section", "Copy section"),
        Binding("ctrl+y", "copy_selection", "Copy selected text"),
    ]

    def __init__(
        self,
        title: str,
        evidence: object,
        *,
        actions: dict[str, Callable[[], object]] | None = None,
        sections: dict[str, object] | None = None,
        choose: Callable[[str, Mapping[str, object]], None] | None = None,
        section: str | None = None,
        searchable: bool = False,
        evidence_label: str = "F3 Evidence",
        files: tuple[RetainedFile, ...] = (),
        section_values: Mapping[str, object] | None = None,
        scalar_summary: bool = False,
    ) -> None:
        super().__init__()
        self.heading, self.evidence = title, evidence
        self.links = actions or {}
        self.sections = dict(sections or sections_for(evidence))
        self.files = files
        self.section_values = section_values or {}
        self.scalar_summary = scalar_summary
        if files:
            self.sections.setdefault("Artifacts", {})
        self.column_keys: tuple[str, ...] = ()
        self.section = section if section in self.sections else next(iter(self.sections))
        self.choose = choose
        self.searchable = searchable
        self.evidence_label = evidence_label
        self.search = ""
        self.items: list[Mapping[str, object]] = []

    def compose(self) -> ComposeResult:
        yield Static(self.heading, classes="screen-heading", markup=False)
        yield Static("", classes="workspace-monitor", markup=False)
        with Horizontal(classes="section-tabs"):
            for n, section in enumerate(self.sections):
                yield Button(section, id=f"section-{n}", classes="compact")
        yield OpenInspection(StructuredValue(None), id="inspect-open-json")
        if self.files:
            yield from self.artifact_row(self.files[0], "inspect-primary-file", primary=True)
        if self.searchable:
            yield Input(placeholder="Search text, severity, or stage", id="inspection-search")
        with VerticalScroll(id="inspection-scroll"):
            yield Static(id="inspection-fields")
            with Vertical(id="inspection-artifacts"):
                for index, target in enumerate(self.files):
                    yield from self.artifact_row(target, f"inspect-file-{index}")
        yield RecordTable(cursor_type="cell", id="inspection-rows")
        yield Static(id="inspection-operation", classes="inspect-operation")
        with Horizontal(classes="screen-footer"):
            yield Button("Esc Back", id="inspect-back", classes="compact")
            yield Button("F2 Actions", id="inspect-actions", classes="compact")
            yield Button("Copy section", id="inspect-copy", classes="compact tertiary")
            yield Button("Ctrl+Y Copy selection", id="inspect-copy-selection", classes="compact")
            yield Button(self.evidence_label, id="inspect-raw", classes="compact")

    def artifact_row(
        self, target: RetainedFile, id: str, *, primary: bool = False
    ) -> ComposeResult:
        with Horizontal(
            id="inspection-primary-artifact" if primary else None,
            classes="inspection-artifact",
        ):
            yield Static(f"{target.label}  ·  {target.path.name}", markup=False)
            yield OpenInspection(target, "Open", id=id)

    def on_mount(self) -> None:
        self.render_section()

    def render_section(self) -> None:
        self.clear_selection()
        self.query_one(RecordTable).clear_text_selection()
        value = self.sections[self.section]
        artifacts = bool(self.files) and self.section == "Artifacts"
        self.query_one("#inspection-artifacts").display = artifacts
        if self.files:
            self.query_one("#inspection-primary-artifact").display = (
                self.section == next(iter(self.sections)) and not artifacts
            )
        open_json = self.query_one("#inspect-open-json", OpenInspection)
        target = structured(self.section_values.get(self.section, value))
        scalar_summary = (
            self.scalar_summary
            and self.section == "Summary"
            and not any(structured(item) is not None for item in document(value).values())
        )
        open_json.display = target is not None and not artifacts and not scalar_summary
        if target is not None:
            open_json.target = target
        self.items = [
            entry
            for entry in entries(value)
            if not self.search or self.search in plain(entry).casefold()
        ]
        scroll = self.query_one("#inspection-scroll")
        rows = self.query_one(RecordTable)
        scroll.display = not bool(self.items)
        rows.display = bool(self.items)
        self.query_one("#inspection-fields").display = not artifacts or bool(value)
        for index, section in enumerate(self.sections):
            self.query_one(f"#section-{index}", Button).set_class(section == self.section, "active")
        if not self.items:
            self.query_one("#inspection-fields", Static).update(
                Text("No matching entries")
                if self.search
                else Text("No records in this section", style=SECONDARY)
                if isinstance(value, list)
                else value
                if isinstance(value, Text)
                else fields(value)
            )
            return
        rows.clear(columns=True)
        keys = tuple(dict.fromkeys(key for item in self.items for key in item))
        self.column_keys = keys
        for key in keys:
            label = key.replace("_", " ").title()
            width = max(len(label), *(len(field_text(key, item.get(key))) for item in self.items))
            rows.add_column(label, width=min(40, max(9, width)))
        for item in self.items:
            rows.add_row(*(record_cell(key, item.get(key)) for key in keys))
        if not isinstance(self.focused, Input):
            rows.focus()

    def on_input_changed(self, event: Input.Changed) -> None:
        self.search = event.value.casefold()
        self.render_section()

    def on_data_table_cell_selected(self, event: DataTable.CellSelected) -> None:
        if event.coordinate.row >= len(self.items):
            return
        item = self.items[event.coordinate.row]
        column = event.coordinate.column
        exact = item.get(self.column_keys[column]) if column < len(self.column_keys) else None
        if self.choose is not None and self.section in {
            "Fields",
            "Child runs",
            "Files",
            "Streams",
            "Parents",
            "Selected files",
            "Raw inputs",
            "Selected archives",
            "Markets",
            "Publications",
            "Partitions",
            "Timelines",
        }:
            self.choose(self.section, item)
        elif structured(exact) is not None:
            cast(WorkspaceHost, self).app.push_screen(
                Detail(self.column_keys[column].replace("_", " ").title(), exact)
            )
        else:
            value = event.value
            cast(WorkspaceHost, self).app.push_screen(Detail("Exact value", str(value), raw=True))

    def action_back(self) -> None:
        self.dismiss(None)

    def check_action(self, action: str, parameters: tuple[object, ...]) -> bool | None:
        if action == "copy_section" and isinstance(self.focused, Input):
            return False
        return super().check_action(action, parameters)

    @work(exclusive=True, exit_on_error=False)
    async def action_copy_selection(self) -> None:
        table = self.query_one(RecordTable)
        text = table.selected_text if table.display else self.get_selected_text()
        if text:
            await copy_text(cast(WorkspaceHost, self).app, text)
        else:
            self.notify("Drag over text to select it.", severity="warning")

    def action_copy_section(self) -> None:
        value = self.items if self.search else self.sections[self.section]
        cast(WorkspaceHost, self).app.push_screen(Detail(self.section, value))

    def action_evidence(self) -> None:
        cast(WorkspaceHost, self).app.push_screen(Detail("Complete evidence", self.evidence))

    def action_actions(self) -> None:
        if self.links:

            def selected(key: str | None) -> None:
                if key is not None:
                    self.links[key]()

            cast(WorkspaceHost, self).app.push_screen(
                ActionMenu(self.heading, tuple(self.links)), selected
            )

    def on_button_pressed(self, event: Button.Pressed) -> None:
        key = event.button.id or ""
        if key.startswith("section-"):
            self.section = tuple(self.sections)[int(key.removeprefix("section-"))]
            self.render_section()
        elif key == "inspect-back":
            self.action_back()
        elif key == "inspect-actions":
            self.action_actions()
        elif key == "inspect-copy-selection":
            self.action_copy_selection()
        elif key == "inspect-copy":
            self.action_copy_section()
        elif key == "inspect-raw":
            self.action_evidence()


def sections_for(value: object) -> dict[str, object]:
    info = document(value)
    result: dict[str, object] = {"Summary": info if info else value}
    for key, label in (
        ("columns", "Schema"),
        ("rows", "Sample"),
        ("streams", "Streams"),
        ("file", "File"),
        ("publication", "Publication"),
        ("datasets", "Files"),
        ("primary_files", "Files"),
        ("partitions", "Partitions"),
        ("row_groups", "Row groups"),
        ("raw", "Raw inputs"),
        ("inputs", "Inputs"),
        ("jobs", "Jobs"),
        ("tasks", "Tasks"),
        ("requests", "Requests"),
        ("outputs", "Outputs"),
        ("diagnostics", "Diagnostics"),
        ("validation", "Validation"),
        ("archives", "Archives"),
        ("sources", "Sources"),
        ("parents", "Parents"),
        ("publication_datasets", "Selected files"),
        ("publication_raw", "Selected archives"),
        ("events", "Activity"),
        ("targets", "Targets"),
        ("snapshots", "Snapshots"),
        ("totals", "Impact"),
        ("entries", "Ranking"),
        ("instruments", "Instruments"),
    ):
        if key in info:
            result[label] = info[key]
    source = info.get("source_toml")
    if isinstance(source, str):
        result["Source TOML"] = Text(source)
    if "columns" in info:
        result = {"Schema": result["Schema"], **result}
    elif "rows" in info:
        result = {"Sample": result["Sample"], **result}
    return result
