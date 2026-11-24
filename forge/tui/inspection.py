"""Readable resource details and explicit full-value evidence."""

from collections.abc import Callable, Mapping
from decimal import Decimal
from typing import cast

from rich.console import Group
from rich.table import Table
from rich.text import Text
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, VerticalScroll
from textual.screen import ModalScreen, Screen
from textual.widgets import Button, DataTable, Input, Static

from forge.shared.terminal.palette import CYAN, FG
from forge.tui.clipboard import field_text, plain
from forge.tui.controller import document
from forge.tui.design import SECONDARY
from forge.tui.presentation import entries, status
from forge.tui.screens import Detail, WorkspaceHost

SUMMARY_GROUPS = {
    "Identity": {
        "id",
        "registry_publication_id",
        "engine_publication_id",
        "engine_dataset_id",
        "output_ref",
        "local_id",
        "run_id",
        "instrument_id",
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
    "Recorded outcome": {
        "status",
        "state",
        "verification",
        "registration",
        "integrity",
        "validation",
    },
}
OUTCOME_FIELDS = SUMMARY_GROUPS["Recorded outcome"]


def fields(value: object) -> Group:
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
    sections: list[Table | Text] = []
    for heading, records in groups.items():
        if len(groups) > 1:
            sections.append(Text(heading, f"bold {CYAN}"))
        table = Table.grid(padding=(0, 1))
        table.add_column(style=SECONDARY, max_width=29)
        table.add_column(style=FG, overflow="fold")
        for name, item in records:
            key = name.lower().replace(" ", "_")
            text = field_text(name, item)
            rendered = status(text) if key in OUTCOME_FIELDS else Text(text)
            table.add_row(name.replace("_", " ").capitalize(), rendered)
        sections.append(table)
    return Group(*sections)


class RecordTable(DataTable[Text]):
    def on_mount(self) -> None:
        self.zebra_stripes = True
        self.cursor_foreground_priority = "renderable"


class ActionMenu(ModalScreen[str | None]):
    BINDINGS = [Binding("escape", "back", "Back")]

    def __init__(self, title: str, actions: tuple[str, ...]) -> None:
        super().__init__()
        self.heading, self.actions = title, actions

    def compose(self) -> ComposeResult:
        with VerticalScroll(classes="menu"):
            yield Static(self.heading, classes="section-title", markup=False)
            for index, label in enumerate(self.actions):
                yield Button(label, id=f"menu-{index}", classes="compact")

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
    ) -> None:
        super().__init__()
        self.heading, self.evidence = title, evidence
        self.links = actions or {}
        self.sections = sections or sections_for(evidence)
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
        if self.searchable:
            yield Input(placeholder="Search text, severity, or stage", id="inspection-search")
        with VerticalScroll(id="inspection-scroll"):
            yield Static(id="inspection-fields")
        yield RecordTable(cursor_type="cell", id="inspection-rows")
        yield Static(id="inspection-operation", classes="inspect-operation")
        with Horizontal(classes="screen-footer"):
            yield Button("Esc Back", id="inspect-back", classes="compact")
            yield Button("F2 Actions", id="inspect-actions", classes="compact")
            yield Button("Copy section", id="inspect-copy", classes="compact tertiary")
            yield Button(self.evidence_label, id="inspect-raw", classes="compact")

    def on_mount(self) -> None:
        self.render_section()

    def render_section(self) -> None:
        value = self.sections[self.section]
        self.items = [
            entry
            for entry in entries(value)
            if not self.search or self.search in plain(entry).casefold()
        ]
        scroll = self.query_one("#inspection-scroll")
        rows = self.query_one(RecordTable)
        scroll.display = not bool(self.items)
        rows.display = bool(self.items)
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
        for key in keys:
            label = key.replace("_", " ").title()
            width = max(len(label), *(len(field_text(key, item.get(key))) for item in self.items))
            rows.add_column(label, width=min(40, max(9, width)))
        for item in self.items:
            rows.add_row(
                *(
                    status(field_text(key, item.get(key)))
                    if key.lower() in OUTCOME_FIELDS
                    else Text(
                        field_text(key, item.get(key)),
                        no_wrap=True,
                        overflow="ellipsis",
                        justify="right"
                        if isinstance(item.get(key), (int, float, Decimal))
                        and not isinstance(item.get(key), bool)
                        and not key.lower().endswith("id")
                        else "left",
                    )
                    for key in keys
                )
            )
        if not isinstance(self.focused, Input):
            rows.focus()

    def on_input_changed(self, event: Input.Changed) -> None:
        self.search = event.value.casefold()
        self.render_section()

    def on_data_table_cell_selected(self, event: DataTable.CellSelected) -> None:
        if event.coordinate.row >= len(self.items):
            return
        item = self.items[event.coordinate.row]
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
        else:
            value = event.value
            cast(WorkspaceHost, self).app.push_screen(Detail("Exact value", str(value), raw=True))

    def action_back(self) -> None:
        self.dismiss(None)

    def check_action(self, action: str, parameters: tuple[object, ...]) -> bool | None:
        if action == "copy_section" and isinstance(self.focused, Input):
            return False
        return super().check_action(action, parameters)

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
