"""Readable resource details and explicit full-value evidence."""

from collections.abc import Callable, Mapping
from typing import cast

from rich.table import Table
from rich.text import Text
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, VerticalScroll
from textual.screen import ModalScreen, Screen
from textual.widgets import Button, DataTable, Static

from forge.tui.controller import document
from forge.tui.presentation import display, entries
from forge.tui.screens import Detail, WorkspaceHost


def fields(value: object) -> Table:
    table = Table.grid(padding=(0, 1))
    table.add_column(style="#7aa2f7", no_wrap=True)
    table.add_column(style="#c0caf5")
    mapping = document(value)
    if not mapping:
        table.add_row("Value", Text(display(value)))
    for name, item in mapping.items():
        if isinstance(item, (dict, list, tuple)):
            continue
        table.add_row(name.replace("_", " ").capitalize(), Text(display(item)))
    return table


class RecordTable(DataTable[Text]):
    pass


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
    ) -> None:
        super().__init__()
        self.heading, self.evidence = title, evidence
        self.links = actions or {}
        self.sections = sections or sections_for(evidence)
        self.section = section if section in self.sections else next(iter(self.sections))
        self.choose = choose
        self.items: list[Mapping[str, object]] = []

    def compose(self) -> ComposeResult:
        yield Static(self.heading, classes="screen-heading", markup=False)
        with Horizontal(classes="section-tabs"):
            for n, section in enumerate(self.sections):
                yield Button(section, id=f"section-{n}", classes="compact")
        with VerticalScroll(id="inspection-scroll"):
            yield Static(id="inspection-fields")
        yield RecordTable(cursor_type="cell", id="inspection-rows")
        yield Static(id="inspection-operation", classes="inspect-operation")
        with Horizontal(classes="screen-footer"):
            yield Button("Esc Back", id="inspect-back", classes="compact")
            yield Button("F2 Actions", id="inspect-actions", classes="compact")
            yield Button("F3 Raw evidence", id="inspect-raw", classes="compact")

    def on_mount(self) -> None:
        self.call_after_refresh(self.render_section)

    def render_section(self) -> None:
        value = self.sections[self.section]
        self.items = entries(value)
        scroll = self.query_one("#inspection-scroll")
        rows = self.query_one(RecordTable)
        scroll.display = not bool(self.items)
        rows.display = bool(self.items)
        for index, section in enumerate(self.sections):
            self.query_one(f"#section-{index}", Button).set_class(section == self.section, "active")
        if not self.items:
            self.query_one("#inspection-fields", Static).update(
                value if isinstance(value, Text) else fields(value)
            )
            return
        rows.clear(columns=True)
        keys = tuple(dict.fromkeys(key for item in self.items for key in item))
        for key in keys:
            label = key.replace("_", " ").title()
            width = max(len(label), *(len(display(item.get(key))) for item in self.items))
            rows.add_column(label, width=min(40, max(9, width)))
        for item in self.items:
            rows.add_row(*(Text(display(item.get(key)), no_wrap=True) for key in keys))
        rows.focus()

    def on_data_table_cell_selected(self, event: DataTable.CellSelected) -> None:
        if event.coordinate.row >= len(self.items):
            return
        item = self.items[event.coordinate.row]
        if self.choose is not None and self.section in {
            "Files",
            "Streams",
            "Parents",
            "Selected files",
            "Raw inputs",
            "Selected archives",
        }:
            self.choose(self.section, item)
        else:
            value = event.value
            cast(WorkspaceHost, self).app.push_screen(Detail("Exact value", str(value), raw=True))

    def action_back(self) -> None:
        self.dismiss(None)

    def action_evidence(self) -> None:
        cast(WorkspaceHost, self).app.push_screen(Detail("Raw evidence", self.evidence))

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
        ("raw", "Raw inputs"),
        ("inputs", "Inputs"),
        ("archives", "Archives"),
        ("parents", "Parents"),
        ("publication_datasets", "Selected files"),
        ("publication_raw", "Selected archives"),
        ("events", "Activity"),
        ("targets", "Targets"),
        ("snapshots", "Snapshots"),
        ("totals", "Impact"),
        ("targets", "Targets"),
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
