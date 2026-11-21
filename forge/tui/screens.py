"""Forms, full-value details and deliberate confirmation for Forge."""

import json
from collections.abc import Callable
from pathlib import Path
from typing import Protocol, cast

from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical, VerticalScroll
from textual.screen import ModalScreen
from textual.widgets import (
    Button,
    Checkbox,
    Collapsible,
    DirectoryTree,
    Input,
    Label,
    Select,
    Static,
    TextArea,
)

from forge.shared.errors import MarketForgeError
from forge.tui.actions import Action, Field, Value, Values


class Host(Protocol):
    @property
    def app(self) -> App[None]: ...


class WorkspaceHost(Protocol):
    @property
    def app(self) -> App[None]: ...


def pretty(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, indent=2, default=str)


class Form(ModalScreen[Values | None]):
    BINDINGS = [Binding("escape", "dismiss_form", "Back")]

    def __init__(
        self,
        action: Action,
        preset: Values | None = None,
        choices: tuple[str, ...] = (),
        root: Path | None = None,
    ) -> None:
        super().__init__()
        self.spec = action
        self.preset = preset or {}
        self.choices = choices
        self.root = root or Path.cwd()

    def compose(self) -> ComposeResult:
        with Vertical(classes="dialog"):
            yield Label(self.spec.key, classes="dialog-title")
            with VerticalScroll(classes="fields"):
                if self.choices and self.spec.fields and self.spec.fields[0].key == "reference":
                    yield Label(
                        "Retained runs"
                        if self.spec.key
                        in {
                            "pipeline resume",
                            "pipeline status",
                            "runs show",
                            "runs report",
                            "runs logs",
                        }
                        else "Saved recipes"
                    )
                    yield Select([(x, x) for x in self.choices], id="saved")
                groups = grouped_fields(self.spec.fields)
                for heading, items in groups.items():
                    if heading == "Advanced":
                        with Collapsible(title=heading, collapsed=True):
                            for field in items:
                                yield from self.render_field(field)
                    else:
                        for field in items:
                            yield from self.render_field(field)
            yield Static("", id="form-error", markup=False)
            with Horizontal(classes="buttons"):
                yield Button(
                    primary_label(self.spec),
                    id="submit",
                    variant="primary",
                )
                yield Button("Copy command", id="copy-command")
                yield Button("Back", id="back")

    def render_field(self, field: Field) -> ComposeResult:
        field_value = self.preset.get(field.key, field.default)
        with Vertical(classes="form-field"):
            if field.kind != "check":
                yield Label(field.label + (" *" if field.required else ""))
            if field.kind == "check":
                yield Checkbox(field.label, value=bool(field_value), id=f"field-{field.key}")
            elif field.kind == "choice":
                yield Select(
                    [(choice, choice) for choice in field.choices],
                    value=str(field_value) if field_value else Select.NULL,
                    id=f"field-{field.key}",
                )
            elif field.key in {"source", "file"} or any(
                word in field.label.lower() for word in ("path", "directory")
            ):
                with Horizontal(classes="path-field"):
                    yield Input(str(field_value), id=f"field-{field.key}")
                    yield Button("Browse", id=f"browse-{field.key}")
            else:
                yield Input(str(field_value), id=f"field-{field.key}")

    def on_input_submitted(self, event: Input.Submitted) -> None:
        self.query_one("#submit", Button).press()

    def action_dismiss_form(self) -> None:
        self.dismiss(None)

    def _field(self, field: Field) -> Value:
        widget = self.query_one(f"#field-{field.key}")
        if isinstance(widget, Checkbox):
            return widget.value
        if isinstance(widget, Select):
            selection = cast(Select[str], widget).value
            return selection if isinstance(selection, str) else ""
        return self.query_one(f"#field-{field.key}", Input).value.strip()

    def on_select_changed(self, event: Select.Changed) -> None:
        if event.select.id == "saved" and event.value is not Select.NULL:
            self.query_one("#field-reference", Input).value = str(event.value)

    def on_button_pressed(self, event: Button.Pressed) -> None:
        identifier = event.button.id or ""
        if identifier.startswith("browse-"):
            field = self.query_one(f"#field-{identifier.removeprefix('browse-')}", Input)

            def choose(path: Path | None) -> None:
                if path is not None:
                    field.value = str(path)

            cast(WorkspaceHost, self).app.push_screen(PathPicker(self.root), choose)
            return
        if event.button.id == "back":
            self.dismiss(None)
            return
        try:
            values = {field.key: field.parse(self._field(field)) for field in self.spec.fields}
        except MarketForgeError as error:
            self.query_one("#form-error", Static).update(str(error))
            return
        if event.button.id == "copy-command":
            cast(WorkspaceHost, self).app.push_screen(
                Detail("Equivalent CLI command", self.spec.command(values), raw=True)
            )
        else:
            self.dismiss(values)


class PathPicker(ModalScreen[Path | None]):
    BINDINGS = [Binding("escape", "back", "Back")]

    def __init__(self, root: Path) -> None:
        super().__init__()
        self.root_path = root
        self.selected_path = root

    def compose(self) -> ComposeResult:
        with Vertical(classes="dialog detail"):
            yield Label("Choose a workspace path", classes="dialog-title")
            yield DirectoryTree(self.root_path, id="path-tree")
            yield Static(str(self.selected_path), id="path-selection", markup=False)
            with Horizontal(classes="buttons"):
                yield Button("Use selected path", id="use-path")
                yield Button("Back", id="back")

    def on_directory_tree_file_selected(self, event: DirectoryTree.FileSelected) -> None:
        self.dismiss(event.path)

    def on_directory_tree_directory_selected(self, event: DirectoryTree.DirectorySelected) -> None:
        self.selected_path = event.path
        self.query_one("#path-selection", Static).update(str(event.path))

    def action_back(self) -> None:
        self.dismiss(None)

    def on_button_pressed(self, event: Button.Pressed) -> None:
        self.dismiss(self.selected_path if event.button.id == "use-path" else None)


class Confirm(ModalScreen[bool]):
    BINDINGS = [Binding("escape", "back", "Back")]

    def __init__(self, title: str, explanation: str) -> None:
        super().__init__()
        self.title_text = title
        self.explanation = explanation

    def compose(self) -> ComposeResult:
        with Vertical(classes="dialog confirmation"):
            yield Label(self.title_text, classes="dialog-title")
            yield Static(self.explanation, markup=False)
            with Horizontal(classes="buttons"):
                yield Button("Back", id="back")
                yield Button("Confirm", id="confirm", variant="warning")

    def action_back(self) -> None:
        self.dismiss(False)

    def on_button_pressed(self, event: Button.Pressed) -> None:
        self.dismiss(event.button.id == "confirm")


class Detail(ModalScreen[None]):
    BINDINGS = [Binding("escape", "back", "Back")]

    def __init__(
        self,
        title: str,
        value: object,
        *,
        raw: bool = False,
        actions: dict[str, Callable[[], object]] | None = None,
    ) -> None:
        super().__init__()
        self.title_text = title
        self.value_text = str(value) if raw else pretty(value)
        self.actions = actions or {}

    def compose(self) -> ComposeResult:
        with Vertical(classes="dialog detail"):
            yield Label(self.title_text, classes="dialog-title")
            yield TextArea(self.value_text, read_only=True, soft_wrap=False, id="detail-text")
            with Horizontal(classes="buttons"):
                yield Button("Back", id="back")
                yield Button("Copy value", id="copy")
                for index, title in enumerate(self.actions):
                    yield Button(title, id=f"detail-{index}")

    def action_back(self) -> None:
        self.dismiss(None)

    def on_button_pressed(self, event: Button.Pressed) -> None:
        identity = event.button.id or ""
        if identity == "back":
            self.dismiss(None)
        elif identity == "copy":
            text = self.query_one(TextArea).selected_text or self.value_text
            cast(WorkspaceHost, self).app.copy_to_clipboard(text)
            self.notify("Clipboard request sent. Select text here if your terminal blocks it.")
        elif identity.startswith("detail-"):
            callback = list(self.actions.values())[int(identity.removeprefix("detail-"))]
            self.dismiss(None)
            cast(WorkspaceHost, self).app.call_later(callback)


class LiveDetail(Detail):
    """An owned operation can be inspected while its service continues."""

    def __init__(self, title: str, snapshot: Callable[[], object]) -> None:
        self.snapshot = snapshot
        super().__init__(title, snapshot())

    def on_mount(self) -> None:
        self.set_interval(0.5, self.reload)

    def reload(self) -> None:
        content = pretty(self.snapshot())
        if content != self.value_text:
            self.value_text = content
            widget = self.query_one(TextArea)
            x, y = widget.scroll_x, widget.scroll_y
            widget.load_text(content)
            widget.scroll_to(x, y, animate=False, immediate=True)


class RunDetail(Detail):
    """Common report sections backed by the producer's retained evidence."""

    def __init__(
        self, title: str, value: object, *, actions: dict[str, Callable[[], object]] | None = None
    ) -> None:
        from forge.tui.controller import document

        report = document(value)
        result = document(report.get("result"))
        evidence = result if result else report
        self.sections: dict[str, object] = {
            "Overview": {
                key: item
                for key, item in evidence.items()
                if key
                in {
                    "run_id",
                    "execution_id",
                    "status",
                    "state",
                    "environment",
                    "name",
                    "operation",
                    "started_at",
                    "finished_at",
                    "error",
                    "registration",
                }
            },
            "Inputs": {
                key: item
                for key, item in evidence.items()
                if key
                in {
                    "input",
                    "inputs",
                    "source",
                    "interval",
                    "submission",
                    "collection_path",
                }
            },
            "Activity": report.get("activity") or evidence.get("recent") or evidence.get("stages"),
            "Outputs": {
                key: item
                for key, item in evidence.items()
                if key
                in {
                    "catalogue_dataset_ids",
                    "publications",
                    "publication",
                    "dataset",
                    "jobs",
                    "stages",
                }
            },
            "Report": value,
        }
        super().__init__(title, self.sections["Overview"], actions=actions)

    def compose(self) -> ComposeResult:
        with Vertical(classes="dialog detail"):
            yield Label(self.title_text, classes="dialog-title")
            with Horizontal(classes="buttons"):
                for index, title in enumerate(self.sections):
                    yield Button(title, id=f"run-section-{index}")
            yield TextArea(self.value_text, read_only=True, soft_wrap=False, id="detail-text")
            with Horizontal(classes="buttons"):
                yield Button("Back", id="back")
                yield Button("Copy value", id="copy")
                for index, title in enumerate(self.actions):
                    yield Button(title, id=f"detail-{index}")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        identity = event.button.id or ""
        if not identity.startswith("run-section-"):
            super().on_button_pressed(event)
            return
        name = list(self.sections)[int(identity.removeprefix("run-section-"))]
        content = self.sections[name]
        self.value_text = pretty(content) if content else f"No retained {name.lower()} fields."
        self.query_one(TextArea).load_text(self.value_text)


def primary_label(action: Action) -> str:
    if action.effect == "guarded":
        return "Preview selection"
    if action.key in {"pipeline run", "pipeline resume", "pipeline plan", "normalize prepare"}:
        return {
            "pipeline run": "Run pipeline",
            "pipeline resume": "Resume pipeline",
            "pipeline plan": "Plan pipeline",
            "normalize prepare": "Prepare normalization",
        }[action.key]
    word = action.key.rsplit(" ", 1)[-1]
    return {
        "validate": "Validate",
        "refresh": "Refresh selection",
        "verify": "Verify selected input",
    }.get(word, word.capitalize())


def grouped_fields(items: tuple[Field, ...]) -> dict[str, list[Field]]:
    groups: dict[str, list[Field]] = {}
    for field in items:
        key = field.key
        category = (
            "Interval"
            if key in {"start", "end"}
            else "Source"
            if key in {"source", "file", "route", "from_acquisition", "from_normalize"}
            else "Advanced"
            if key
            in {
                "limit",
                "offset",
                "rate",
                "jobs",
                "offline",
                "precise",
                "wide",
                "checksums",
                "refresh",
                "overlap",
                "allow_partial",
            }
            else "Selection"
            if key in {"id", "reference", "archive", "exchanges", "markets", "instrument"}
            else "Options"
        )
        groups.setdefault(category, []).append(field)
    return groups
