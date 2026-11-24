"""Forms, full-value details and deliberate confirmation for Forge."""

import json
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Protocol, cast

from textual import events, work
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical, VerticalScroll
from textual.screen import ModalScreen
from textual.widget import Widget
from textual.widgets import (
    Button,
    Checkbox,
    Collapsible,
    DirectoryTree,
    Input,
    Label,
    OptionList,
    Select,
    Static,
    TextArea,
)
from textual.widgets.option_list import Option

from forge.historical.archives.storage.files import disk
from forge.shared.errors import MarketForgeError
from forge.tui.actions import Action, Field, Value, Values
from forge.tui.clipboard import deliver, plain, safe_text


class Host(Protocol):
    @property
    def app(self) -> App[None]: ...


class WorkspaceHost(Protocol):
    @property
    def app(self) -> App[None]: ...


def pretty(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, indent=2, default=str)


EntityChoices = tuple[tuple[str, str], ...]
EntitySearch = Callable[[str, str, Values], Awaitable[EntityChoices]]


class Form(ModalScreen[Values | None]):
    BINDINGS = [Binding("escape", "dismiss_form", "Back")]

    def __init__(
        self,
        action: Action,
        preset: Values | None = None,
        choices: tuple[str, ...] = (),
        root: Path | None = None,
        *,
        local: bool = False,
        testing: bool = False,
        locked: frozenset[str] = frozenset(),
        submit_label: str | None = None,
        documentation: Callable[[], object] | None = None,
        open_docs: Callable[[str], object] | None = None,
        validate: Callable[[Values], Awaitable[dict[str, str]]] | None = None,
        search_fields: frozenset[str] = frozenset(),
        entity_search: EntitySearch | None = None,
        multiple_entities: frozenset[str] = frozenset(),
        review: Callable[[Values], str] | None = None,
        commit_label: str = "Save",
        expand: Callable[[Values], tuple[tuple[Field, ...], Values]] | None = None,
    ) -> None:
        super().__init__()
        self.spec = action
        self.preset: Values = preset or {}
        self.choices = choices
        self.root = root or Path.cwd()
        self.local = local
        self.testing = testing
        self.locked = locked
        self.submit_label = submit_label
        self.documentation = documentation
        self.open_docs = open_docs
        self.validator = validate
        self.add_fields = expand
        self.review = review
        self.commit_label = commit_label
        self.search_fields = search_fields
        self.entity_search = entity_search
        self.multiple_entities = multiple_entities
        self.issues: dict[str, str] = {}
        self.baseline: Values = {}
        self.validated: Values | None = None

    def compose(self) -> ComposeResult:
        with Vertical(classes="dialog workflow-form"):
            yield Label(self.spec.key, classes="dialog-title", markup=False)
            with VerticalScroll(classes="fields"):
                if (
                    self.choices
                    and self.spec.fields
                    and self.spec.fields[0].key == "reference"
                    and "reference" not in self.locked
                ):
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
                    if heading == "Advanced Options":
                        with Collapsible(title=heading, collapsed=True):
                            for field in items:
                                yield from self.render_field(field)
                    else:
                        if len(groups) > 1 or any(field.group for field in items):
                            yield Label(heading, classes="form-heading", markup=False)
                        for field in items:
                            yield from self.render_field(field)
                if self.add_fields:
                    yield Button("Add another entry", id="form-add")
            yield Static("", id="form-progress", markup=False)
            yield Static("", id="form-error", markup=False)
            with Horizontal(classes="buttons"):
                yield Button("Back", id="back")
                if self.validator:
                    yield Button("Validate", id="form-validate")
                yield Button(
                    self.submit_label or primary_label(self.spec),
                    id="submit",
                    variant="primary",
                )
                if not self.local:
                    yield Button("Copy command", id="copy-command")
                if self.documentation:
                    yield Button("Docs", id="form-docs")
                yield Button("Issues", id="form-issues")

    def on_mount(self) -> None:
        self.baseline = self.raw_values()
        self.refresh_fields()
        for field in self.query("#field-reference").results(Input):
            field.focus()

    def render_field(self, field: Field) -> ComposeResult:
        field_value = self.preset.get(field.key, field.default)
        with Vertical(classes="form-field", id=f"row-{field.key}"):
            if field.kind != "check":
                with Horizontal(classes="form-label"):
                    yield Label("", id=f"label-{field.key}", markup=False)
                    if field.help:
                        yield Button("?", id=f"help-{field.key}")
                    if (
                        field.key in self.search_fields
                        or field.key.rsplit("--", 1)[-1] in self.search_fields
                    ) and field.key not in self.locked:
                        yield Button("Find", id=f"find-{field.key}")
            if field.key in self.locked:
                yield Input(str(field_value), id=f"field-{field.key}", disabled=True)
            elif field.kind == "check":
                yield Checkbox(field.label, value=bool(field_value), id=f"field-{field.key}")
            elif field.kind == "choice":
                yield Select(
                    [(choice, choice) for choice in field.choices],
                    compact=True,
                    value=str(field_value) if field_value else Select.NULL,
                    id=f"field-{field.key}",
                )
            elif field.key not in self.search_fields and (
                field.key in {"source", "file", "root"}
                or field.key.endswith("--root")
                or any(word in field.label.lower() for word in ("path", "directory"))
            ):
                with Horizontal(classes="path-field"):
                    yield Input(
                        str(field_value), placeholder=field.placeholder, id=f"field-{field.key}"
                    )
                    yield Button("Browse", id=f"browse-{field.key}")
            else:
                yield Input(
                    str(field_value), placeholder=field.placeholder, id=f"field-{field.key}"
                )

            yield Static("", classes="field-error", id=f"error-{field.key}", markup=False)

    def on_input_submitted(self, event: Input.Submitted) -> None:
        self.focus_next()

    def has_pending(self) -> bool:
        return self.raw_values() != self.baseline

    def action_dismiss_form(self) -> None:
        if self.has_pending():

            def finish(discard: bool | None) -> None:
                if discard:
                    self.dismiss(None)

            cast(WorkspaceHost, self).app.push_screen(
                Confirm(
                    "Discard pending changes?",
                    "Back returns to the form. Confirm discards this draft.",
                ),
                finish,
            )
        else:
            self.dismiss(None)

    def _field(self, field: Field) -> Value:
        if field.key in self.locked:
            return self.preset[field.key]
        widget = self.query_one(f"#field-{field.key}")
        if isinstance(widget, Checkbox):
            return widget.value
        if isinstance(widget, Select):
            selection = cast(Select[str], widget).value
            return selection if isinstance(selection, str) else ""
        return self.query_one(f"#field-{field.key}", Input).value.strip()

    def raw_values(self) -> Values:
        return {field.key: self._field(field) for field in self.spec.fields}

    def is_required(self, field: Field, values: Values) -> bool:
        return field.required or bool(field.required_when and field.required_when(values))

    def refresh_fields(self, changed: str = "") -> None:
        values = self.raw_values()
        if changed:
            self.issues.pop(changed, None)
            self.issues.pop("", None)
        required: list[str] = []
        active_issues: list[str] = []
        validated = self.validated is not None and values == self.validated
        for field in self.spec.fields:
            key = field.key
            active = not field.visible_when or field.visible_when(values)
            row = self.query_one(f"#row-{key}")
            row.display = active
            mandatory = self.is_required(field, values)
            if active and mandatory:
                required.append(key)
            if field.kind != "check":
                suffix = (
                    " · read-only" if key in self.locked else " *" if mandatory else " · optional"
                )
                if key not in self.preset and field.default != "" and key not in self.locked:
                    suffix += f" · default {field.default}"
                self.query_one(f"#label-{key}", Label).update(field.label + suffix)
            message = self.issues.get(key, "") if active else ""
            if message:
                active_issues.append(message)
            error = self.query_one(f"#error-{key}", Static)
            error.update(message)
            error.display = bool(message)
            row.set_class(mandatory, "-required")
            row.set_class(values[key] != self.baseline.get(key), "-changed")
            row.set_class(validated and not message, "-validated")
            row.set_class(bool(message), "-invalid")
            row.set_class(active and mandatory and key not in self.locked, "-required")
            row.set_class(
                active and key in self.baseline and values[key] != self.baseline[key], "-changed"
            )
            row.set_class(active and self.validated == values and mandatory, "-validated")
        status = self.query_one("#form-progress", Static)
        missing = sum(values[key] == "" for key in required)
        line = f"* Required · {missing} blank" if required else ""
        if values != self.baseline:
            line += (" · " if line else "") + "Pending changes"
        if validated:
            line = "Configuration validated" + (
                " · Pending changes" if values != self.baseline else ""
            )
        status.update(line)
        status.display = bool(line)
        status.set_class(validated, "-validated")
        count = len(active_issues)
        summary = self.issues.get("", "")
        if count and not self.validator and not summary:
            summary = active_issues[0]
        if count:
            summary = f"{count} field issue{'s' if count != 1 else ''}" + (
                f" · {summary}" if summary else ""
            )
        banner = self.query_one("#form-error", Static)
        banner.update(summary)
        banner.display = bool(summary)
        self.query_one("#form-issues", Button).display = bool(summary)

    def on_descendant_blur(self, event: events.DescendantBlur) -> None:
        if (
            not isinstance(event.widget, Input)
            or not event.widget.id
            or not event.widget.id.startswith("field-")
        ):
            return
        key = event.widget.id.removeprefix("field-")
        field = next((field for field in self.spec.fields if field.key == key), None)
        if field is None or key in self.locked or not self.query_one(f"#row-{key}").display:
            return
        value = self._field(field)
        if key in self.baseline and value == self.baseline[key]:
            return
        try:
            if value == "" and field.required_when and field.required_when(self.raw_values()):
                raise MarketForgeError(f"Supply {field.label.lower()}.")
            field.parse(value)
        except MarketForgeError as error:
            self.issues[key] = str(error)
            self.refresh_fields()

    def on_input_changed(self, event: Input.Changed) -> None:
        self.field_changed(event.input)

    def on_checkbox_changed(self, event: Checkbox.Changed) -> None:
        self.field_changed(event.checkbox)

    def field_changed(self, widget: Widget) -> None:
        if self.is_mounted and widget.id and widget.id.startswith("field-"):
            self.refresh_fields(widget.id.removeprefix("field-"))

    def on_select_changed(self, event: Select.Changed) -> None:
        if event.value is not Select.NULL:
            if event.select.id == "saved":
                self.query_one("#field-reference", Input).value = str(event.value)
            elif event.select.id == "field-catalogue_quote":
                for widget in self.query("#field-quote, #field-target_quote").results(Input):
                    widget.value = str(event.value)
        self.field_changed(event.select)

    def focus_error(self) -> None:
        for field in self.spec.fields:
            if field.key not in self.issues or field.key in self.locked:
                continue
            widget = self.query_one(f"#field-{field.key}")
            for ancestor in widget.ancestors:
                if isinstance(ancestor, Collapsible):
                    ancestor.collapsed = False
            widget.focus()
            break

    async def on_button_pressed(self, event: Button.Pressed) -> None:
        identifier = event.button.id or ""
        app = cast(WorkspaceHost, self).app
        if identifier == "form-issues":
            labels = {field.key: field.label for field in self.spec.fields}
            messages = "\n".join(
                f"{labels.get(key, 'Configuration')}  ·  {message}"
                for key, message in self.issues.items()
            )
            app.push_screen(Detail("Configuration issues", messages, raw=True))
            return
        if identifier == "form-docs" and self.documentation:
            self.documentation()
            return
        if identifier.startswith("help-"):
            from functools import partial

            field = next(field for field in self.spec.fields if field.key == identifier[5:])
            docs = (
                partial(self.open_docs, field.documentation_url)
                if self.open_docs and field.documentation_url
                else self.documentation
            )
            app.push_screen(
                Detail(
                    field.label,
                    field.help,
                    raw=True,
                    actions={"Open documentation": docs} if docs else None,
                )
            )
            return
        if identifier.startswith("find-") and self.entity_search:
            key = identifier[5:]
            field = next(field for field in self.spec.fields if field.key == key)
            widget = self.query_one(f"#field-{key}", Input)
            values = self.raw_values()

            async def search(pattern: str) -> EntityChoices:
                assert self.entity_search is not None
                return await self.entity_search(key, pattern, values)

            def choose_entity(identity: str | None) -> None:
                if identity is not None:
                    if key in self.multiple_entities:
                        selected = [
                            item.strip() for item in widget.value.split(",") if item.strip()
                        ]
                        widget.value = ", ".join(dict.fromkeys([*selected, identity]))
                    else:
                        widget.value = identity
                    widget.focus()

            app.push_screen(
                EntityPicker(
                    field.label, search, "" if key in self.multiple_entities else widget.value
                ),
                choose_entity,
            )
            return
        if identifier.startswith("browse-"):
            field = self.query_one(f"#field-{identifier.removeprefix('browse-')}", Input)

            def choose(path: Path | None) -> None:
                if path is not None:
                    field.value = str(path)

            app.push_screen(PathPicker(self.root), choose)
            return
        if identifier == "back":
            self.action_dismiss_form()
            return
        raw = self.raw_values()
        if identifier == "form-add" and self.add_fields:
            from dataclasses import replace

            fields, preset = self.add_fields(raw)
            self.issues.clear()
            self.spec = replace(self.spec, fields=fields)
            self.preset = preset
            await self.recompose()
            self.refresh_fields()
            first_new = next(
                (field for field in fields if field.key not in raw and field.kind != "check"), None
            )
            if first_new:
                self.query_one(f"#field-{first_new.key}").focus()
            else:
                self.query_one("#form-add", Button).focus()
            return
        self.issues.clear()
        values: Values = {}
        for field in self.spec.fields:
            key = field.key
            if key == "catalogue_quote" or (field.visible_when and not field.visible_when(raw)):
                continue
            try:
                if raw[key] == "" and self.is_required(field, raw):
                    raise MarketForgeError(f"Supply {field.label.lower()}.")
                values[key] = field.parse(raw[key])
            except MarketForgeError as error:
                self.issues[key] = str(error)
        if not self.issues and self.validator and identifier != "copy-command":
            self.issues = await self.validator(values)
            if not self.issues:
                self.validated = raw
        self.refresh_fields()
        if self.issues:
            self.focus_error()
            return
        if identifier == "copy-command":
            app.push_screen(
                Detail(
                    "Equivalent CLI command",
                    self.spec.command(values, testing=self.testing),
                    raw=True,
                )
            )
        elif identifier != "form-validate":
            if self.review:

                def finish(accepted: bool | None) -> None:
                    if accepted:
                        self.dismiss(values)

                app.push_screen(
                    Confirm(
                        f"Review · {self.spec.key}",
                        self.review(values),
                        confirm_label=self.commit_label,
                    ),
                    finish,
                )
            else:
                self.dismiss(values)


class EntityPicker(ModalScreen[str | None]):
    """Search the connected service without substituting an identity."""

    BINDINGS = [Binding("escape", "back", "Back")]

    def __init__(
        self, title: str, search: Callable[[str], Awaitable[EntityChoices]], initial: str
    ) -> None:
        super().__init__()
        self.heading = title
        self.search = search
        self.initial = initial

    def compose(self) -> ComposeResult:
        with Vertical(classes="dialog entity-picker"):
            yield Label(f"Find · {self.heading}", classes="dialog-title", markup=False)
            with Horizontal(classes="path-field"):
                yield Input(
                    self.initial, placeholder="Search registered records", id="entity-query"
                )
                yield Button("Search", id="entity-search")
            yield Static("", id="entity-status", markup=False)
            yield Select[str]([], prompt="Choose an exact record", id="entity-result", compact=True)
            with Horizontal(classes="buttons"):
                yield Button("Back", id="entity-back")
                yield Button("Use selection", id="entity-use", variant="primary", disabled=True)

    def on_mount(self) -> None:
        self.query_one(Input).focus()
        self.load_results()

    @work(exclusive=True, exit_on_error=False)
    async def load_results(self) -> None:
        query = self.query_one(Input)
        status = self.query_one("#entity-status", Static)
        result = cast(Select[str], self.query_one("#entity-result", Select))
        result.set_options([])
        self.query_one("#entity-use", Button).disabled = True
        status.update("Searching…")
        try:
            matches = await self.search(query.value.strip())
        except Exception as error:
            status.update(safe_text(str(error)))
            return
        result.set_options([(safe_text(label), identity) for label, identity in matches])
        status.update(
            f"{len(matches)} results shown · refine the search if needed"
            if matches
            else "No matching records."
        )
        if matches:
            result.focus()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        self.load_results()

    def on_select_changed(self, event: Select.Changed) -> None:
        self.query_one("#entity-use", Button).disabled = event.value is Select.NULL

    def on_button_pressed(self, event: Button.Pressed) -> None:
        match event.button.id:
            case "entity-search":
                self.load_results()
            case "entity-use":
                result = cast(Select[str], self.query_one("#entity-result", Select)).value
                if isinstance(result, str):
                    self.dismiss(result)
            case "entity-back":
                self.action_back()
            case _:
                pass

    def action_back(self) -> None:
        self.dismiss(None)


class StreamOrder(ModalScreen[tuple[str, ...] | None]):
    """Edit explicit stream ranks in the service's reviewed default order."""

    BINDINGS = [
        Binding("escape", "back", "Back"),
        Binding("ctrl+up", "move(-1)", "Move up"),
        Binding("ctrl+down", "move(1)", "Move down"),
    ]

    def __init__(self, entries: EntityChoices) -> None:
        super().__init__()
        self.entries = list(entries)

    def compose(self) -> ComposeResult:
        with Vertical(classes="dialog stream-order"):
            yield Label("Merge · stream ordering", classes="dialog-title")
            yield Static("Engine order · Ctrl+↑ / Ctrl+↓ to move the highlighted stream.")
            yield OptionList(id="ordered-streams")
            with Horizontal(classes="buttons"):
                yield Button("Back", id="order-back")
                yield Button("Move up", id="order-up")
                yield Button("Move down", id="order-down")
                yield Button("Use order", id="order-use", variant="primary")

    def on_mount(self) -> None:
        self.refresh_order(0)
        self.query_one(OptionList).focus()

    def refresh_order(self, index: int) -> None:
        widget = self.query_one(OptionList)
        widget.clear_options()
        widget.add_options(
            Option(f"{rank + 1}. {safe_text(label)}")
            for rank, (label, _) in enumerate(self.entries)
        )
        widget.highlighted = index

    def action_move(self, offset: int) -> None:
        index = self.query_one(OptionList).highlighted
        if index is not None and 0 <= index + offset < len(self.entries):
            self.entries[index], self.entries[index + offset] = (
                self.entries[index + offset],
                self.entries[index],
            )
            self.refresh_order(index + offset)

    def action_back(self) -> None:
        self.dismiss(None)

    def on_button_pressed(self, event: Button.Pressed) -> None:
        key = event.button.id
        if key == "order-use":
            self.dismiss(tuple(identity for _, identity in self.entries))
        elif key == "order-up":
            self.action_move(-1)
        elif key == "order-down":
            self.action_move(1)
        elif key == "order-back":
            self.action_back()


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

    def __init__(
        self,
        title: str,
        explanation: str,
        *,
        destructive: bool = False,
        confirm_label: str = "Confirm",
    ) -> None:
        super().__init__()
        self.title_text = title
        self.explanation = explanation
        self.confirm_label = confirm_label
        self.destructive = destructive

    def compose(self) -> ComposeResult:
        with Vertical(classes="dialog confirmation"):
            yield Label(self.title_text, classes="dialog-title", markup=False)
            with VerticalScroll(classes="confirm-scope"):
                yield Static(safe_text(self.explanation), markup=False)
            with Horizontal(classes="buttons"):
                yield Button("Back", id="back")
                yield Button(
                    self.confirm_label,
                    id="confirm",
                    variant="error" if self.destructive else "primary",
                )

    def action_back(self) -> None:
        self.dismiss(False)

    def on_button_pressed(self, event: Button.Pressed) -> None:
        self.dismiss(event.button.id == "confirm")


class Detail(ModalScreen[None]):
    BINDINGS = [Binding("escape", "back", "Back"), Binding("ctrl+y", "copy", "Copy", priority=True)]

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
        self.value_text = safe_text(str(value)) if raw else plain(value)
        self.actions = actions or {}

    def compose(self) -> ComposeResult:
        with Vertical(classes="dialog detail"):
            yield Label(self.title_text, classes="dialog-title", markup=False)
            text = TextArea(self.value_text, read_only=True, soft_wrap=False, id="detail-text")
            text.styles.height = min(40, max(2, self.value_text.count("\n") + 2))
            yield text
            with Horizontal(classes="buttons"):
                yield Button("Back", id="back")
                yield Button("Copy value", id="copy", classes="tertiary")
                for index, title in enumerate(self.actions):
                    yield Button(title, id=f"detail-{index}")

    def action_back(self) -> None:
        self.dismiss(None)

    @work(exclusive=True, exit_on_error=False)
    async def action_copy(self) -> None:
        value = self.query_one(TextArea).selected_text or self.value_text
        app = cast(WorkspaceHost, self).app
        if len(value.encode("utf-8")) > 74_000:
            self.notify(
                "Clipboard limit is 74,000 bytes. Select a smaller passage or export the log.",
                severity="warning",
            )
            return
        provider = None if app.is_headless else await disk(deliver, value)
        if provider:
            self.notify(f"Copied via {provider}.")
        else:
            app.copy_to_clipboard(value)
            self.notify(
                "OSC 52 sent. If paste fails, select text with your terminal's mouse override."
            )

    def on_button_pressed(self, event: Button.Pressed) -> None:
        identity = event.button.id or ""
        if identity == "back":
            self.dismiss(None)
        elif identity == "copy":
            self.action_copy()
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
            widget.styles.height = min(40, max(2, content.count("\n") + 2))
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
            yield Label(self.title_text, classes="dialog-title", markup=False)
            with Horizontal(classes="buttons"):
                for index, title in enumerate(self.sections):
                    yield Button(title, id=f"run-section-{index}")
            text = TextArea(self.value_text, read_only=True, soft_wrap=False, id="detail-text")
            text.styles.height = min(40, max(2, self.value_text.count("\n") + 2))
            yield text
            with Horizontal(classes="buttons"):
                yield Button("Back", id="back")
                yield Button("Copy value", id="copy", classes="tertiary")
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
        widget = self.query_one(TextArea)
        widget.load_text(self.value_text)
        widget.styles.height = min(40, max(2, self.value_text.count("\n") + 2))


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
            "Historical interval"
            if key in {"start", "end"}
            else "Source"
            if key in {"source", "file", "route", "from_acquisition", "from_normalize"}
            else "Advanced Options"
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
        groups.setdefault(field.group or category, []).append(field)
    advanced = groups.pop("Advanced Options", None)
    if advanced:
        groups["Advanced Options"] = advanced
    return groups
