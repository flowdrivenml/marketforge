"""Forms, full-value details and deliberate confirmation for Forge."""

import json
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Literal, Protocol, cast

from rich.cells import cell_len
from textual import events, work
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical, VerticalScroll
from textual.content import Content
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

from forge.shared.errors import MarketForgeError
from forge.shared.terminal.palette import YELLOW
from forge.tui.actions import Action, Field, Value, Values
from forge.tui.clipboard import copy_text, plain, safe_text
from forge.tui.design import SECONDARY
from forge.tui.editor_inspection import InspectionTarget, OpenInspection, structured
from forge.tui.interval import combine_timestamp


class FormCheckbox(Checkbox):
    """Keep Textual toggle behavior with an explicit checked glyph."""

    BUTTON_LEFT = "["
    BUTTON_INNER = "✓"
    BUTTON_RIGHT = "]"


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
        expand_label: str = "Add another entry",
        initial_focus: str = "reference",
        historical_interval: bool | None = None,
    ) -> None:
        super().__init__()
        self.spec = action
        self.historical_interval = historical_interval
        if historical_interval is not None:
            from dataclasses import replace

            self.spec = replace(action, fields=(*action.fields, *historical_interval_fields()))
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
        self.expand_label = expand_label
        self.initial_focus = initial_focus
        self.review = review
        self.commit_label = commit_label
        self.search_fields = search_fields
        self.entity_search = entity_search
        self.multiple_entities = multiple_entities
        self.issues: dict[str, str] = {}
        self.baseline: Values = {}
        self.observed_values: Values = {}
        self.validated: Values | None = None
        self.disclosure_states: dict[str, bool] = {}
        self.advanced_required: set[str] = set()

    def compose(self) -> ComposeResult:
        sizing = "dialog-large" if len(self.spec.fields) > 8 else "dialog-compact"
        with Vertical(classes=f"dialog workflow-form {sizing}"):
            yield Label(self.spec.key, classes="dialog-title", markup=False)
            with VerticalScroll(classes="fields dialog-body"):
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
                    yield Select([(x, x) for x in self.choices], id="saved", compact=True)
                yield from self.render_sections()
            with Vertical(classes="form-footer"):
                yield Static("", id="form-progress", markup=False)
                yield Static("", id="form-error", markup=False)
                with Horizontal(classes="buttons"):
                    yield Button("Back", id="back")
                    if self.validator:
                        yield Button("Validate", id="form-validate")
                    if not self.local:
                        yield Button("Copy command", id="copy-command")
                    if self.documentation:
                        yield Button("Docs", id="form-docs")
                    yield Button("Issues", id="form-issues")
                    yield Button(
                        self.submit_label or primary_label(self.spec),
                        id="submit",
                        variant="success"
                        if self.spec.key.endswith((" run", " resume"))
                        else "primary",
                    )

    def render_sections(self) -> ComposeResult:
        groups = grouped_fields(self.spec.fields)
        requests = tuple(
            name
            for name in groups
            if name.startswith("Request ") and name.removeprefix("Request ").isdecimal()
        )
        stages = tuple(
            name
            for name in groups
            if name
            in {
                "Normalization · required stage",
                "Merge · required stage",
                "Conversion · optional stage",
                "Processing options",
            }
        )
        for heading, items in groups.items():
            if heading in requests:
                if heading == requests[0]:
                    with Vertical(classes="form-section"):
                        yield Label("Acquisition requests", classes="form-heading")
                        for index, request in enumerate(requests):
                            key = f"request-{index}"
                            with self.disclosure(request, key, "request-group", index > 0):
                                yield from self.render_fields(self.request_fields(groups[request]))
                        if self.add_fields:
                            yield Button(f"+ {self.expand_label}", id="form-add")
            elif heading in stages and requests:
                if heading == stages[0]:
                    with Vertical(classes="form-section"):
                        yield Label("Processing options", classes="form-heading")
                        for stage in stages:
                            with Vertical(classes="form-stage"):
                                if stage != "Processing options":
                                    yield Label(stage, classes="field-group-title stage-heading")
                                yield from self.render_fields(groups[stage])
            elif heading == "Advanced Options":
                with self.disclosure(heading, "advanced-options", "advanced-group"):
                    yield from self.render_fields(items)
            else:
                with Vertical(classes="form-section"):
                    if len(groups) > 1 or any(field.group for field in items):
                        yield Label(heading, classes="form-heading", markup=False)
                    yield from self.render_fields(items)
        if self.add_fields and not requests:
            yield Button(f"+ {self.expand_label}", id="form-add")

    @staticmethod
    def request_fields(items: list[Field]) -> list[Field]:
        """Pair feed settings without changing the schema's field order."""
        fields = items.copy()
        category = next(
            (
                index
                for index, field in enumerate(fields)
                if field.key.endswith("--market_category")
            ),
            None,
        )
        data = next(
            (index for index, field in enumerate(fields) if field.key.endswith("--data_type")),
            None,
        )
        if category is not None and data is not None and data > category + 1:
            fields.insert(category + 1, fields.pop(data))
        return fields

    def disclosure(self, title: str, key: str, classes: str, collapsed: bool = True) -> Collapsible:
        return Collapsible(
            title=title,
            id=key,
            classes=classes,
            collapsed=self.disclosure_states.get(key, collapsed),
            collapsed_symbol="▸",
            expanded_symbol="▾",
        )

    def set_layout_width(self, width: int) -> None:
        self.set_class(width < 96, "stack-fields")
        self.set_class(width >= 116, "wide-interval")

    def on_resize(self, event: events.Resize) -> None:
        self.set_layout_width(event.size.width)
        if self.focused is not None:
            self.call_after_refresh(self.focused.scroll_visible, animate=False)

    def on_mount(self) -> None:
        self.set_layout_width(cast(WorkspaceHost, self).app.size.width)
        self.baseline = self.raw_values()
        self.observed_values = self.baseline.copy()
        self.refresh_fields()
        preferred = next(iter(self.query(f"#field-{self.initial_focus}")), None)
        if preferred is not None and not preferred.disabled:
            self.reveal_field(preferred)
        else:
            first = next(
                (
                    widget
                    for widget in self.query(".dialog-body Input, .dialog-body Select")
                    if not widget.disabled
                    and all(ancestor.display for ancestor in widget.ancestors)
                    and not any(
                        isinstance(ancestor, Collapsible) and ancestor.collapsed
                        for ancestor in widget.ancestors
                    )
                ),
                None,
            )
            if first is not None:
                first.focus(scroll_visible=False)

    def render_interval_endpoint(self, date: Field, time: Field) -> ComposeResult:
        with Vertical(classes="interval-group"):
            endpoint = (
                "Start · Inclusive"
                if date.key.rsplit("--", 2)[-2].startswith("start")
                else "End · Exclusive"
            )
            yield Label(endpoint, classes="field-group-title")
            with Horizontal(classes="field-pair"):
                yield from self.render_field(date, label="Date")
                yield from self.render_field(time, label="Time UTC")

    def render_fields(self, items: list[Field]) -> ComposeResult:
        index = 0
        while index < len(items):
            field = items[index]
            following = items[index + 1] if index + 1 < len(items) else None
            key = field.key.removesuffix("--date")
            if following and field.key.endswith("--date") and following.key == f"{key}--time":
                date_end, time_end = (
                    items[index + 2 : index + 4] if index + 4 <= len(items) else (None, None)
                )
                if (
                    date_end
                    and time_end
                    and date_end.key
                    in {
                        f"{key.removesuffix('start')}end--date",
                        f"{key.removesuffix('start_ns')}end_ns--date",
                    }
                    and time_end.key == date_end.key.removesuffix("--date") + "--time"
                ):
                    with Horizontal(classes="interval-pair"):
                        yield from self.render_interval_endpoint(field, following)
                        yield from self.render_interval_endpoint(date_end, time_end)
                    index += 4
                else:
                    yield from self.render_interval_endpoint(field, following)
                    index += 2
            elif following and (
                field.key.rsplit("--", 1)[-1],
                following.key.rsplit("--", 1)[-1],
            ) in {
                ("exchange", "instrument_type"),
                ("market_category", "data_type"),
                ("instrument_type", "market_category"),
                ("base_asset", "quote_asset"),
            }:
                with Horizontal(classes="field-pair"):
                    yield from self.render_field(field)
                    yield from self.render_field(following)
                index += 2
            else:
                yield from self.render_field(field)
                index += 1

    def render_field(self, field: Field, *, label: str | None = None) -> ComposeResult:
        field_value = self.preset.get(field.key, field.default)
        with Vertical(classes="form-field", id=f"row-{field.key}"):
            if field.kind != "check":
                with Horizontal(classes="form-label"):
                    yield Label(label or field.label, id=f"label-{field.key}", markup=False)
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
                label = (
                    "Enable request"
                    if field.group.startswith("Request ")
                    and field.key.rsplit("--", 1)[-1].isdecimal()
                    else field.label
                )
                yield FormCheckbox(label, value=bool(field_value), id=f"field-{field.key}")
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
        if changed and values[changed] != self.observed_values.get(changed):
            self.observed_values[changed] = values[changed]
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
                label = (
                    "Date"
                    if key.endswith("--date")
                    else "Time UTC"
                    if key.endswith("--time")
                    else field.label
                )
                self.query_one(f"#label-{key}", Label).update(
                    Content.assemble(
                        label,
                        (suffix, YELLOW if mandatory and key not in self.locked else SECONDARY),
                    )
                )
            message = self.issues.get(key, "") if active else ""
            if message:
                active_issues.append(message)
            error = self.query_one(f"#error-{key}", Static)
            error.update(message)
            error.display = bool(message)
            row.set_class(bool(message), "-invalid")
            row.set_class(active and mandatory and key not in self.locked, "-required")
            row.set_class(
                active and key in self.baseline and values[key] != self.baseline[key], "-changed"
            )
            row.set_class(active and self.validated == values and mandatory, "-validated")
        self.refresh_sections(values, required)
        status = self.query_one("#form-progress", Static)
        missing = sum(values[key] == "" for key in required)
        line = (
            f"{missing} required {'field' if missing == 1 else 'fields'} remaining"
            if missing
            else "Required fields complete"
            if required
            else ""
        )
        if validated:
            line = "Configuration validated"
        pending = (" · " if line else "") + "Pending changes" if values != self.baseline else ""
        status.update(Content.assemble(line, (pending, YELLOW)))
        status.display = bool(line or pending)
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

    def refresh_sections(self, values: Values, required: list[str]) -> None:
        for section in self.query(".form-section, .interval-group"):
            section.display = any(row.display for row in section.query(".form-field")) or bool(
                section.query(".request-group")
            )
        for stage in self.query(".form-stage"):
            headings = stage.query(".stage-heading")
            expanded = sum(row.display for row in stage.query(".form-field")) > 1
            stage.set_class(bool(headings) and not expanded, "compact-stage")
            for heading in headings:
                heading.display = expanded
        for disclosure in self.query(Collapsible):
            rows = disclosure.query(".form-field")
            all_keys = {
                row.id.removeprefix("row-"): row.display for row in rows if row.id is not None
            }
            keys = {key for key, visible in all_keys.items() if visible}
            disclosure.display = bool(keys)
            issues = len(keys.intersection(self.issues))
            modified = any(values[key] != self.baseline.get(key) for key in all_keys)
            if disclosure.id == "advanced-options":
                title = "Advanced Options"
                mandatory = set(keys).intersection(required)
                conditional = {
                    field.key
                    for field in self.spec.fields
                    if field.required_when and field.key in mandatory
                }
                needs_attention = conditional | {key for key in mandatory if values[key] == ""}
                if needs_attention - self.advanced_required:
                    disclosure.collapsed = False
                self.advanced_required = needs_attention
                if mandatory:
                    title += " · Required fields"
            else:
                index = int((disclosure.id or "").removeprefix("request-"))
                prefix = f"requests--{index}"
                enabled = bool(values.get(prefix))
                title = f"Request {index + 1} · {'Enabled' if enabled else 'Disabled'}"
                if enabled:
                    for key in ("exchange", "scope--identifier", "data_type"):
                        value = values.get(f"{prefix}--{key}")
                        if value:
                            title += f" · {safe_text(str(value))}"
                disclosure.set_class(not enabled, "-inactive")
            if issues:
                title += f" · {issues} {'issue' if issues == 1 else 'issues'}"
            if modified:
                title += " · Modified"
            disclosure.title = title
            disclosure.set_class(bool(issues), "-has-issues")

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

    def assemble_interval(self, values: Values) -> None:
        nanoseconds = self.historical_interval
        if nanoseconds is None:
            return
        from datetime import datetime

        bounds: list[int | datetime] = []
        for endpoint in ("start", "end"):
            date = str(values.pop(f"{endpoint}--date", ""))
            time = str(values.pop(f"{endpoint}--time", ""))
            if not date and time == "00:00:00":
                continue
            try:
                stamp = combine_timestamp(date, time, nanoseconds=nanoseconds)
            except ValueError as error:
                self.issues[f"{endpoint}--date"] = str(error)
                continue
            bounds.append(stamp)
            if isinstance(stamp, datetime):
                values[endpoint] = stamp.isoformat().replace("+00:00", "Z")
            else:
                if not -(2**63) <= stamp < 2**63:
                    self.issues[f"{endpoint}--date"] = "Timestamp is outside the Engine range."
                values[f"{endpoint}_ns"] = stamp
        if len(bounds) == 2:
            start, end = bounds
            if (isinstance(start, int) and isinstance(end, int) and start >= end) or (
                isinstance(start, datetime) and isinstance(end, datetime) and start >= end
            ):
                self.issues["end--date"] = "End must be later than start."

    def reveal_field(self, widget: Widget) -> None:
        for ancestor in widget.ancestors:
            if isinstance(ancestor, Collapsible):
                ancestor.collapsed = False
        widget.focus()
        self.call_after_refresh(widget.scroll_visible, animate=False)

    def focus_error(self) -> None:
        for row in self.query(".form-field"):
            if row.id is None or not row.display:
                continue
            key = row.id.removeprefix("row-")
            if key in self.issues and key not in self.locked:
                self.reveal_field(self.query_one(f"#field-{key}"))
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
            self.disclosure_states = {
                disclosure.id: disclosure.collapsed
                for disclosure in self.query(Collapsible)
                if disclosure.id is not None
            }
            self.spec = replace(self.spec, fields=fields)
            self.preset = preset
            self.observed_values.update(
                {
                    field.key: preset.get(field.key, field.default)
                    for field in fields
                    if field.key not in raw
                }
            )
            await self.recompose()
            self.refresh_fields()
            first_new = next(
                (field for field in fields if field.key not in raw and field.kind != "check"), None
            )
            if first_new:
                self.reveal_field(self.query_one(f"#field-{first_new.key}"))
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
        self.assemble_interval(values)
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
        self,
        title: str,
        search: Callable[[str], Awaitable[EntityChoices]],
        initial: str,
        *,
        context: str = "",
        selectable: Callable[[str], bool] | None = None,
    ) -> None:
        super().__init__()
        self.heading = title
        self.search = search
        self.initial = initial
        self.context_text = context
        self.selectable = selectable

    def compose(self) -> ComposeResult:
        with Vertical(classes="dialog entity-picker"):
            yield Label(f"Find · {self.heading}", classes="dialog-title", markup=False)
            if self.context_text:
                yield Static(self.context_text, classes="picker-context", markup=False)
            with Horizontal(classes="path-field"):
                yield Input(
                    self.initial,
                    placeholder="Search recipes"
                    if self.context_text
                    else "Search registered records",
                    id="entity-query",
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
        result.set_options([(Content(safe_text(label)), identity) for label, identity in matches])
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
        self.query_one("#entity-use", Button).disabled = not self.can_use(event.value)

    def can_use(self, value: object) -> bool:
        return isinstance(value, str) and (self.selectable is None or self.selectable(value))

    def on_button_pressed(self, event: Button.Pressed) -> None:
        match event.button.id:
            case "entity-search":
                self.load_results()
            case "entity-use":
                result = cast(Select[str], self.query_one("#entity-result", Select)).value
                if isinstance(result, str) and self.can_use(result):
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
        with Vertical(classes="dialog path-picker"):
            yield Label("Choose a workspace path", classes="dialog-title")
            yield DirectoryTree(self.root_path, id="path-tree")
            yield Static(str(self.selected_path), id="path-selection", markup=False)
            with Horizontal(classes="buttons"):
                yield Button("Back", id="back")
                yield Button("Use selected path", id="use-path", variant="primary")

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
        back_label: str = "Back",
        tone: Literal["", "error"] = "",
        preview: str | None = None,
    ) -> None:
        super().__init__()
        self.title_text = title
        self.explanation = explanation
        self.confirm_label = confirm_label
        self.back_label = back_label
        self.destructive = destructive
        self.tone = "error" if destructive else tone
        self.preview = preview

    def compose(self) -> ComposeResult:
        with Vertical(classes="dialog dialog-compact confirmation"):
            yield Label(
                self.title_text,
                classes="dialog-title status-error" if self.tone else "dialog-title",
                markup=False,
            )
            with VerticalScroll(classes="confirm-scope"):
                yield Static(safe_text(self.explanation), markup=False)
            with Horizontal(classes="buttons"):
                yield Button(self.back_label, id="back")
                if self.preview is not None:
                    yield Button("Review TOML", id="confirm-preview")
                yield Button(
                    self.confirm_label,
                    id="confirm",
                    variant="error" if self.tone else "primary",
                )

    def fit_body(self, width: int, height: int) -> None:
        dialog_width = min(68, width * 96 // 100)
        body_lines = len(Content(safe_text(self.explanation)).wrap(max(1, dialog_width - 6)))
        title_lines = len(Content(self.title_text).wrap(max(1, dialog_width - 6)))
        chrome = min(3, title_lines) + 5
        self.set_class(body_lines + chrome > height * 92 // 100, "long-confirmation")

    def on_resize(self, event: events.Resize) -> None:
        self.fit_body(event.size.width, event.size.height)

    def on_mount(self) -> None:
        viewport = cast(WorkspaceHost, self).app.size
        self.fit_body(viewport.width, viewport.height)
        if self.destructive:
            self.query_one("#back", Button).focus()

    def action_back(self) -> None:
        self.dismiss(False)

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "confirm-preview":
            cast(WorkspaceHost, self).app.push_screen(
                Detail("Candidate TOML", self.preview, raw=True)
            )
        else:
            self.dismiss(event.button.id == "confirm")


class Detail(ModalScreen[None]):
    BINDINGS = [Binding("escape", "back", "Back"), Binding("ctrl+y", "copy", "Copy", priority=True)]

    def __init__(
        self,
        title: str,
        value: object,
        *,
        raw: bool = False,
        error: bool = False,
        actions: dict[str, Callable[[], object]] | None = None,
        editor_target: InspectionTarget | None = None,
    ) -> None:
        super().__init__()
        self.title_text = title
        self.value_text = safe_text(str(value)) if raw else plain(value)
        self.actions = actions or {}
        self.error = error
        self.editor_target = editor_target or (None if raw or error else structured(value))

    def compose(self) -> ComposeResult:
        with Vertical(classes="dialog detail"):
            yield Label(
                self.title_text,
                classes="dialog-title status-error" if self.error else "dialog-title",
                markup=False,
            )
            text = TextArea(self.value_text, read_only=True, soft_wrap=True, id="detail-text")
            text.styles.height = self.detail_height(self.value_text)
            yield text
            yield from self.render_actions()

    def render_actions(self, *, include_editor: bool = True) -> ComposeResult:
        with Horizontal(classes="buttons detail-actions"):
            with Horizontal(classes="action-group"):
                yield Button("Back", id="back")
                yield Button("Copy value", id="copy", classes="tertiary")
                if include_editor and self.editor_target is not None:
                    yield OpenInspection(
                        self.editor_target, "Open in Neovim", id="detail-open-editor"
                    )
            if self.actions:
                with Horizontal(classes="action-group"):
                    for index, title in enumerate(self.actions):
                        yield Button(title, id=f"detail-{index}")

    def detail_height(self, content: str) -> int:
        width = max(16, min(100, cast(WorkspaceHost, self).app.size.width * 96 // 100) - 7)
        lines = sum(max(1, (len(line) + width - 1) // width) for line in content.split("\n"))
        return min(40, max(2, lines + 1))

    def fit_actions(self, width: int) -> None:
        footer_width = min(100, width * 96 // 100) - 4
        buttons = self.query(".detail-actions Button").results(Button)
        required_width = sum(cell_len(str(button.label)) + 3 for button in buttons)
        self.set_class(bool(self.actions) and required_width > footer_width, "wrap-detail-actions")

    def on_mount(self) -> None:
        self.fit_actions(cast(WorkspaceHost, self).app.size.width)

    def on_resize(self, event: events.Resize) -> None:
        self.fit_actions(event.size.width)
        for text in self.query("#detail-text").results(TextArea):
            text.styles.height = self.detail_height(self.value_text)

    def action_back(self) -> None:
        self.dismiss(None)

    @work(exclusive=True, exit_on_error=False)
    async def action_copy(self) -> None:
        value = self.query_one(TextArea).selected_text or self.value_text
        app = cast(WorkspaceHost, self).app
        await copy_text(app, value)

    def on_button_pressed(self, event: Button.Pressed) -> None:
        identity = event.button.id or ""
        if identity == "back":
            self.dismiss(None)
        elif identity == "copy":
            self.action_copy()
        elif identity.startswith("detail-") and identity.removeprefix("detail-").isdigit():
            callback = list(self.actions.values())[int(identity.removeprefix("detail-"))]
            self.dismiss(None)
            cast(WorkspaceHost, self).app.call_later(callback)


class LiveDetail(Detail):
    """An owned operation can be inspected while its service continues."""

    def __init__(self, title: str, snapshot: Callable[[], object]) -> None:
        self.snapshot = snapshot
        super().__init__(title, snapshot())

    def on_mount(self) -> None:
        super().on_mount()
        self.set_interval(0.5, self.reload)

    def reload(self) -> None:
        content = pretty(self.snapshot())
        if content != self.value_text:
            self.value_text = content
            widget = self.query_one(TextArea)
            x, y = widget.scroll_x, widget.scroll_y
            widget.load_text(content)
            widget.styles.height = self.detail_height(content)
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
            with Horizontal(classes="buttons detail-tabs"):
                for index, title in enumerate(self.sections):
                    yield Button(
                        title,
                        id=f"run-section-{index}",
                        classes="run-section primary" if index == 0 else "run-section",
                    )
            text = TextArea(self.value_text, read_only=True, soft_wrap=True, id="detail-text")
            text.styles.height = self.detail_height(self.value_text)
            yield text
            yield from self.render_actions(include_editor=False)

    def on_button_pressed(self, event: Button.Pressed) -> None:
        identity = event.button.id or ""
        if not identity.startswith("run-section-"):
            super().on_button_pressed(event)
            return
        for button in self.query(".run-section").results(Button):
            button.set_class(button is event.button, "primary")
        name = list(self.sections)[int(identity.removeprefix("run-section-"))]
        content = self.sections[name]
        self.value_text = pretty(content) if content else f"No retained {name.lower()} fields."
        widget = self.query_one(TextArea)
        widget.load_text(self.value_text)
        widget.styles.height = self.detail_height(self.value_text)


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


def historical_interval_fields() -> tuple[Field, ...]:
    return tuple(
        Field(
            f"{key}--{part}",
            f"{label} · {part.title()}",
            default="00:00:00" if part == "time" else "",
            group="Historical interval · optional bounds",
            placeholder="YYYY-MM-DD" if part == "date" else "HH:MM:SS",
        )
        for key, label in (("start", "Start UTC (inclusive)"), ("end", "End UTC (exclusive)"))
        for part in ("date", "time")
    )


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
