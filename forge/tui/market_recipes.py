"""Append exact Markets instruments to existing historical recipes."""

import tomllib
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC
from pathlib import Path
from typing import Literal, cast

from rich.text import Text
from textual import work
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical, VerticalScroll
from textual.content import Content
from textual.screen import ModalScreen
from textual.widgets import Button, Checkbox, Input, Label, Select, Static

from forge.historical.archives.config import AcquisitionConfig, ArchiveRequest, ArchiveScope
from forge.historical.archives.selection import request_key
from forge.historical.archives.sources.base import UnsupportedRequestError
from forge.historical.archives.sources.exchanges import validate_request
from forge.historical.archives.storage.files import disk
from forge.historical.authoring import request_addition
from forge.historical.imports.models import ImportIdentity
from forge.historical.pipelines.config import load_recipe
from forge.shared.config.library import resolve_config_file
from forge.shared.terminal.palette import CYAN, FG, GREEN, MUTED
from forge.tui.clipboard import safe_text
from forge.tui.controller import document
from forge.tui.resources import Row
from forge.tui.screens import FormCheckbox

type Kind = Literal["trades", "order_book_l2"]

DATA_TYPES: tuple[tuple[Kind, str, str], ...] = (
    ("trades", "Trades", "Market executions"),
    ("order_book_l2", "Depth", "Order book data"),
)
LABELS = {kind: label for kind, label, _ in DATA_TYPES}


def market_label(exchange: str, instrument_type: str, category: str) -> str:
    return f"{exchange.title()} · {instrument_type.title()} / {category.title()}"


def preview_label(request: ArchiveRequest) -> str:
    market = market_label(request.exchange, request.instrument_type, request.market_category)
    return f"{request.scope.identifier} · {LABELS[request.data_type]} · {market}"


@dataclass(frozen=True)
class MarketInstrument:
    identity: ImportIdentity
    reference: str

    @property
    def market(self) -> str:
        identity = self.identity
        return market_label(identity.exchange, identity.instrument_type, identity.market_category)

    def request(self, kind: Kind) -> ArchiveRequest:
        identity = self.identity
        request = ArchiveRequest.model_validate(
            {
                "exchange": identity.exchange,
                "instrument_type": identity.instrument_type,
                "market_category": identity.market_category,
                "scope": ArchiveScope(kind="instrument", identifier=identity.symbol),
                "data_type": kind,
            }
        )
        validate_request(request)
        return request


def instrument_from_row(row: Row) -> MarketInstrument | None:
    item = document(row.value)
    if not item.get("id") or not item.get("reference"):
        return None
    try:
        identity = ImportIdentity.model_validate(
            {
                key: item.get(key)
                for key in ("exchange", "instrument_type", "market_category", "symbol")
            }
        )
    except ValueError:
        return None
    return MarketInstrument(identity, str(item["reference"]))


@dataclass(frozen=True)
class Destination:
    name: str
    path: Path
    original: bytes
    count: int
    interval: str


def load_destination(name: str, path: Path, *, pipeline: bool) -> Destination:
    original = path.read_bytes()
    request_addition(original, (), pipeline=pipeline)
    model = (
        load_recipe(original)
        if pipeline
        else AcquisitionConfig.model_validate(tomllib.loads(original.decode()))
    )
    start, end = (value.astimezone(UTC) for value in (model.interval.start, model.interval.end))
    return Destination(
        name,
        path,
        original,
        len(model.requests),
        f"{start.replace(tzinfo=None)} → {end.replace(tzinfo=None)} UTC",
    )


type Save = Callable[[Destination, tuple[ArchiveRequest, ...]], Awaitable[None]]


class MarketRecipeDialog(ModalScreen[tuple[Path, tuple[ArchiveRequest, ...]] | None]):
    BINDINGS = [Binding("escape", "cancel", "Cancel")]

    def __init__(
        self,
        family: str,
        directory: Path,
        names: tuple[str, ...],
        instruments: tuple[MarketInstrument, ...],
        save: Save,
    ) -> None:
        super().__init__()
        self.family = family
        self.directory = directory
        self.names = names
        self.instruments = instruments
        self.save = save
        self.requests: list[ArchiveRequest] = []
        self.blocked: dict[Kind, list[str]] = {kind: [] for kind, _, _ in DATA_TYPES}
        for instrument in instruments:
            for kind, _, _ in DATA_TYPES:
                try:
                    self.requests.append(instrument.request(kind))
                except UnsupportedRequestError as error:
                    self.blocked[kind].append(f"{instrument.reference} — {error}")
                except ValueError:
                    self.blocked[kind].append(
                        f"{instrument.reference} — No instrument request in the archive schema."
                    )
        self.destination: Destination | None = None
        self.additions: tuple[ArchiveRequest, ...] = ()
        self.saving = False

    def compose(self) -> ComposeResult:
        with Vertical(classes="dialog market-recipe"):
            yield Label(f"ADD TO {self.family.upper()}", classes="dialog-title")
            with VerticalScroll(classes="market-recipe-body"):
                multiple = len(self.instruments) > 1
                yield Label(
                    f"SELECTED INSTRUMENTS · {len(self.instruments)}" if multiple else "INSTRUMENT",
                    classes="section-title",
                )
                for instrument in self.instruments:
                    identity = Text(safe_text(instrument.identity.symbol), style=CYAN)
                    identity.append(" · " if multiple else "\n")
                    identity.append(safe_text(instrument.market), style=FG)
                    summary = Static(identity, classes="market-identity")
                    summary.tooltip = safe_text(instrument.reference)
                    yield summary
                yield Label("DESTINATION RECIPE", classes="section-title")
                yield Input(
                    placeholder="Search existing recipes", id="market-recipe-search", compact=True
                )
                yield Select[str](
                    self.recipe_options(""),
                    prompt=f"Select existing {self.family.title()} recipe",
                    id="market-recipe-target",
                    compact=True,
                )
                yield Static("", id="market-recipe-destination", markup=False)
                yield Label("DATA TYPES", classes="section-title")
                with Horizontal(classes="market-kinds"):
                    for kind, label, description in DATA_TYPES:
                        with Vertical(classes="market-kind"):
                            blocked = bool(self.blocked[kind])
                            yield FormCheckbox(label, id=kind, disabled=blocked, compact=True)
                            yield Static(description, classes="market-kind-description")
                            yield Static(
                                "Unsupported" if blocked else "Adapter supported",
                                classes="market-kind-status "
                                + ("market-warning" if blocked else "market-supported"),
                            )
                for kind, label, _ in DATA_TYPES:
                    for reason in self.blocked[kind]:
                        yield Static(
                            f"{label} · {safe_text(reason)}", classes="market-warning", markup=False
                        )
                yield Static(
                    "Archive availability · Not verified",
                    classes="market-kind-status",
                    markup=False,
                )
                yield Label("REQUEST PREVIEW", classes="section-title")
                yield Static("", id="market-recipe-preview")
                yield Static("", id="form-error", markup=False)
            with Horizontal(classes="buttons"):
                yield Button("Cancel", id="market-recipe-cancel")
                yield Button(
                    "Add to Recipe", id="market-recipe-save", variant="primary", disabled=True
                )

    def recipe_options(self, term: str) -> list[tuple[Content, str]]:
        return [(Content(safe_text(name)), name) for name in self.names if term in name.casefold()]

    def target(self) -> Select[str]:
        return cast(Select[str], self.query_one("#market-recipe-target", Select))

    def on_mount(self) -> None:
        self.query_one(Input).focus()
        self.query_one("#market-recipe-destination", Static).display = False
        self.update_preview()

    def on_input_changed(self, event: Input.Changed) -> None:
        target = self.target()
        previous = target.value
        options = self.recipe_options(event.value.strip().casefold())
        if isinstance(previous, str) and any(name == previous for _, name in options):
            # Keep the reviewed snapshot while filtering around the same recipe.
            target.set_options(options)
            target.value = previous
        else:
            target.set_options(options)

    def on_input_submitted(self) -> None:
        self.target().focus()

    def on_select_changed(self, event: Select.Changed) -> None:
        if event.value != self.target().value:
            return
        if self.destination is not None and event.value == self.destination.name:
            return
        self.destination = None
        self.update_preview()
        self.load_recipe(str(event.value) if isinstance(event.value, str) else None)

    @work(exclusive=True, exit_on_error=False)
    async def load_recipe(self, name: str | None) -> None:
        status = self.query_one("#market-recipe-destination", Static)
        status.display = name is not None
        status.remove_class("market-warning")
        status.update("Loading recipe…" if name else "")
        if name is None:
            return
        try:
            path = resolve_config_file(self.directory, name)
            destination = await disk(
                load_destination, name, path, pipeline=self.family == "pipeline"
            )
        except Exception as error:
            if self.target().value == name:
                status.update(f"Incompatible · {safe_text(str(error))}")
                status.add_class("market-warning")
            return
        if self.target().value != name:
            return
        self.destination = destination
        count = destination.count
        status.update(
            f"{safe_text(str(path))}\n{count} existing {'request' if count == 1 else 'requests'}"
            " · Compatible\n"
            f"Historical interval · {destination.interval} (exclusive end)"
        )
        self.update_preview()

    def selected_requests(self) -> tuple[ArchiveRequest, ...]:
        kinds = {
            kind
            for kind, _, _ in DATA_TYPES
            if not self.blocked[kind] and self.query_one(f"#{kind}", Checkbox).value
        }
        return tuple(
            {
                request_key(request): request
                for request in self.requests
                if request.data_type in kinds
            }.values()
        )

    def on_checkbox_changed(self) -> None:
        self.update_preview()

    def update_preview(self) -> None:
        preview = self.query_one("#market-recipe-preview", Static)
        error = self.query_one("#form-error", Static)
        error.display = False
        save = self.query_one("#market-recipe-save", Button)
        save.disabled = True
        self.additions = ()
        options = self.selected_requests()
        if self.destination is None:
            preview.update("Choose an existing recipe.")
            return
        if not options:
            preview.update("Select at least one supported data type.")
            return
        try:
            _, additions = request_addition(
                self.destination.original,
                options,
                pipeline=self.family == "pipeline",
            )
        except Exception as exc:
            preview.update("The selected requests could not be added.")
            error.update(safe_text(str(exc)))
            error.display = True
            return
        self.additions = additions
        new_keys = {request_key(request) for request in additions}
        lines = Text()
        for request in options:
            new = request_key(request) in new_keys
            lines.append(
                f"{'+' if new else '='} {safe_text(preview_label(request))}"
                f"{'' if new else ' · Already included'}\n",
                style=GREEN if new else MUTED,
            )
        if additions:
            lines.append(
                f"{len(additions)} {'request' if len(additions) == 1 else 'requests'} to add"
            )
        else:
            lines.append("All selected requests are already included. No changes required.")
        preview.update(lines)
        save.disabled = self.saving or not additions

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "market-recipe-cancel":
            self.action_cancel()
        elif (
            event.button.id == "market-recipe-save"
            and self.destination
            and self.additions
            and not self.saving
        ):
            self.saving = True
            self.query_one(VerticalScroll).disabled = True
            event.button.disabled = True
            self.query_one("#market-recipe-cancel", Button).disabled = True
            self.persist(self.destination, self.additions)

    @work(exit_on_error=False)
    async def persist(
        self, destination: Destination, additions: tuple[ArchiveRequest, ...]
    ) -> None:
        try:
            await self.save(destination, additions)
        except Exception as error:
            self.saving = False
            self.query_one(VerticalScroll).disabled = False
            self.query_one("#market-recipe-cancel", Button).disabled = False
            self.query_one("#market-recipe-save", Button).disabled = False
            status = self.query_one("#form-error", Static)
            status.update(safe_text(str(error)))
            status.display = True
            self.call_after_refresh(status.scroll_visible, immediate=True)
        else:
            self.dismiss((destination.path, additions))

    def action_cancel(self) -> None:
        if not self.saving:
            self.dismiss(None)
