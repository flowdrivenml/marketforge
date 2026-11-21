"""Presentation of retained Forge evidence at terminal widths."""

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal, cast

from rich.text import Text
from textual import events
from textual.message import Message
from textual.widgets import DataTable

from forge.orchestration.run_summary import when
from forge.tui.actions import Action, Values
from forge.tui.controller import document

Kind = Literal["publication", "file", "stream", "archive", "instrument", "recipe", "run", "other"]


@dataclass(frozen=True, slots=True)
class Selection:
    kind: Kind
    identity: str
    reference: str = ""
    exchange: str = ""
    market: str = ""
    symbol: str = ""


def bind(action: Action, selected: Selection) -> Values:
    keys = {field.key for field in action.fields}
    match selected.kind:
        case "file" if action.key.startswith("datasets ") and "id" in keys:
            return {"id": int(selected.identity)}
        case "archive" if action.key.startswith("storage ") and "archive" in keys:
            return {"archive": selected.identity}
        case "instrument" if action.key.startswith("markets "):
            values: Values = {
                "exchanges": selected.exchange,
                "markets": selected.market,
                "instrument": selected.symbol,
                "reference": selected.reference,
            }
            return {key: value for key, value in values.items() if key in keys and value}
        case "run" if action.key.startswith("runs ") or action.key in {
            "pipeline resume",
            "pipeline status",
        }:
            return {"reference": selected.identity}
        case "recipe" | "other" if "reference" in keys:
            return {"reference": selected.reference} if selected.reference else {}
        case _:
            return {}


def entries(value: object) -> list[Mapping[str, object]]:
    return (
        [document(x) for x in cast(list[object], value) if document(x)]
        if isinstance(value, list)
        else []
    )


def display(value: object) -> str:
    if value is None or value == "":
        return "—"
    if isinstance(value, bool):
        return "Yes" if value else "No"
    return str(value)


def compact(value: object) -> str:
    return f"{value:,}" if isinstance(value, int) else display(value)


def date(value: object) -> str:
    if value is None:
        return "—"
    if isinstance(value, int):
        from forge.datasets.query import utc

        return utc(value)[:10]
    return str(value)[:10]


def interval(start: object, end: object) -> str:
    return f"{date(start)} → {date(end)}" if start is not None or end is not None else "—"


def members(row: Mapping[str, object]) -> list[Mapping[str, object]]:
    return entries(row.get("streams") or document(row.get("summary")).get("memberships"))


def membership(rows: list[Mapping[str, object]], key: str) -> str:
    values = list(dict.fromkeys(str(row[key]) for row in rows if row.get(key)))
    return ", ".join(values) if values else "—"


def status(value: object) -> Text:
    label = display(value)
    state = label.removeprefix("Last recorded ").lower()
    color = (
        "#9ece6a"
        if state in {"complete", "verified", "published", "registered", "installed", "ready"}
        else "#f7768e"
        if state in {"failed", "corrupt", "missing", "error"}
        else "#e0af68"
        if state in {"blocked", "partial", "stale", "interrupted", "stopped", "pending"}
        else "#7dcfff"
        if state in {"running", "executing", "interrupting"}
        else "#c0caf5"
    )
    return Text(label, style=color, no_wrap=True, overflow="ellipsis")


@dataclass(frozen=True, slots=True)
class Cell:
    label: str
    value: object
    width: int
    priority: int = 0
    number: bool = False
    state: bool = False


def str_values(value: object) -> tuple[str, ...]:
    return (
        tuple(str(item) for item in cast(tuple[object, ...], value))
        if isinstance(value, tuple)
        else ()
    )


def columns(view: str, name: str, kind: str, state: str, value: object) -> tuple[Cell, ...]:
    r = document(value)
    group = members(r)
    c = Cell
    if view in {"Publications", "Merged", "Normalized"}:
        symbols = membership(group, "symbol")
        identities = {
            (entry.get("exchange"), entry.get("instrument_id"), entry.get("symbol"))
            for entry in group
            if entry.get("symbol")
        }
        if len(symbols) > 23 or len(identities) > len({entry.get("symbol") for entry in group}):
            symbols = f"{len(identities)} instruments"
        venue = membership(group, "exchange")
        if len(venue) > 12:
            venue = f"{len({entry.get('exchange') for entry in group if entry.get('exchange')})} exchanges"
        kinds = {entry.get("kind") or entry.get("data_type") for entry in group}
        recognized = [
            label
            for label, possible in (
                ("Trade", {"trade", "trades"}),
                ("depth", {"depth", "l2", "order_book_l2"}),
            )
            if kinds & possible
        ]
        return (
            c("Instruments", symbols, 23),
            c("Exchange", venue, 12),
            c("Stream kinds", " + ".join(recognized) or None, 13),
            c("Files", r.get("primary_file_count"), 6, 0, True),
            c("Recorded size", r.get("bytes_written"), 13, 0, True),
            c(
                "Observed UTC",
                interval(r.get("first_observed_ns"), r.get("last_observed_ns")),
                24,
                1,
            ),
            c("Publication", r.get("engine_dataset_id"), 20, 1),
            c("Evidence", "Registered", 10, 1, state=True),
        )
    if view == "Primary files":
        return (
            c("Primary file", r.get("primary_file", name), 36),
            c("Content", r.get("data_type", kind), 10),
            c("Evidence", state, 12, state=True),
            c("Instruments", membership(group, "symbol"), 23, 1),
            c(
                "Observed UTC",
                interval(r.get("start_timestamp_ns"), r.get("end_timestamp_ns")),
                25,
                1,
            ),
            c("Rows", document(r.get("summary")).get("rows"), 12, 2, True),
            c("Bytes", document(r.get("summary")).get("size_bytes"), 12, 2, True),
        )
    if view == "Converted":
        return (
            c("Output", name, 24),
            c("State", state, 18, state=True),
            c("Registration", r.get("registration"), 16),
            c("Physical rows", document(r.get("publication")).get("events"), 15, 1, True),
            c("Stream kinds", r.get("content_type"), 16, 1),
            c(
                "Observed UTC",
                interval(r.get("first_observed_ns"), r.get("last_observed_ns")),
                22,
                1,
            ),
            c("Recorded size", r.get("bytes_written"), 13, 1, True),
            c("Manifest", r.get("manifest"), 35, 2),
        )
    if view in {"Raw", "Raw storage"}:
        return (
            c("Scope", r.get("scope_identifier") or r.get("instrument") or name, 28),
            c("Source", r.get("exchange") or r.get("origin"), 12),
            c("Data type", kind, 14),
            c("Local state", state, 12, state=True),
            c("Archive UTC", interval(r.get("start_at"), r.get("end_at")), 25, 1),
            c("Bytes", r.get("bytes_written"), 14, 1, True),
            c("Archive", r.get("relative_path", name), 25, 2),
            c("Origin", r.get("origin"), 12, 2),
        )
    if view == "Instruments" and kind == "Instruments":
        return (
            c("Symbol", r.get("symbol", name), 13),
            c("Exchange", r.get("exchange"), 9),
            c(
                "Type / market",
                f"{display(r.get('instrument_type'))} / {display(r.get('market_category'))}",
                15,
            ),
            c(
                "Observations",
                "Unavailable"
                if r.get("observation_id") is None
                else "Stale"
                if r.get("stale")
                else "Available",
                12,
                state=True,
            ),
            c("Turnover 24h", r.get("turnover_24h"), 16, 1, True),
            c("Native unit", r.get("turnover_denomination"), 11, 1),
            c("Observed UTC", date(r.get("measured_at")), 14, 2),
        )
    if view in {"Retained", "Owned"} or kind == "Retained run":
        active = document(document(r.get("stages")).get(str(r.get("active_stage", ""))))
        info = document(r.get("run_context"))
        start, finish = when(r.get("started_at")), when(r.get("finished_at"))
        elapsed = finish - start if start and finish else None
        return (
            c("Operation", info.get("operation") or r.get("operation") or name, 12),
            c("Exchange", ", ".join(str_values(info.get("exchanges"))) or None, 11),
            c("Scope", ", ".join(str_values(info.get("scope"))) or None, 15),
            c("Outcome", state, 16, state=True),
            c(
                "Result",
                info.get("result")
                or active.get("summary")
                or ("Output recorded" if info.get("output") else None),
                17,
            ),
            c("Recipe", info.get("recipe"), 18, 1),
            c("Started UTC", start.strftime("%m-%d %H:%M") if start else None, 13, 1),
            c("Duration", str(elapsed).split(".")[0] if elapsed is not None else None, 10, 1),
        )
    if view == "Rankings":
        return (
            c("Policy", name, 24),
            c("Currency", r.get("comparison_currency"), 12),
            c("Ranked", r.get("included_count"), 12, number=True),
            c("Published UTC", date(r.get("created_at")), 16),
            c("Universe", r.get("universe_count"), 12, 1, True),
        )
    if view == "Profiles":
        return (
            c("Profile", name, 40),
            c("Format", r.get("format") or r.get("record_format"), 18),
            c("State", state, 13, state=True),
            c("Fingerprint", r.get("sha256"), 18, 1),
        )
    if view == "Engines":
        return (
            c("Target", name, 20),
            c("Installation", state, 16, state=True),
            c("Executable", r.get("path"), 36, 1),
        )
    if r.get("path"):
        return (
            c("Recipe", name, 29),
            c("Workflow", kind, 23),
            c("State", state, 13, state=True),
            c("Requested interval", r.get("interval"), 36, 1),
            c("Source", r.get("path"), 35, 2),
        )
    if r.get("symbol") or r.get("code"):
        return (
            c("Declaration", name, 28),
            c("Exchange", r.get("exchange") or r.get("code"), 14),
            c("Type", r.get("instrument_type") or kind, 16),
            c("State", state, 13, state=True),
            c("Tick size", r.get("tick_size"), 15, 1, True),
        )
    return (c("Resource", name, 29), c("Evidence", kind, 22), c("State", state, 22, state=True))


def visible(cells: tuple[Cell, ...], width: int) -> tuple[Cell, ...]:
    remaining = max(25, width - 7)
    chosen: list[Cell] = []
    for cell in cells:
        if cell.width + 2 <= remaining or (not chosen):
            chosen.append(cell)
            remaining -= cell.width + 2
        elif cell.priority == 0 and remaining >= 8:
            chosen.append(
                Cell(cell.label, cell.value, remaining - 2, cell.priority, cell.number, cell.state)
            )
            remaining = 0
    return tuple(chosen)


class Results(DataTable[Text]):
    """Single-line resource table with keyboard and pointer selection."""

    recipe_links = False

    class ContextRequested(Message):
        def __init__(self, identity: str) -> None:
            super().__init__()
            self.identity = identity

    async def _on_click(self, event: events.Click) -> None:
        metadata = event.style.meta
        row, column = metadata.get("row"), metadata.get("column")
        if isinstance(row, int) and isinstance(column, int) and 0 <= row < self.row_count:
            key = self.ordered_rows[row].key
            if event.button == 3 and key.value is not None:
                self.move_cursor(row=row)
                self.post_message(self.ContextRequested(key.value))
                event.stop()
                return
            if self.recipe_links and event.button == 1:
                self.move_cursor(row=row)
                if self.ordered_columns[column].key.value == "Recipe":
                    self.post_message(self.RowSelected(self, row, key))
                event.stop()
                return

    def mark(self, previous: str | None, selected: str | None) -> None:
        for key in dict.fromkeys(x for x in (previous, selected) if x is not None):
            if any(row.value == key for row in self.rows):
                self.update_cell(key, "mark", Text("›" if key == selected else "", style="#7dcfff"))


def cell_text(cell: Cell) -> Text:
    if cell.state:
        return status(cell.value)
    return Text(
        compact(cell.value) if cell.number else display(cell.value),
        justify="right" if cell.number else "left",
        no_wrap=True,
        overflow="ellipsis",
    )
