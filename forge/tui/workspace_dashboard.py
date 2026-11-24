"""Presentation for the six workspace overview panels and shared host metrics."""

from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from shutil import disk_usage

from rich.table import Table
from rich.text import Text
from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.events import Click
from textual.message import Message
from textual.widgets import Button, Static

from forge.datasets.query import utc
from forge.datasets.workspace_inventory import WorkspaceInventory
from forge.orchestration.run_summary import when
from forge.shared.terminal.palette import BLUE, CYAN, GREEN, MUTED, YELLOW
from forge.tui.design import SECONDARY
from forge.tui.inventory import size_label
from forge.tui.presentation import Cell, Results, cell_text, status

STAGES = ("Raw", "Normalized", "Merged", "Converted")
OPERATIONS = {
    "acquire": "Data Acquisition",
    "normalize": "Normalization",
    "merge": "Merge",
    "convert": "Conversion",
    "pipeline": "Pipeline",
}


def operation(item: Mapping[str, object]) -> str:
    raw = str(item.get("operation") or item.get("kind") or "").removeprefix("historical_")
    prefix = str(item.get("run_id", "")).split("-")[0]
    if raw in {"acquisition", "download"}:
        return "acquire"
    return raw if raw in OPERATIONS else prefix


def outcome(item: Mapping[str, object], *, active: bool = False) -> str:
    state = str(item.get("state") or item.get("status") or "")
    if item.get("registration") == "failed" or item.get("verification") == "failed":
        return "Failed"
    if state in {"failed", "blocked", "interrupted", "cancelled", "partial"}:
        return "Failed"
    if active or state in {"pending", "queued", "awaiting_registration"}:
        return "Pending"
    if state in {"complete", "completed", "success", "registered", "prepared", "stopped"}:
        return "Completed"
    return "—"


def result(value: str) -> Text:
    return status(value)


def stamp(value: object) -> str:
    date = when(value)
    return date.strftime("%m-%d %H:%MZ") if date else "—"


def activity(items: list[dict[str, object]]) -> dict[str, tuple[str, str]]:
    events: dict[str, tuple[datetime, str]] = {}
    for item in items:
        key = operation(item)
        date = when(item.get("finished_at")) or when(item.get("started_at"))
        if key in OPERATIONS and date and (key not in events or date > events[key][0]):
            events[key] = (date, outcome(item, active=item.get("owned_active") is True))
    return {key: (stamp(date.isoformat()), state) for key, (date, state) in events.items()}


def market(kind: object, category: object) -> str:
    parts = list(dict.fromkeys(str(part).title() for part in (kind, category) if part))
    return " · ".join(parts) or "—"


def period(row: Mapping[str, object]) -> str:
    values: list[str] = []
    for key in ("start", "finish"):
        value = row.get(key)
        if isinstance(value, datetime):
            values.append(value.astimezone(UTC).strftime("%Y-%m-%d"))
        elif value is not None:
            try:
                values.append(utc(int(str(value)))[:10])
            except (ValueError, OverflowError, TypeError):
                values.append("—")
    if len(values) == 2 and values[0][:8] == values[1][:8] and values[0] != values[1]:
        return f"{values[0]}→{values[1][8:]}"
    return " → ".join(dict.fromkeys(values)) or "Unknown"


def words(value: object) -> str:
    if value is None:
        return "—"
    if isinstance(value, list):
        return ", ".join(str(item) for item in value)  # pyright: ignore[reportUnknownVariableType, reportUnknownArgumentType]
    return str(value)


def data_kind(value: object) -> str:
    label = words(value)
    return {
        "trade": "Trades",
        "trades": "Trades",
        "l2": "Depth",
        "order_book_l2": "Depth",
        "depth": "Depth",
        "combined": "Trades + Depth",
        "trade_l2": "Trades + Depth",
        "depth, trade": "Trades + Depth",
    }.get(label, label)


def stage_columns(stage: str, row: Mapping[str, object]) -> tuple[Cell, ...]:
    count = words(row.get("file_count"))
    byte_count = row.get("recorded_bytes")
    size = size_label(int(str(byte_count))) if byte_count is not None else "Unknown"
    partitions = Cell("Partitions", words(row.get("partitions")), 10, 1, number=True)
    period_label = Cell("Recorded UTC" if stage == "Raw" else "Observed UTC", period(row), 23)

    if stage in {"Raw", "Normalized"}:
        fields = (
            Cell("Exchange", words(row.get("exchange")), 8),
            Cell("Instrument", words(row.get("symbol") or row.get("instrument") or "Unknown"), 12),
            Cell("Market", market(row.get("instrument_type"), row.get("market_category")), 19),
            Cell("Data", data_kind(row.get("data_type")), 6),
        )
        if stage == "Raw":
            return (
                *fields,
                period_label,
                Cell("Archives", count, 8, 1, number=True),
                Cell("Size", size, 10, 1, number=True),
                Cell("Feed", words(row.get("feed")), 11, 1),
                Cell("Source", words(row.get("source")), 24, 1),
                Cell("State", words(row.get("state")), 10, 1, state=True),
            )
        return (
            *fields,
            period_label,
            Cell("Pub ID", words(row.get("publication")), 8),
            Cell("Files", count, 5, 1, number=True),
            partitions,
            Cell("Size", size, 10, 1, number=True),
            Cell("Stream", words(row.get("stream")), 18, 1),
            Cell("State", words(row.get("state")), 10, 1, state=True),
        )
    members = (
        Cell("Exchanges", words(row.get("exchanges")), 10),
        Cell("Instruments", words(row.get("instruments")), 11, number=True),
        Cell("Data", data_kind(row.get("kinds") or row.get("data_type")), 14),
    )
    identity = (
        Cell("Pub ID", words(row.get("publication")), 8)
        if stage == "Merged"
        else Cell("Output", words(row.get("output_ref")), 12)
    )
    target = (Cell("Target", words(row.get("target_quote")), 6),) if stage == "Converted" else ()
    return (
        *members,
        *target,
        identity,
        period_label,
        partitions,
        Cell("Size", size, 10, 1, number=True),
        Cell("Market", words(row.get("markets")), 18, 1),
        Cell("Files", count, 5, 1, number=True),
        Cell("State", words(row.get("state")), 10, 1, state=True),
    )


def stage_layout(cells: tuple[Cell, ...], width: int) -> tuple[Cell, ...]:
    required = tuple(cell for cell in cells if not cell.priority)
    spare = width - 5 - sum(cell.width + 2 for cell in required)
    widths = {cell.label: cell.width for cell in required}
    for label, minimum in (
        ("Observed UTC", 12),
        ("Recorded UTC", 13),
        ("Instrument", 10),
        ("Pub ID", 6),
        ("Market", 19),
        ("Output", 8),
        ("Data", 7),
        ("Instruments", 11),
    ):
        if spare < 0 and label in widths:
            reduction = max(0, min(-spare, widths[label] - minimum))
            widths[label] -= reduction
            spare += reduction
    result: list[Cell] = []
    for cell in cells:
        if cell.priority:
            if spare < cell.width + 2:
                continue
            spare -= cell.width + 2
        result.append(
            Cell(
                cell.label,
                cell.value,
                widths.get(cell.label, cell.width),
                cell.priority,
                cell.number,
                cell.state,
            )
        )
    return tuple(result)


def stage_cell(cell: Cell, width: int) -> Text:
    value = str(cell.value)
    if cell.label in {"Observed UTC", "Recorded UTC"} and len(value) > width:
        bounds = value.replace(" ", "").split("→")
        if len(bounds) == 2:
            value = "→".join(bound[2:] if len(bound) == 10 else bound for bound in bounds)
            if len(value) > width and bounds[0][:4] == bounds[1][:4]:
                value = "→".join(bound[5:] for bound in bounds)
    return cell_text(replace(cell, value=value))


class OverviewTable(Results):
    """Cell-aware mouse activation with row-based keyboard navigation."""

    class Activated(Message):
        def __init__(self, table_id: str, row: str, column: int) -> None:
            super().__init__()
            self.table_id, self.row, self.column = table_id, row, column

    async def _on_click(self, event: Click) -> None:
        row, column = event.style.meta.get("row"), event.style.meta.get("column")
        if (
            event.button == 1
            and isinstance(row, int)
            and isinstance(column, int)
            and 0 <= row < self.row_count
        ):
            self.move_cursor(row=row)
            key = self.ordered_rows[row].key.value
            if key is not None and self.id is not None:
                self.post_message(self.Activated(self.id, key, column))
            event.stop()
            event.prevent_default()


class WorkspaceDashboard(Vertical):
    def compose(self) -> ComposeResult:
        with Horizontal(classes="workspace-pair"):
            for column in (("universe", "storage", "activity"), ("availability",)):
                with Vertical(classes="workspace-column"):
                    for key in column:
                        yield Static("Loading…", id=f"workspace-{key}", classes="workspace-panel")
        with Vertical(id="workspace-stage", classes="workspace-panel"):
            with Horizontal(id="workspace-stage-tabs"):
                for index, key in enumerate(STAGES):
                    yield Button(key, id=f"workspace-stage-{index}")
            yield Static("", id="workspace-stage-message")
            yield OverviewTable(id="workspace-inventory", cursor_type="row")
        with Vertical(id="workspace-recent", classes="workspace-panel"):
            yield Static("", id="overview-history-note")
            yield OverviewTable(id="overview-results", cursor_type="row")

    def on_mount(self) -> None:
        for key, title in (
            ("universe", "Instrument Universe"),
            ("availability", "Market Data Availability"),
            ("activity", "Recent Activity"),
            ("storage", "Data Storage"),
            ("stage", "Data by Stage"),
            ("recent", "Recent Runs"),
        ):
            self.query_one(f"#workspace-{key}").border_title = title

    def show_summary(self, data: WorkspaceInventory | None, stale: bool) -> None:
        universe = self.query_one("#workspace-universe", Static)
        availability = self.query_one("#workspace-availability", Static)
        storage = self.query_one("#workspace-storage", Static)
        if data is None:
            for widget in (universe, availability, storage):
                widget.update("Unavailable" if stale else "Loading…")
            return
        marker = " · Stale" if stale else ""
        table = Table(
            "Exchange",
            "Spot",
            "Perpetual",
            "Futures",
            "Equity",
            box=None,
            expand=True,
            header_style=f"bold {BLUE}",
        )
        for exchange, spot, perp, future, equity, _ in data.universe:
            table.add_row(str(exchange), str(spot), str(perp), str(future), str(equity))
        other = sum(row[-1] - sum(row[1:5]) for row in data.universe)
        headline = Text.assemble(
            (f"{data.registered:,}", f"bold {CYAN}"),
            " registered · ",
            (f"{data.exchanges:,}", CYAN),
            " exchanges",
            (marker, YELLOW),
        )
        from rich.console import Group

        universe.update(
            Group(
                headline,
                Text(f"{data.with_history:,} with registered historical evidence"),
                table,
                *([Text(f"{other:,} instruments in other types")] if other else []),
            )
        )
        available = Table(
            "Exchange",
            "Market",
            "Trades",
            "Depth",
            box=None,
            expand=True,
            collapse_padding=True,
            pad_edge=False,
            header_style=f"bold {BLUE}",
        )
        for exchange, kind, category, trades, depth in data.availability:
            available.add_row(
                str(exchange),
                market(kind, category),
                Text("✓", GREEN) if trades else Text("—", MUTED),
                Text("✓", GREEN) if depth else Text("—", MUTED),
            )
        availability.update(
            Group(
                Text.assemble(("Recorded presence · gaps may exist", SECONDARY), (marker, YELLOW)),
                available,
            )
            if data.availability
            else Text.assemble(("No eligible records", MUTED), (marker, YELLOW))
        )
        sizes = Table.grid(expand=True)
        sizes.add_column()
        sizes.add_column(justify="right")
        known: list[int] = []
        partial = stale
        for key in STAGES:
            value, missing = data.storage[key]
            if value is not None:
                known.append(value)
            partial |= value is None or bool(missing)
            sizes.add_row(key, size_label(value, missing))
        sizes.add_row(
            Text.assemble("Known recorded subtotal", (" · partial" if partial else "", YELLOW)),
            Text(size_label(sum(known) if known else None), f"bold {CYAN}" if known else MUTED),
        )
        storage.update(sizes)

    def show_activity(self, items: list[dict[str, object]], unavailable: bool) -> None:
        summary = activity(items)
        table = Table(
            "Operation",
            "Last activity",
            "Result",
            box=None,
            expand=True,
            collapse_padding=True,
            pad_edge=False,
            header_style=f"bold {BLUE}",
        )
        for key, label in OPERATIONS.items():
            date, state = summary.get(key, ("Unavailable" if unavailable else "Never", "—"))
            table.add_row(label, date, result(state))
        self.query_one("#workspace-activity", Static).update(table)


@dataclass(frozen=True)
class HostSample:
    cpu: tuple[int, int] | None
    memory: tuple[int, int] | None


def host_sample() -> HostSample:
    cpu = None
    memory = None
    try:
        values = [
            int(value) for value in Path("/proc/stat").read_text().splitlines()[0].split()[1:9]
        ]
        cpu = (sum(values), values[3] + values[4])
    except (OSError, IndexError, ValueError):
        pass
    try:
        entries = dict(
            (line.split(":")[0], int(line.split()[1]))
            for line in Path("/proc/meminfo").read_text().splitlines()
        )
        memory = (
            (entries["MemTotal"] - entries["MemAvailable"]) * 1024,
            entries["MemTotal"] * 1024,
        )
    except (OSError, IndexError, KeyError, ValueError):
        pass
    return HostSample(cpu, memory)


def cpu_percent(previous: tuple[int, int] | None, current: tuple[int, int] | None) -> float | None:
    if previous is None or current is None or current[0] <= previous[0]:
        return None
    return max(0.0, min(100.0, (1 - (current[1] - previous[1]) / (current[0] - previous[0])) * 100))


def capacity(path: Path) -> tuple[int, int]:
    while not path.exists() and path != path.parent:
        path = path.parent
    usage = disk_usage(path)
    return usage.free, usage.total
