"""Compact Overview inventory panels and retained converter evidence."""

from dataclasses import dataclass
from json import dumps

from rich.text import Text
from textual.app import ComposeResult
from textual.containers import Container, Vertical
from textual.widgets import Static

from forge.datasets.inventory import Inventory, Market
from forge.datasets.query import utc
from forge.tui.controller import document
from forge.tui.presentation import Results


@dataclass(frozen=True)
class Converted:
    records: tuple[dict[str, object], ...]
    checked_reports: int

    @property
    def rows(self) -> int | None:
        counts = [document(item.get("publication")).get("events") for item in self.records]
        if any(not isinstance(count, int) for count in counts):
            return None
        return sum(count for count in counts if isinstance(count, int))


def converted_from_reports(reports: list[dict[str, object]]) -> Converted:
    outputs: dict[str, dict[str, object]] = {}
    for item in reports:
        manifest = document(item.get("publication")).get("manifest")
        if item.get("status") == "complete" and isinstance(manifest, str) and manifest:
            outputs.setdefault(manifest, item)
    return Converted(tuple(outputs.values()), len(reports))


def size_label(size: int | None, missing: int = 0) -> str:
    if size is None:
        return "Unknown"
    value = float(size)
    units = ("B", "KiB", "MiB", "GiB", "TiB", "PiB")
    index = 0
    while value >= 1024 and index < len(units) - 1:
        value /= 1024
        index += 1
    return (
        (f"{value:,.1f}" if index else f"{size:,}")
        + f" {units[index]}"
        + (" partial" if missing else "")
    )


def market_key(item: Market) -> str:
    return dumps((item.namespace, item.exchange, item.identity))


def market_values(item: Market) -> tuple[str, str, str, str]:
    qualifiers = "/".join(dict.fromkeys(filter(None, (item.category, item.instrument_type))))
    symbol = f"{item.symbol} {qualifiers}".strip()
    return (
        f"{item.exchange} · {symbol}",
        "Yes" if item.trades else "—",
        "Yes" if item.depth else "—",
        utc(item.latest)[:16].replace("T", " ") if item.latest is not None else "Unknown",
    )


class InventoryPanels(Vertical):
    def compose(self) -> ComposeResult:
        yield Static("Registered processed data · normalized + merged", id="inventory-scope")
        with Container(id="inventory-stats"):
            for key in ("instruments", "streams", "publications", "bytes"):
                yield Static(id=f"stat-{key}", classes="inventory-stat")
        with Container(id="inventory-body"):
            yield Results(id="inventory-stages", cursor_type="row")
            yield Results(id="inventory-markets", cursor_type="row")

    def on_mount(self) -> None:
        for key, title in (
            ("instruments", "Instruments"),
            ("streams", "Logical streams"),
            ("publications", "Publications"),
            ("bytes", "Primary-file size"),
            ("inventory-stages", "Data by stage"),
            ("inventory-markets", "Markets with data · UTC"),
        ):
            widget = self.query_one(f"#{key if key.startswith('inventory-') else 'stat-' + key}")
            widget.border_title = title

    def render_inventory(
        self,
        inventory: Inventory | None,
        raw: tuple[int, int | None] | None,
        converted: Converted | None,
        registry_state: str,
        raw_state: str,
        converted_failed: bool,
    ) -> None:
        scope = "Registered processed data · normalized + merged"
        if registry_state:
            scope += " · " + registry_state
        if inventory and inventory.unresolved_files:
            scope += f" · {inventory.unresolved_files:,} files lack stream evidence"
        self.query_one("#inventory-scope", Static).update(scope)
        values = (
            (
                (
                    f"{inventory.instruments:,}",
                    f"{inventory.exchanges:,} "
                    + ("exchange" if inventory.exchanges == 1 else "exchanges"),
                ),
                (
                    f"{inventory.streams:,}",
                    f"{inventory.trades:,} trade · {inventory.depth:,} depth"
                    + (
                        f" · {other:,} other"
                        if (other := inventory.streams - inventory.trades - inventory.depth)
                        else ""
                    ),
                ),
                (f"{sum(item.count for item in inventory.stages):,}", "Registry"),
                (
                    size_label(inventory.size, inventory.missing_sizes),
                    f"{inventory.missing_sizes:,} missing"
                    if inventory.missing_sizes
                    else "Recorded",
                ),
            )
            if inventory
            else (("Unavailable" if registry_state else "Checking…", ""),) * 4
        )
        for key, (value, subtitle) in zip(
            ("instruments", "streams", "publications", "bytes"), values, strict=True
        ):
            line = Text(value, style="bold #7dcfff", no_wrap=True, overflow="ellipsis")
            if subtitle:
                line.append((" · " if self.size.width < 90 else "\n") + subtitle, "#c0caf5")
            self.query_one(f"#stat-{key}", Static).update(line)
        stage_table = self.query_one("#inventory-stages", Results)
        stages = {stage.operation: stage for stage in inventory.stages} if inventory else {}
        rows = [
            (
                "Raw",
                f"{raw[0]:,} archives" if raw else raw_state or "Checking…",
                size_label(raw[1]) if raw else "—",
                "Recorded local" + (" · stale" if raw_state == "Stale" else ""),
                "—",
            ),
        ]
        for label, op in (("Normalized", "normalize"), ("Merged", "merge")):
            stage = stages.get(op)
            rows.append(
                (
                    label,
                    f"{stage.count:,} pubs/{stage.files:,} files"
                    if stage
                    else "0 publications"
                    if inventory
                    else registry_state or "Checking…",
                    size_label(stage.size, stage.missing_sizes)
                    if stage
                    else "0 B"
                    if inventory
                    else "—",
                    "Registry",
                    f"{stage.rows:,}" if stage and stage.rows is not None else "—",
                )
            )
        rows.append(
            (
                "Converted",
                f"{len(converted.records):,} known outputs"
                if converted
                else "Unavailable"
                if converted_failed
                else "Checking…",
                "Unknown",
                "Local · stale" if converted_failed and converted else "Local · last 8",
                f"{converted.rows:,}" if converted and converted.rows is not None else "—",
            )
        )
        self.fill(
            stage_table,
            ("Stage", "Inventory", "Recorded size", "Source", "Phys rows"),
            (10, 16, 15, 14, 10),
            tuple((row[0], row) for row in rows),
            required=4,
        )
        markets = self.query_one("#inventory-markets", Results)
        records = (
            tuple((market_key(item), market_values(item)) for item in inventory.markets)
            if inventory
            else ()
        )
        if not records:
            records = (
                (
                    "",
                    (
                        "No recorded markets" if inventory else registry_state or "Checking…",
                        "—",
                        "—",
                        "—",
                    ),
                ),
            )
        self.fill(
            markets,
            ("Market", "Trades", "Depth", "Observed UTC"),
            (21, 6, 5, 17),
            records,
            required=3,
        )
        markets.border_title = "Markets with data · Enter opens publications"

    def fill(
        self,
        table: Results,
        names: tuple[str, ...],
        widths: tuple[int, ...],
        rows: tuple[tuple[str, tuple[str, ...]], ...],
        *,
        required: int,
    ) -> None:
        old = (
            table.coordinate_to_cell_key(table.cursor_coordinate).row_key.value
            if table.row_count
            else None
        )
        table.clear(columns=True)
        table.styles.height = len(rows) + 3
        width = table.size.width or self.size.width or 80
        count = len(names) if sum(widths) + 2 * len(names) + 2 <= width else required
        for index in range(count):
            table.add_column(names[index], width=widths[index])
        for key, row in rows:
            table.add_row(
                *(Text(value, no_wrap=True, overflow="ellipsis") for value in row[:count]),
                key=key or None,
            )
        if old is not None:
            for index, (key, _) in enumerate(rows):
                if key == old:
                    table.move_cursor(row=index, scroll=False)
                    break
