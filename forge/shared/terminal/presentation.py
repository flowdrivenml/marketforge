"""Small Rich components for Forge command results."""

from collections.abc import Iterable
from pathlib import Path

from rich.console import Console
from rich.style import Style
from rich.table import Table
from rich.text import Text


def header(console: Console, title: str, *, environment: str = "") -> None:
    heading = Text(f"MarketForge · {title}", style="mf.heading")
    if environment in {"testing", "production"}:
        grid = Table.grid(expand=True)
        grid.width = min(console.width, 110)
        grid.add_column()
        grid.add_column(justify="right", no_wrap=True)
        grid.add_row(heading, Text(environment.upper(), style="mf.identifier"))
        console.print(grid)
    else:
        console.print(heading)
    console.print()


def section(console: Console, title: str) -> None:
    console.print()
    console.print(title, style="mf.heading")


def data_table() -> Table:
    return Table(
        box=None, padding=(0, 1), collapse_padding=True, pad_edge=False, header_style="mf.heading"
    )


def summary(
    console: Console, rows: Iterable[tuple[str, str | Text]], *, numeric: bool = False
) -> None:
    table = Table.grid(padding=(0, 2))
    table.add_column(style="mf.identifier" if not numeric else "mf.body")
    table.add_column(justify="right" if numeric else "left", overflow="fold")
    for label, value in rows:
        table.add_row(label, value if isinstance(value, Text) else Text(value))
    console.print(table)


def status_style(state: str) -> str:
    if state in {"complete", "completed", "registered", "verified", "valid", "prepared", "deleted"}:
        return "mf.success"
    if state in {"failed", "invalid", "blocked"}:
        return "mf.failure"
    return "mf.warning"


def status_label(state: str, *, label: str | None = None) -> Text:
    style = status_style(state)
    mark = {"mf.success": "✓", "mf.failure": "✗", "mf.warning": "!"}[style]
    return Text(f"{mark} {label or state.replace('_', ' ').upper()}", style=style)


def display_path(path: Path, root: Path) -> str:
    return str(path.relative_to(root) if path.is_relative_to(root) else path)


def reference(console: Console, label: str, value: str) -> None:
    console.print(label, style="mf.identifier")
    console.print(Text(f"  {value}"))


def empty_state(console: Console, message: str) -> None:
    console.print(message, style="mf.secondary")


def documentation_link(console: Console, url: str) -> None:
    section(console, "Documentation")
    style = console.get_style("mf.identifier")
    if console.is_terminal:
        style += Style(link=url)
    indent = "  " if len(url) + 2 <= console.width else ""
    console.print(Text(f"{indent}{url}", style=style, overflow="fold"))
