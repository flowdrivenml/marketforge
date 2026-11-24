"""Rich catalogue output. All data is supplied by the presentation adapter."""

from pathlib import Path

from rich.console import Console
from rich.text import Text

from forge.shared.presentation.catalog import CatalogView
from forge.shared.terminal.presentation import (
    data_table,
    display_path,
    empty_state,
    header,
    reference,
    section,
    status_label,
    summary,
)


def render(console: Console, view: CatalogView, root: Path, *, ordering: str = "") -> None:
    header(console, f"Catalog · {view.title}", environment=view.environment)
    if ordering:
        console.print(Text(ordering, style="mf.secondary"))
    if view.status:
        summary(console, [("Status", status_label(view.status))])
    if view.source is not None:
        reference(console, "Source declaration", display_path(view.source, root))
    for part in view.sections:
        if not (part.details or part.columns or part.note):
            continue
        if part.title != view.title:
            section(console, part.title)
        if part.details:
            summary(console, part.details)
        if part.columns and part.rows:
            table = data_table()
            for column in part.columns:
                table.add_column(column, overflow="fold")
            for row in part.rows:
                table.add_row(*(Text(value) for value in row))
            console.print(table)
            console.print(Text(f"{len(part.rows):,} displayed", style="mf.secondary"))
        if part.note and not part.rows:
            empty_state(console, part.note)
    if view.inspect:
        reference(console, "Inspect", view.inspect)
