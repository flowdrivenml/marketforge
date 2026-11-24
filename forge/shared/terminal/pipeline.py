"""Render typed Pipeline results using the shared Forge theme."""

from collections.abc import Iterable

from rich.console import Console
from rich.text import Text

from forge.shared.presentation.pipeline import PipelineView, RecipeLine
from forge.shared.terminal.presentation import (
    data_table,
    empty_state,
    header,
    section,
    status_label,
    summary,
)


def table(
    console: Console, columns: tuple[str, ...], rows: Iterable[tuple[str | Text, ...]]
) -> None:
    result = data_table()
    for column in columns:
        result.add_column(column, overflow="fold")
    for row in rows:
        result.add_row(*(Text(cell) if isinstance(cell, str) else cell for cell in row))
    console.print(result)


def pipeline_status(state: str) -> Text:
    if state == "planned":
        return Text("✓ PLANNED", style="mf.success")
    if state in {"enabled", "deferred", "pending"}:
        return Text(state.upper(), style="mf.secondary")
    return status_label(state)


def recipes(console: Console, lines: tuple[RecipeLine, ...], ordering: str) -> None:
    header(console, "Pipeline · Recipes")
    console.print(Text(ordering, style="mf.secondary"))
    if not lines:
        empty_state(console, "No managed pipeline recipes.")
        return
    table(
        console,
        ("Recipe", "Stages", "Requests", "Interval", "Target", "Local structure"),
        (
            (row.name, row.stages, row.requests, row.requested, row.target, status_label(row.state))
            for row in lines
        ),
    )
    for line in lines:
        if line.diagnostic:
            console.print(Text(f"{line.name} · {line.diagnostic}", style="mf.failure"))


def render(console: Console, view: PipelineView) -> None:
    header(console, f"Pipeline · {view.action.title()}", environment=view.environment)
    summary(
        console,
        [
            *view.identity,
            (
                "Processing" if view.action in {"status", "run", "resume"} else "Status",
                pipeline_status(view.state),
            ),
        ],
    )
    if view.stages:
        section(console, "Stages")
        table(
            console,
            ("Stage", "State", "Duration", "Result"),
            (
                (row.name, pipeline_status(row.state), row.duration, row.detail)
                for row in view.stages
            ),
        )
    if view.requests:
        section(console, "Requested sources")
        table(console, ("Exchange", "Market", "Scope", "Data"), view.requests)
    if view.inventory:
        section(console, "Registered raw evidence")
        table(console, ("Scope", "Data", "Recorded complete"), view.inventory)
    if view.outputs:
        section(console, "Outputs")
        for label, identity in view.outputs:
            console.print(Text(label, style="mf.identifier"))
            console.print(Text(f"  {identity}"), soft_wrap=True)
    for title, rows in (
        ("Execution registry", view.index),
        ("Retained evidence", view.artifacts),
    ):
        if rows:
            section(console, title)
            summary(console, rows)
    if view.diagnostics:
        section(console, "Diagnostics")
        for message in view.diagnostics:
            console.print(Text(message, style="mf.warning", overflow="fold"))
    if view.commands:
        section(console, "Inspect and recover")
        for command in view.commands:
            console.print(Text(command), soft_wrap=True)
