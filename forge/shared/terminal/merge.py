"""Terminal views for managed merge commands."""

from pathlib import Path

from rich.console import Console
from rich.text import Text

from forge.shared.presentation.merge import LIMIT, MergeView, RecipeView
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


def printable(value: str, *, multiline: bool = False) -> str:
    return "".join(
        character
        if character.isprintable() or (multiline and character == "\n")
        else ascii(character)[1:-1]
        for character in value
    )


def table(console: Console, headings: tuple[str, ...], rows: tuple[tuple[str, ...], ...]) -> None:
    display = data_table()
    for heading in headings:
        display.add_column(heading, overflow="fold")
    for row in rows[:LIMIT]:
        display.add_row(*(Text(printable(value)) for value in row))
    console.print(display)
    if len(rows) > LIMIT:
        console.print(
            f"Showing {LIMIT:,} of {len(rows):,} entries. Use --json for the complete result.",
            style="mf.secondary",
        )


def render_inventory(console: Console, items: list[RecipeView]) -> None:
    header(console, "Merge · Recipes")
    if not items:
        empty_state(console, "No managed merge recipes. Create one with mf merge new NAME.")
        return
    table(
        console,
        ("Recipe", "Form", "Requested interval (UTC)", "State"),
        tuple(
            (item.name, item.form, item.period, "Valid" if item.recipe else "Invalid")
            for item in items
        ),
    )


def render_recipe(console: Console, view: RecipeView, *, action: str, root: Path) -> None:
    title = {"new": "Recipe Created", "edit": "Recipe Edited", "validate": "Recipe Validation"}.get(
        action, "Recipe"
    )
    header(console, f"Merge · {title}")
    summary(
        console,
        (
            ("Recipe", printable(view.name)),
            ("Configuration", printable(display_path(view.path, root))),
            ("Structure", status_label("valid" if view.recipe else "invalid")),
            *((label, printable(value)) for label, value in view.details),
        ),
    )
    if view.error:
        console.print(Text(printable(view.error, multiline=True), style="mf.failure"))
    else:
        console.print(
            "Preparation resolves registry inputs and checks the generated job.",
            style="mf.secondary",
        )


def render_merge(console: Console, view: MergeView, *, root: Path) -> None:
    header(console, f"Merge · {view.title}", environment=view.environment)
    state = "complete" if view.status == "template_created" else view.status
    summary(
        console,
        (
            ("Status", status_label(state, label=view.status.replace("_", " ").upper())),
            *(
                (label, status_label(value, label=value.replace("_", " ").upper()))
                if label in {"Execution", "Physical verification", "Registration", "Structure"}
                else (label, printable(value))
                for label, value in view.details
            ),
        ),
    )
    if view.selection:
        section(console, "Selection")
        summary(console, ((label, printable(value)) for label, value in view.selection))
    if view.streams:
        section(console, "Selected Streams")
        table(console, ("Rank", "Stream ID", "Type", "Files"), view.streams)
    if view.output:
        section(console, "Published Output")
        summary(console, ((label, printable(value)) for label, value in view.output))
    if view.registration or view.files:
        section(console, "Registration")
        if view.registration:
            summary(console, view.registration)
        if view.files:
            table(console, ("Registry file ID", "State"), view.files)
    if view.issues:
        section(console, "Diagnostics")
        for item in view.issues[:LIMIT]:
            console.print(Text(printable(item, multiline=True), style="mf.warning"))
        if len(view.issues) > LIMIT:
            console.print("See the retained report or --json for remaining diagnostics.")
    if view.artifacts:
        section(console, "Artifacts")
        summary(
            console,
            ((label, printable(display_path(Path(path), root))) for label, path in view.artifacts),
        )
    if view.next_action:
        console.print()
        reference(console, "Inspect or recover", printable(view.next_action))
