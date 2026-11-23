"""Database migration commands."""

import json
from pathlib import Path

from forge.shared.config.models import PresentationSettings
from forge.shared.database.migrations import migrate_database as apply_migrations
from forge.shared.database.status import inspect_database
from forge.shared.terminal.console import create_console


def migrate_database(
    *,
    testing: bool,
    presentation: PresentationSettings,
    as_json: bool = False,
    project_root: Path | None = None,
) -> None:
    """Apply pending migrations to the explicitly selected database."""
    result = apply_migrations(project_root=project_root, testing=testing)

    if as_json:
        print(
            json.dumps(
                {
                    "schema_version": 1,
                    "database": result.database,
                    "applied": list(result.applied),
                    "already_applied": list(result.already_applied),
                },
                indent=2,
            )
        )
        return

    console = create_console(presentation)
    console.print("MarketForge · Database migrations", style="mf.heading")
    console.print(f"Database: {result.database}", style="mf.body")

    if result.applied:
        versions = ", ".join(str(version) for version in result.applied)
        console.print(f"Applied migrations: {versions}", style="mf.success")
    else:
        console.print("Database is up to date.", style="mf.success")

    if result.already_applied:
        versions = ", ".join(str(version) for version in result.already_applied)
        console.print(f"Previously applied: {versions}", style="mf.secondary")


def show_database_status(
    *,
    testing: bool,
    presentation: PresentationSettings,
    as_json: bool = False,
    project_root: Path | None = None,
) -> None:
    """Display the selected database identity and migration state."""
    status = inspect_database(project_root=project_root, testing=testing)

    if as_json:
        print(
            json.dumps(
                {
                    "schema_version": 1,
                    "database": status.database,
                    "environment": "testing" if testing else "production",
                    "missing_schemas": list(status.missing_schemas),
                    "initialized": status.initialized,
                    "database_id": status.database_id,
                    "purpose": status.purpose,
                    "applied": list(status.applied),
                    "pending": list(status.pending),
                },
                indent=2,
            )
        )
        return

    console = create_console(presentation)
    console.print("MarketForge · Database status", style="mf.heading")
    console.print(f"Database: {status.database}", style="mf.body")

    if status.initialized:
        console.print(
            f"Identity: {status.database_id}",
            style="mf.secondary",
        )
        console.print(f"Purpose: {status.purpose}", style="mf.body")
    else:
        console.print(
            "Forge migration metadata is not initialized.",
            style="mf.warning",
        )

    if status.missing_schemas:
        console.print(f"Missing schemas: {', '.join(status.missing_schemas)}", style="mf.warning")

    applied = ", ".join(str(version) for version in status.applied) or "none"
    console.print(f"Applied migrations: {applied}", style="mf.body")

    if status.pending:
        pending = ", ".join(str(version) for version in status.pending)
        console.print(f"Pending migrations: {pending}", style="mf.warning")
    else:
        console.print("Database is up to date.", style="mf.success")
