"""Presentation of effective application settings."""

from __future__ import annotations

import json

from forge.startup.context import ApplicationContext


def show_config(
    context: ApplicationContext,
    *,
    as_json: bool = False,
) -> None:
    """Display public settings without modifying application state."""
    payload = context.configuration_report()
    settings_file = payload["settings_file"]
    sources = payload["setting_sources"]
    settings = context.settings
    sections = {
        "storage": settings.storage,
        "presentation": settings.presentation,
        "http": settings.http,
    }

    if as_json:
        print(json.dumps(payload, indent=2))
        return

    from rich.table import Table
    from rich.text import Text

    console = context.console
    console.print("MarketForge · Configuration", style="mf.heading")
    console.print(Text(f"Project root   {context.paths.root}"))
    console.print(Text(f"Settings file  {settings_file or 'Built-in defaults'}"))
    console.print(Text(f"Data root      {context.paths.data_root}"))
    table = Table("Setting", "Value", "Source")
    table.add_row("version", str(settings.version), Text(sources["version"] or "Built-in default"))
    for section, model in sections.items():
        for name, value in model.model_dump().items():
            label = f"{section}.{name}"
            table.add_row(
                label,
                Text(str(value) if value is not None else "Environment / editor fallback"),
                Text(sources[label] or "Built-in default"),
            )
    console.print(table)
