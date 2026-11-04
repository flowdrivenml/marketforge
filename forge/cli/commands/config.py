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
    settings_file = str(context.settings_file) if context.settings_file is not None else None

    # Select public fields explicitly so future secrets are not exposed.
    payload = {
        "schema_version": 1,
        "project_root": str(context.paths.root),
        "settings_file": settings_file,
        "settings": {
            "version": context.settings.version,
            "storage": {
                "data_root": context.settings.storage.data_root,
                "resolved_data_root": str(context.paths.data_root),
            },
            "presentation": {
                "mode": context.settings.presentation.mode,
                "color": context.settings.presentation.color,
                "editor": context.settings.presentation.editor,
            },
        },
        "http": context.settings.http.model_dump(mode="json"),
    }

    if as_json:
        print(json.dumps(payload, indent=2))
        return

    console = context.console

    console.print("MarketForge · Configuration", style="mf.heading")
    console.print()
    console.print(f"Project root   {context.paths.root}")
    console.print(f"Settings file  {settings_file or 'Built-in defaults'}")
    console.print(f"Data root      {context.paths.data_root}")
    console.print(f"Presentation   {context.settings.presentation.mode}")
    console.print(f"Color          {context.settings.presentation.color}")
    console.print(
        f"Editor         {context.settings.presentation.editor or 'VISUAL / EDITOR / vi'}"
    )
    console.print("HTTP defaults", style="mf.heading")
    console.print(
        f"Requests per second: {context.settings.http.requests_per_second}",
        style="mf.body",
    )
    console.print(
        f"Concurrent requests: {context.settings.http.max_concurrent}",
        style="mf.body",
    )
    console.print(
        f"Timeout: {context.settings.http.timeout_seconds} seconds",
        style="mf.body",
    )
    console.print(
        f"Maximum attempts: {context.settings.http.max_attempts}",
        style="mf.body",
    )
