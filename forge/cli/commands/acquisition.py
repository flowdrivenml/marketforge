"""Historical acquisition command handlers."""

from __future__ import annotations

import json
from pathlib import Path

from forge.historical.archives.config import load_acquisition
from forge.historical.archives.templates import write_acquisition_template
from forge.shared.config.library import list_config_names
from forge.shared.config.models import PresentationSettings
from forge.shared.terminal.console import create_console
from forge.shared.terminal.editor import edit_file


def validate_acquisition(
    path: Path,
    *,
    as_json: bool = False,
) -> None:
    """Validate a saved acquisition request without network access."""
    configuration = load_acquisition(path)

    if as_json:
        print(
            json.dumps(
                {
                    "schema_version": 1,
                    "validation": "structural",
                    "valid": True,
                    "request_count": len(configuration.requests),
                    "configuration": configuration.model_dump(mode="json"),
                },
                indent=2,
            ),
        )
        return

    console = create_console()
    console.print("MarketForge · Acquisition", style="mf.heading")
    console.print(f"Configuration  {path.expanduser().resolve()}")
    console.print(f"Requests       {len(configuration.requests)}")
    console.print(f"Start          {configuration.interval.start.isoformat()}")
    console.print(f"End            {configuration.interval.end.isoformat()}")
    console.print()
    console.print("Configuration structure valid", style="mf.success")
    console.print("Source support and archive availability have not been checked.")


def new_acquisition(
    path: Path,
    *,
    as_json: bool = False,
) -> None:
    """Create an editable acquisition configuration."""
    destination = write_acquisition_template(path)

    if as_json:
        print(
            json.dumps(
                {
                    "schema_version": 1,
                    "created": True,
                    "configuration_file": str(destination),
                },
                indent=2,
            ),
        )
        return

    console = create_console()
    console.print("Acquisition configuration created", style="mf.success")
    console.print(str(destination), style="mf.identifier")
    console.print("Edit the interval and requests, then run acquisition validate.")


def edit_acquisition(
    path: Path,
    *,
    presentation: PresentationSettings,
) -> None:
    """Edit an acquisition file and validate its saved contents."""
    source = edit_file(path, editor=presentation.editor)
    configuration = load_acquisition(source)

    console = create_console(presentation)
    console.print("Configuration structure valid", style="mf.success")
    console.print(str(source), style="mf.identifier")
    console.print(f"Requests: {len(configuration.requests)}")
    console.print("Source support and archive availability have not been checked.")


def list_acquisition_configs(
    directory: Path,
    *,
    as_json: bool = False,
) -> None:
    """List saved acquisition configurations."""
    names = list_config_names(directory)

    if as_json:
        print(
            json.dumps(
                {
                    "schema_version": 1,
                    "configurations": list(names),
                },
                indent=2,
            ),
        )
        return

    console = create_console()
    console.print("MarketForge · Acquisition configurations", style="mf.heading")

    if not names:
        console.print("No acquisition configurations found.")
        return

    for name in names:
        console.print(name, style="mf.identifier")
