"""Historical acquisition command handlers."""

from __future__ import annotations

import asyncio
import json
import math
import shlex
import tomllib
from dataclasses import asdict
from datetime import UTC
from pathlib import Path

from rich.console import Console

from forge.historical.archives.config import load_acquisition
from forge.historical.archives.operation import run_acquisition
from forge.historical.archives.sources.capabilities import source_capabilities
from forge.historical.archives.templates import write_acquisition_template
from forge.shared.config.library import list_config_names
from forge.shared.config.loader import ConfigurationLoadError
from forge.shared.config.models import PresentationSettings
from forge.shared.database.connection import database_dsn
from forge.shared.documentation import ACQUISITION_CONFIGURATION
from forge.shared.errors import MarketForgeError
from forge.shared.ownership import WorkspaceOwnership
from forge.shared.presentation.acquisition import (
    AcquisitionResult,
    ConfigurationResult,
    configuration_inventory,
)
from forge.shared.terminal.acquisition import AcquisitionDisplay
from forge.shared.terminal.acquisition_result import (
    render_inventory,
    render_result,
    render_sources,
    render_validation,
)
from forge.shared.terminal.editor import edit_file
from forge.shared.terminal.presentation import display_path, header, reference, summary
from forge.startup.context import ApplicationContext


def validate_acquisition(
    path: Path,
    *,
    console: Console,
    error_console: Console,
    root: Path,
    as_json: bool = False,
) -> int:
    """Validate a saved acquisition request without network access."""
    try:
        configuration = load_acquisition(path)
    except ConfigurationLoadError as error:
        if as_json:
            raise
        message = str(error)
        if isinstance(error.__cause__, tomllib.TOMLDecodeError):
            message = str(error.__cause__)
        render_validation(
            error_console, ConfigurationResult(path.stem, path, error=message), root=root
        )
        return 2

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
        return 0

    render_validation(console, ConfigurationResult.from_config(path, configuration), root=root)
    return 0


def new_acquisition(
    path: Path,
    *,
    console: Console,
    root: Path,
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

    header(console, "Acquisition Recipe")
    summary(console, (("Created", destination.stem), ("Location", display_path(destination, root))))
    console.print()
    reference(console, "Next", f"mf acquisition edit {shlex.quote(destination.stem)}")
    reference(console, "Configuration reference", ACQUISITION_CONFIGURATION)


def show_acquisition_sources(console: Console, *, as_json: bool) -> None:
    capabilities = source_capabilities()
    if as_json:
        print(
            json.dumps(
                {"schema_version": 1, "capabilities": [asdict(row) for row in capabilities]},
                indent=2,
            )
        )
    else:
        render_sources(console, capabilities)


def edit_acquisition(
    path: Path,
    *,
    presentation: PresentationSettings,
    console: Console,
    error_console: Console,
    root: Path,
) -> int:
    """Edit an acquisition file and validate its saved contents."""
    source = edit_file(path, editor=presentation.editor)
    return validate_acquisition(source, console=console, error_console=error_console, root=root)


def list_acquisition_configs(
    directory: Path,
    *,
    console: Console,
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

    render_inventory(console, configuration_inventory(directory, names))


def execute_acquisition(
    context: ApplicationContext,
    path: Path,
    *,
    download: bool,
    refresh: bool = False,
    requests_per_second: float | None = None,
    silent: bool = False,
    as_json: bool = False,
) -> int:
    """Run mapping/acquisition under the existing historical operation owner."""
    if requests_per_second is not None and (
        not math.isfinite(requests_per_second) or requests_per_second <= 0
    ):
        raise MarketForgeError("--requests-per-second must be finite and positive.")
    configuration = load_acquisition(path)
    dsn = database_dsn(context.paths.root)
    interval = configuration.interval.model_copy(
        update={
            "start": configuration.interval.start.astimezone(UTC),
            "end": configuration.interval.end.astimezone(UTC),
        }
    )
    display = AcquisitionDisplay(
        context.error_console,
        download=download,
        name=path.stem,
        interval=interval,
        silent=silent or as_json or context.settings.presentation.mode == "silent",
    )
    try:
        with WorkspaceOwnership(context.paths), display:
            result = asyncio.run(
                run_acquisition(
                    context,
                    configuration,
                    dsn=dsn,
                    download=download,
                    refresh=refresh,
                    requests_per_second=requests_per_second,
                    silent=silent or as_json or context.settings.presentation.mode == "silent",
                    source_file=path,
                    observer=display.update,
                )
            )
    except KeyboardInterrupt:
        if not as_json:
            snapshot = display.snapshot
            if snapshot is not None and snapshot.report:
                from typing import cast

                from forge.historical.archives.operation import OperationReport
                from forge.orchestration.inspection import read_report

                try:
                    retained = read_report(
                        context.paths.runs_dir, Path(snapshot.report).parent.name
                    )
                    render_result(
                        context.console,
                        AcquisitionResult.from_report(
                            cast(OperationReport, retained),
                            download=download,
                            recipe=path.stem,
                        ),
                        root=context.paths.root,
                    )
                except Exception:
                    context.error_console.print(
                        "Acquisition interrupted. Inspect the retained run.", markup=False
                    )
        raise
    if as_json:
        print(json.dumps(result, indent=2))
    else:
        try:
            render_result(
                context.console,
                AcquisitionResult.from_report(result, download=download, recipe=path.stem),
                root=context.paths.root,
            )
        except Exception:
            # Formatting cannot change an already retained acquisition outcome.
            context.error_console.print(
                f"Acquisition report retained at {result['report']}.", markup=False
            )
    return 1 if result["status"] == "failed" else 0
