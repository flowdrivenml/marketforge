"""MarketForge command-line entry point."""

from __future__ import annotations

import argparse
import json
import sys
import tomllib
from collections.abc import Callable, Sequence
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

from pydantic import ValidationError

from forge.cli.commands.acquisition import (
    edit_acquisition,
    execute_acquisition,
    list_acquisition_configs,
    new_acquisition,
    validate_acquisition,
)
from forge.cli.commands.config import show_config
from forge.cli.commands.database import migrate_database, show_database_status
from forge.cli.commands.imports import configure as configure_imports
from forge.cli.commands.maintenance import (
    configure_backup,
    configure_doctor,
    configure_plan,
    configure_runs,
    configure_storage,
)
from forge.cli.commands.markets import configure as configure_markets
from forge.shared.config.library import resolve_config_file
from forge.shared.errors import MarketForgeError
from forge.shared.ownership import WorkspaceOwnership
from forge.shared.terminal.console import create_console
from forge.startup.context import ApplicationContext, build_context


class _Arguments(argparse.Namespace):
    """Typed namespace populated by the argument parser."""

    service_handler: Callable[[argparse.Namespace, ApplicationContext], int] | None = None
    project_root: Path | None = None
    config_file: Path | None = None
    as_json: bool = False
    command: str | None = None
    config_command: str | None = None
    acquisition_command: str | None = None
    configuration_name: str | None = None
    database_command: str | None = None
    testing: bool = False
    refresh: bool = False
    silent: bool = False
    requests_per_second: float | None = None


def _application_version() -> str:
    try:
        return version("marketforge")
    except PackageNotFoundError:
        return "uninstalled"


def build_parser() -> argparse.ArgumentParser:
    """Build the root command parser."""
    parser = argparse.ArgumentParser(
        prog="marketforge",
        description=("Historical market-data acquisition and Rust engine orchestration."),
        allow_abbrev=False,
    )

    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {_application_version()}",
    )
    parser.add_argument(
        "--project-root",
        type=Path,
        help="MarketForge repository root.",
    )
    parser.add_argument(
        "--config",
        dest="config_file",
        type=Path,
        help="Explicit application TOML configuration.",
    )
    parser.add_argument(
        "--json",
        dest="as_json",
        action="store_true",
        help="Emit command results and runtime errors as plain JSON.",
    )

    commands = parser.add_subparsers(dest="command")

    configure_markets(
        commands.add_parser("markets", help="Instrument catalogue and current statistics.")
    )

    configure_storage(
        commands.add_parser(
            "storage", help="Inspect, verify and reconcile managed raw acquisitions."
        )
    )
    configure_runs(
        commands.add_parser("runs", help="Inspect retained and interrupted run artifacts.")
    )
    configure_doctor(
        commands.add_parser(
            "doctor", help="Check Python, testing database and storage prerequisites."
        )
    )

    config_parser = commands.add_parser(
        "config",
        help="Inspect application configuration.",
        allow_abbrev=False,
    )
    config_commands = config_parser.add_subparsers(
        dest="config_command",
        required=True,
    )
    config_commands.add_parser(
        "show",
        help="Show effective application settings.",
        allow_abbrev=False,
    )

    acquisition_parser = commands.add_parser(
        "acquisition",
        help="Manage historical acquisition configurations.",
        allow_abbrev=False,
    )
    acquisition_commands = acquisition_parser.add_subparsers(
        dest="acquisition_command",
        required=True,
    )
    configure_imports(
        acquisition_commands.add_parser(
            "import", help="Import external raw recordings while preserving their source identity."
        )
    )
    configure_plan(
        acquisition_commands.add_parser(
            "plan", help="Plan from registry/cache without network requests."
        )
    )
    acquisition_commands.add_parser(
        "list",
        help="List saved acquisition configurations.",
        allow_abbrev=False,
    )

    for action, description in (
        ("new", "Create an editable acquisition configuration."),
        ("edit", "Edit and validate a saved acquisition configuration."),
        ("validate", "Validate a saved acquisition configuration."),
    ):
        action_parser = acquisition_commands.add_parser(
            action,
            help=description,
            allow_abbrev=False,
        )
        action_parser.add_argument(
            "configuration_name",
            help="Saved configuration name, optionally ending in .toml.",
        )
    for action in ("map", "download"):
        action_parser = acquisition_commands.add_parser(
            action,
            allow_abbrev=False,
            help="Discover archive identities without downloading bodies."
            if action == "map"
            else "Discover and acquire bulk archives; reuse verified local files.",
            description=(
                "Resolve NAME in data/.config/acquisition (optional .toml suffix). "
                "Fresh successful discovery, including empty results, is reused for six hours. "
                "Partial failure returns exit 1 with explicit outcomes in the run report. "
                "Results remain on stdout in silent mode; progress goes to stderr."
            ),
        )
        action_parser.add_argument(
            "configuration_name", help="Saved acquisition configuration name."
        )
        action_parser.add_argument(
            "--testing",
            action="store_true",
            required=True,
            help="Use only the verified marketforge_test database; no production fallback.",
        )
        action_parser.add_argument(
            "--refresh",
            action="store_true",
            help="Force authoritative discovery, superseding overlapping cached coverage.",
        )
        action_parser.add_argument(
            "--requests-per-second",
            type=float,
            help="Positive request rate override, capped by the adapter's upstream budget.",
        )
        action_parser.add_argument(
            "--silent",
            action="store_true",
            help="Suppress progress; retain final results and failures. Global --json emits JSON results.",
        )
    database_parser = commands.add_parser(
        "database",
        help="Manage the application database.",
        allow_abbrev=False,
    )
    database_commands = database_parser.add_subparsers(
        dest="database_command",
        required=True,
    )
    configure_backup(
        database_commands.add_parser(
            "backup",
            help="Back up Forge registry and TOML configuration, separately from data bytes.",
        ),
        restoring=False,
    )
    configure_backup(
        database_commands.add_parser(
            "restore", help="Restore only into a fresh, explicitly disposable testing database."
        ),
        restoring=True,
    )
    migrate_parser = database_commands.add_parser(
        "migrate",
        help="Apply pending database migrations.",
        allow_abbrev=False,
    )
    migrate_parser.add_argument(
        "--testing",
        action="store_true",
        required=True,
        help="Explicitly select the testing database.",
    )
    status_parser = database_commands.add_parser(
        "status",
        help="Inspect database identity and migration history.",
        allow_abbrev=False,
    )
    status_parser.add_argument(
        "--testing",
        action="store_true",
        required=True,
        help="Explicitly select the testing database.",
    )
    return parser


def _validation_message(error: ValidationError) -> str:
    """Format field diagnostics without including submitted input values."""
    messages: list[str] = []

    for detail in error.errors(
        include_input=False,
        include_context=False,
        include_url=False,
    ):
        location = ".".join(str(part) for part in detail["loc"])
        messages.append(f"{location or 'configuration'}: {detail['msg']}")

    return "Invalid configuration:\n" + "\n".join(messages)


def _emit_error(message: str, *, as_json: bool) -> None:
    if as_json:
        print(
            json.dumps(
                {
                    "schema_version": 1,
                    "error": {"message": message},
                },
            ),
            file=sys.stderr,
        )
        return

    create_console(stderr=True).print(
        message,
        style="mf.failure",
    )


def main(argv: Sequence[str] | None = None) -> int:
    """Run the command-line interface."""
    parser = build_parser()
    arguments = _Arguments()
    parser.parse_args(argv, namespace=arguments)

    if arguments.command is None:
        parser.print_help()
        return 0

    try:
        context = build_context(
            project_root=arguments.project_root,
            config_file=arguments.config_file,
        )

        if arguments.service_handler is not None:
            return arguments.service_handler(arguments, context)

        if arguments.command == "acquisition":
            directory = context.paths.acquisition_config_dir

            if arguments.acquisition_command == "list":
                list_acquisition_configs(
                    directory,
                    as_json=arguments.as_json,
                )
                return 0

            if arguments.configuration_name is None:
                parser.error("An acquisition configuration name is required.")

            source = resolve_config_file(
                directory,
                arguments.configuration_name,
            )

            if arguments.acquisition_command in {"map", "download"}:
                return execute_acquisition(
                    context,
                    source,
                    download=arguments.acquisition_command == "download",
                    testing=arguments.testing,
                    refresh=arguments.refresh,
                    requests_per_second=arguments.requests_per_second,
                    silent=arguments.silent,
                    as_json=arguments.as_json,
                )
            if arguments.acquisition_command == "new":
                new_acquisition(source, as_json=arguments.as_json)
            elif arguments.acquisition_command == "validate":
                validate_acquisition(source, as_json=arguments.as_json)
            elif arguments.acquisition_command == "edit":
                if arguments.as_json:
                    parser.error("Interactive editing does not support --json.")

                edit_acquisition(
                    source,
                    presentation=context.settings.presentation,
                )
            else:
                parser.error("Unsupported acquisition command.")

            return 0

        if arguments.command == "config" and arguments.config_command == "show":
            show_config(context, as_json=arguments.as_json)
            return 0
        if arguments.command == "database" and arguments.database_command == "migrate":
            with WorkspaceOwnership(context.paths):
                migrate_database(
                    testing=arguments.testing,
                    presentation=context.settings.presentation,
                    as_json=arguments.as_json,
                )
            return 0

        if arguments.command == "database" and arguments.database_command == "status":
            show_database_status(
                testing=arguments.testing,
                presentation=context.settings.presentation,
                as_json=arguments.as_json,
            )
            return 0

        parser.error("Unsupported command.")
    except KeyboardInterrupt:
        _emit_error(
            "Acquisition cancelled; inspect the run report and rerun to recover.",
            as_json=arguments.as_json,
        )
        return 130
    except tomllib.TOMLDecodeError:
        _emit_error(
            "Invalid TOML configuration; inspect the named file.", as_json=arguments.as_json
        )
        return 2
    except ValidationError as exc:
        _emit_error(
            _validation_message(exc),
            as_json=arguments.as_json,
        )
        return 2
    except (MarketForgeError, OSError, UnicodeError) as exc:
        _emit_error(str(exc), as_json=arguments.as_json)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
