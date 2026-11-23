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
    show_acquisition_sources,
    validate_acquisition,
)
from forge.cli.commands.catalog import configure as configure_catalog
from forge.cli.commands.config import show_config
from forge.cli.commands.conversion import configure as configure_conversion
from forge.cli.commands.database import migrate_database, show_database_status
from forge.cli.commands.datasets import configure as configure_datasets
from forge.cli.commands.engine import configure as configure_engine
from forge.cli.commands.imports import configure as configure_imports
from forge.cli.commands.initialization import configure as configure_initialization
from forge.cli.commands.maintenance import (
    configure_backup,
    configure_doctor,
    configure_plan,
    configure_runs,
    configure_storage,
    run_doctor,
)
from forge.cli.commands.markets import configure as configure_markets
from forge.cli.commands.merge import configure as configure_merge
from forge.cli.commands.normalization import configure as configure_normalization
from forge.cli.commands.pipeline import configure as configure_pipeline
from forge.cli.commands.recipe_deletion import configure_delete
from forge.cli.commands.registration import configure as configure_registration
from forge.cli.environment import add_database_options
from forge.cli.help import HelpParser
from forge.shared.config.library import resolve_config_file
from forge.shared.database.connection import database_environment
from forge.shared.errors import MarketForgeError
from forge.shared.ownership import WorkspaceOwnership
from forge.shared.terminal.console import create_console
from forge.shared.terminal.spacing import terminal_spacing
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
    production: bool = False
    refresh: bool = False
    silent: bool = False
    requests_per_second: float | None = None


def _application_version() -> str:
    try:
        return version("marketforge")
    except PackageNotFoundError:
        return "uninstalled"


def build_parser() -> HelpParser:
    """Build the root command parser."""
    parser = HelpParser(
        prog="marketforge",
        description=("Historical market-data acquisition and Rust engine orchestration."),
        allow_abbrev=False,
    )

    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {_application_version()}",
        help="Show the installed MarketForge version.",
    )
    parser.add_argument(
        "--project-root",
        type=Path,
        metavar="PATH",
        help="Select the MarketForge workspace root.",
    )
    parser.add_argument(
        "--config",
        dest="config_file",
        type=Path,
        metavar="PATH",
        help="Override the Forge application TOML.",
    )
    parser.add_argument(
        "--json",
        dest="as_json",
        action="store_true",
        help="Emit command results and runtime errors as plain JSON.",
    )

    commands = parser.add_subparsers(dest="command")

    tui = commands.add_parser("tui", help="Open the interactive Forge workspace.")
    add_database_options(tui)

    configure_initialization(
        commands.add_parser("init", help="Initialize storage, catalogue and standard rankings.")
    )

    configure_engine(
        commands.add_parser("engine", help="Build and inspect approved managed engines.")
    )

    configure_pipeline(
        commands.add_parser(
            "pipeline", help="Run acquisition, normalization and merge from one recipe."
        )
    )

    configure_normalization(
        commands.add_parser(
            "normalize", help="Prepare, execute and register historical normalization."
        )
    )

    configure_datasets(
        commands.add_parser("datasets", help="Inspect registered primary data files.")
    )

    configure_conversion(
        commands.add_parser("convert", help="Prepare and submit historical conversion jobs.")
    )
    configure_merge(
        commands.add_parser("merge", help="Prepare, execute and register historical merges.")
    )

    configure_catalog(
        commands.add_parser("catalog", help="Register and inspect manual catalogue metadata.")
    )

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
            "doctor", help="Check Python, the selected database and storage prerequisites."
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
        help="Discover and download historical market-data archives.",
        allow_abbrev=False,
    )
    acquisition_commands = acquisition_parser.add_subparsers(
        dest="acquisition_command",
        required=True,
    )
    configure_delete(
        acquisition_commands.add_parser(
            "delete", help="Preview or delete a managed acquisition recipe."
        )
    )
    configure_registration(
        acquisition_commands.add_parser(
            "register", help="Register local raw data in managed storage."
        )
    )
    configure_imports(
        acquisition_commands.add_parser(
            "import", help="Import external raw recordings while preserving their source identity."
        )
    )
    configure_plan(
        acquisition_commands.add_parser(
            "plan", help="Inspect cached discovery and sample local copies (optional)."
        )
    )
    acquisition_commands.add_parser(
        "list",
        help="List saved acquisition configurations.",
        allow_abbrev=False,
    )

    acquisition_commands.add_parser(
        "sources",
        help="List supported historical archive combinations and scopes.",
        allow_abbrev=False,
    )

    for action, description in (
        ("new", "Create an acquisition recipe."),
        ("edit", "Edit a saved recipe and check its structure."),
        ("validate", "Check a saved recipe's structure locally."),
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
            help="Discover historical archives matching a saved recipe."
            if action == "map"
            else "Download archives or reuse verified managed copies.",
            description=(
                "Fresh successful discovery, including empty results, is reused for six hours. "
                "Results remain on stdout in silent mode; progress goes to stderr."
            ),
        )
        action_parser.add_argument(
            "configuration_name", help="Saved acquisition configuration name."
        )
        add_database_options(action_parser)
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
    add_database_options(migrate_parser, required=True)
    status_parser = database_commands.add_parser(
        "status",
        help="Inspect database identity and migration history.",
        allow_abbrev=False,
    )
    add_database_options(status_parser)
    parser.bind_help()
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
    tokens = sys.argv[1:] if argv is None else argv
    with terminal_spacing(enabled="--json" not in tokens):
        parser.parse_args(tokens, namespace=arguments)
        if arguments.testing and arguments.production:
            parser.error("--testing and --production are mutually exclusive.")
    with (
        database_environment(testing=arguments.testing),
        terminal_spacing(enabled=not arguments.as_json and arguments.command != "tui"),
    ):
        return _dispatch(parser, arguments)


def _dispatch(parser: HelpParser, arguments: _Arguments) -> int:

    if arguments.command is None:
        parser.print_root_help(arguments)
        return 0

    try:
        if arguments.command == "doctor":
            return run_doctor(
                project_root=arguments.project_root,
                config_file=arguments.config_file,
                testing=arguments.testing,
                as_json=arguments.as_json,
            )
        context = build_context(
            project_root=arguments.project_root,
            config_file=arguments.config_file,
            testing=arguments.testing,
        )

        if arguments.command == "tui":
            if arguments.as_json or not all(
                stream.isatty() for stream in (sys.stdin, sys.stdout, sys.stderr)
            ):
                raise MarketForgeError(
                    "mf tui requires an interactive terminal and is incompatible with --json."
                )
            from forge.tui.app import ForgeApp

            ForgeApp(context).run()
            return 0

        if arguments.service_handler is not None:
            return arguments.service_handler(arguments, context)

        if arguments.command == "acquisition":
            directory = context.paths.acquisition_config_dir

            if arguments.acquisition_command == "list":
                list_acquisition_configs(
                    directory,
                    as_json=arguments.as_json,
                    console=context.console,
                )
                return 0

            if arguments.acquisition_command == "sources":
                show_acquisition_sources(context.console, as_json=arguments.as_json)
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
                    refresh=arguments.refresh,
                    requests_per_second=arguments.requests_per_second,
                    silent=arguments.silent,
                    as_json=arguments.as_json,
                )
            if arguments.acquisition_command == "new":
                new_acquisition(
                    source,
                    as_json=arguments.as_json,
                    console=context.console,
                    root=context.paths.root,
                )
            elif arguments.acquisition_command == "validate":
                return validate_acquisition(
                    source,
                    as_json=arguments.as_json,
                    console=context.console,
                    error_console=context.error_console,
                    root=context.paths.root,
                )
            elif arguments.acquisition_command == "edit":
                if arguments.as_json:
                    parser.error("Interactive editing does not support --json.")

                return edit_acquisition(
                    source,
                    presentation=context.settings.presentation,
                    console=context.console,
                    error_console=context.error_console,
                    root=context.paths.root,
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
                    project_root=context.paths.root,
                    presentation=context.settings.presentation,
                    as_json=arguments.as_json,
                )
            return 0

        if arguments.command == "database" and arguments.database_command == "status":
            show_database_status(
                testing=arguments.testing,
                project_root=context.paths.root,
                presentation=context.settings.presentation,
                as_json=arguments.as_json,
            )
            return 0

        parser.error("Unsupported command.")
    except KeyboardInterrupt:
        _emit_error(
            "Command interrupted. Inspect the retained run receipt for recovery.",
            as_json=arguments.as_json,
        )
        return 130
    except tomllib.TOMLDecodeError:
        _emit_error(
            "Invalid TOML configuration; inspect the named file.", as_json=arguments.as_json
        )
        return 2
    except json.JSONDecodeError:
        _emit_error(
            "Invalid JSON document; inspect the submitted batch, receipt or authoritative manifest.",
            as_json=arguments.as_json,
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
