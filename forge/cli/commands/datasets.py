"""Browse registered primary data files without reading publication artifacts."""

import argparse
import asyncio
import json
from typing import TextIO, cast

from forge.datasets.query import (
    DATA_TYPES,
    SEARCH_ACTIONS,
    DatasetRequest,
    bounded,
    browse,
    timestamp,
)
from forge.shared.errors import MarketForgeError
from forge.shared.terminal.console import create_console
from forge.shared.terminal.datasets import (
    render_coverage,
    render_datasets,
    render_find,
    render_inspection,
)
from forge.startup.context import ApplicationContext


def configure(parser: argparse.ArgumentParser) -> None:
    actions = parser.add_subparsers(dest="dataset_action", required=True)
    for name in (
        "list",
        "find",
        "show",
        "streams",
        "lineage",
        "coverage",
        "verify",
        "schema",
        "head",
        "migrate-registration",
        "refresh-summary",
    ):
        command = actions.add_parser(
            name,
            allow_abbrev=False,
            help="Find registered files by market identity." if name == "find" else None,
        )
        command.add_argument("--testing", action="store_true", required=True)
        command.set_defaults(service_handler=run)
        if name not in SEARCH_ACTIONS | {"migrate-registration"}:
            command.add_argument("id", type=int)
        if name == "list":
            command.add_argument("--wide", action="store_true")
            command.add_argument(
                "--precise", action="store_true", help="Display full nanosecond UTC timestamps."
            )
        if name in SEARCH_ACTIONS:
            command.add_argument("--operation", choices=("normalize", "merge"))
            command.add_argument("--data-type", choices=DATA_TYPES)
            for flag in (
                "status",
                "exchange",
                "instrument",
                "instrument-type",
                "market-category",
                "publication",
            ):
                command.add_argument(f"--{flag}")
            command.add_argument("--start", type=timestamp)
            command.add_argument("--end", type=timestamp)
            command.add_argument("--limit", type=bounded, default=50)
            command.add_argument("--offset", type=int, default=0)
        if name == "head":
            command.add_argument("--limit", type=bounded, default=10)
            command.add_argument("--column", action="append")
            command.add_argument(
                "--wide", action="store_true", help="Show all stored columns by default."
            )
            command.add_argument(
                "--precise", action="store_true", help="Show exact nanosecond UTC timestamps."
            )
        if name == "lineage":
            command.add_argument(
                "--publication", action="store_true", help="Expand publication-level selection."
            )
            command.add_argument(
                "--checksums", action="store_true", help="Show retained raw input checksums."
            )
            command.add_argument(
                "--limit", type=bounded, default=25, help="Maximum rows per lineage section."
            )
        if name == "migrate-registration":
            command.add_argument("--all", action="store_true", required=True)
            command.add_argument("--dry-run", action="store_true")


def request_from_args(args: argparse.Namespace) -> DatasetRequest:
    return DatasetRequest(
        **{
            key: value
            for key, value in vars(args).items()
            if key in DatasetRequest.__dataclass_fields__
        }
    )


def run(args: argparse.Namespace, context: ApplicationContext) -> int:
    try:
        request = request_from_args(args)
        result = asyncio.run(browse(context.paths, request, detailed=not args.as_json))
    except (ImportError, KeyError, ValueError) as error:
        raise MarketForgeError(str(error)) from error
    if args.as_json:
        print(json.dumps(result, default=str))
        return int(bool(result.get("conflict_count")))
    console = context.console
    if console.color_system is not None and (not console.file.isatty() or console.is_dumb_terminal):
        # Dataset output can be redirected independently of interactive color preferences.
        settings = context.settings.presentation.model_copy(update={"color": "never"})
        console = create_console(settings, stream=cast(TextIO, console.file))
    if args.dataset_action == "list":
        render_datasets(console, result, wide=args.wide, precise=args.precise)
    elif args.dataset_action == "find":
        render_find(console, result, filters=vars(args))
    elif args.dataset_action in {"show", "streams", "lineage", "schema", "head"}:
        render_inspection(console, args.dataset_action, result)
    elif args.dataset_action == "coverage":
        render_coverage(console, result, filters=vars(args))
    else:
        console.print_json(json.dumps(result, default=str))
    return int(bool(result.get("conflict_count")))
