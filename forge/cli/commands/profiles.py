"""Namespaced profile management without database initialization."""

import argparse
import asyncio
import json
from pathlib import Path

from forge.historical.imports.formats import sample
from forge.historical.profiles.service import discover, inspect, installed, register, validate
from forge.shared.errors import MarketForgeError
from forge.startup.context import ApplicationContext


def configure(parser: argparse.ArgumentParser) -> None:
    actions = parser.add_subparsers(dest="profile_action", required=True)
    command = actions.add_parser("list")
    for name in ("type", "exchange", "instrument-type", "market-category"):
        command.add_argument(f"--{name}")
    command.set_defaults(service_handler=run)
    command = actions.add_parser(
        "sample", help="Inspect a bounded raw sample before authoring a profile."
    )
    command.add_argument("--file", type=Path, required=True)
    command.add_argument("--member")
    command.add_argument("--header", action=argparse.BooleanOptionalAction, default=None)
    command.set_defaults(service_handler=run)
    for action in ("inspect", "validate", "add", "register"):
        command = actions.add_parser(action)
        command.add_argument("profile_id", nargs="?" if action == "validate" else None)
        if action == "validate":
            command.add_argument("--file", type=Path)
        if action in {"add", "register"}:
            command.add_argument("--from", dest="source", type=Path, required=True)
            command.add_argument(
                "--testing",
                action="store_true",
                help="Profile publication always checks the testing catalogue.",
            )
        command.set_defaults(service_handler=run)


def run(args: argparse.Namespace, context: ApplicationContext) -> int:
    if args.profile_action == "sample":
        result = sample(args.file, args.member, args.header)
    elif args.profile_action == "list":
        result = {
            "profiles": discover(
                context.paths,
                **{
                    name: getattr(args, name)
                    for name in ("type", "exchange", "instrument_type", "market_category")
                },
            )
        }
    elif args.profile_action in {"add", "register"}:
        result = asyncio.run(register(context.paths, args.profile_id, args.source))
    else:
        source = getattr(args, "file", None)
        if (source is None) == (args.profile_id is None):
            raise MarketForgeError("Supply exactly one profile ID or --file.")
        source = source or installed(context.paths, args.profile_id)
        result = (
            validate(context.paths, source)
            if args.profile_action == "validate"
            else inspect(source)
        )
    if args.as_json:
        print(json.dumps(result))
    else:
        context.console.print_json(data=result)
    return 0
