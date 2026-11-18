"""Thin managed engine commands; no database initialization."""

import argparse
import json

from forge.historical.engine.managed import build, inventory, plan, select
from forge.startup.context import ApplicationContext


def positive(value: str) -> int:
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError("jobs must be positive")
    return number


def configure(parser: argparse.ArgumentParser) -> None:
    actions = parser.add_subparsers(dest="engine_action", required=True)
    status = actions.add_parser("status")
    status.set_defaults(service_handler=run)
    command = actions.add_parser("build")
    command.add_argument("target", nargs="?")
    command.add_argument("--dry-run", action="store_true")
    command.add_argument("--offline", action="store_true")
    command.add_argument("--jobs", type=positive)
    command.set_defaults(service_handler=run)


def run(args: argparse.Namespace, context: ApplicationContext) -> int:
    if args.engine_action == "status":
        result = inventory(context.paths)
    else:
        service = plan if args.dry_run else build
        result = {
            "targets": [
                service(context.paths, target, offline=args.offline, jobs=args.jobs)
                for target in select(args.target)
            ]
        }
    if args.as_json:
        print(json.dumps(result))
    else:
        context.console.print_json(data=result)
    return 0
