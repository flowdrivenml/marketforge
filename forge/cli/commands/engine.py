"""Thin managed engine commands; no database initialization."""

import argparse
import json

from forge.historical.engine.managed import build, inventory, plan, select
from forge.historical.engine.runtime import runtime_report
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
    runtime = actions.add_parser(
        "runtime", help="Validate and show effective Engine runtime settings."
    )
    runtime.add_argument(
        "operation",
        choices=("normalize", "merge", "convert", "pipeline"),
        help="Operation whose settings to inspect.",
    )
    runtime.set_defaults(service_handler=run)
    command = actions.add_parser("build")
    command.add_argument(
        "target", nargs="?", help="Engine target. Omit to build the default targets."
    )
    command.add_argument("--dry-run", action="store_true", help="Show the proposed build.")
    command.add_argument("--offline", action="store_true", help="Use the local Cargo cache.")
    command.add_argument("--jobs", type=positive, help="Positive limit on concurrent Cargo jobs.")
    command.set_defaults(service_handler=run)


def run(args: argparse.Namespace, context: ApplicationContext) -> int:
    if args.engine_action == "status":
        result = inventory(context.paths)
    elif args.engine_action == "runtime":
        result = runtime_report(context.paths, args.operation)
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
