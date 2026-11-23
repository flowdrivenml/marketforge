"""Registration CLI composing shared raw-copy and profile services."""

import argparse
import asyncio
import json
from pathlib import Path

from forge.cli.environment import add_database_options
from forge.historical.drafts import create_registration
from forge.historical.registration.models import load, recipe_path
from forge.historical.registration.service import apply, inspect, validate
from forge.shared.ownership import WorkspaceOwnership
from forge.shared.terminal.acquisition_result import render_external
from forge.shared.terminal.editor import edit_file
from forge.startup.context import ApplicationContext


def configure(parser: argparse.ArgumentParser) -> None:
    actions = parser.add_subparsers(dest="registration_action", required=True)
    for action in ("new", "edit", "inspect", "validate", "plan", "run"):
        child = actions.add_parser(action)
        child.add_argument("name")
        child.set_defaults(service_handler=run)
        if action == "new":
            child.add_argument("--from", dest="source", type=Path, required=True)
        if action in {"validate", "plan", "run"}:
            add_database_options(child)


def run(args: argparse.Namespace, context: ApplicationContext) -> int:
    path = recipe_path(context.paths, args.name)
    action = args.registration_action
    if action == "new":
        create_registration(path, args.name, args.source.resolve())
        result = {"status": "draft_created", "path": str(path)}
    elif action == "edit":
        if args.as_json:
            from forge.shared.errors import MarketForgeError

            raise MarketForgeError("Interactive editing is incompatible with --json.")
        edit_file(path, editor=context.settings.presentation.editor)
        result = inspect(load(path), context.paths)
    elif action in {"plan", "run"}:
        with WorkspaceOwnership(context.paths):
            result = asyncio.run(apply(context.paths, path, planning=action == "plan"))
    else:
        recipe = load(path)
        result = (
            asyncio.run(validate(recipe, context.paths))
            if action == "validate"
            else inspect(recipe, context.paths)
        )
    if args.as_json:
        print(json.dumps(result))
    else:
        render_external(
            context.console,
            result,
            operation="Registration",
            action=action,
            name=args.name,
            root=context.paths.root,
        )
    return 0
