"""Registration CLI composing shared raw-copy and profile services."""

import argparse
import asyncio
import json
from pathlib import Path

from forge.historical.registration.models import load, recipe_path
from forge.historical.registration.service import apply, inspect, validate
from forge.shared.ownership import WorkspaceOwnership
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
            child.add_argument("--testing", action="store_true", required=True)


def run(args: argparse.Namespace, context: ApplicationContext) -> int:
    path = recipe_path(context.paths, args.name)
    action = args.registration_action
    if action == "new":
        # Accept a manually authored recipe, or create an intentionally incomplete
        # draft for a source directory/file. Never infer identity or profiles.
        if args.source.suffix == ".toml":
            data = args.source.read_bytes()
        else:
            root = args.source.resolve()
            data = (
                f"[registration]\nversion = 1\nname = {json.dumps(args.name)}\n\n[source]\nkey = {json.dumps(args.name)}\nroot = {json.dumps(str(root if root.is_dir() else root.parent))}\n# REQUIRED: provider, product, provenance, coverage, and inputs referencing registered catalogue metadata.\n"
            ).encode()
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("xb") as destination:
            destination.write(data)
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
        context.console.print_json(data=result)
    return 0
