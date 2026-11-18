"""Managed historical merge commands."""

import argparse
import asyncio
import json
import tomllib
from pathlib import Path

from forge.historical.merge import service
from forge.shared.config.library import ConfigurationNameError, resolve_config_file, run_reference
from forge.shared.errors import MarketForgeError
from forge.shared.terminal.normalization import render_execution
from forge.startup.context import ApplicationContext


def configure(parser: argparse.ArgumentParser) -> None:
    actions = parser.add_subparsers(dest="merge_action", required=True)
    for action in ("new", "validate", "prepare", "run", "register"):
        child = actions.add_parser(action, allow_abbrev=False)
        child.add_argument("reference")
        if action in {"prepare", "run", "register"}:
            child.add_argument("--testing", required=True, action="store_true")
        if action == "run":
            child.add_argument("--tui", action="store_true")
        child.set_defaults(service_handler=run)


def run(args: argparse.Namespace, context: ApplicationContext) -> int:
    paths = context.paths
    action = args.merge_action
    reference = args.reference
    supplied = Path(reference).expanduser()
    tui = getattr(args, "tui", False)
    if args.as_json and tui:
        raise MarketForgeError("--tui is incompatible with global --json.")
    if action == "register":
        result = asyncio.run(service.register(paths, supplied))
    else:
        library = paths.config_dir / "historical/merge"
        explicit = supplied.is_absolute() or "/" in reference
        try:
            recipe = None if explicit else resolve_config_file(library, reference)
        except ConfigurationNameError:
            if not supplied.is_file():
                raise
            recipe = None
        if action == "new":
            if recipe is None:
                raise MarketForgeError("Use a managed recipe name with merge new.")
            recipe.parent.mkdir(parents=True, exist_ok=True)
            with recipe.open("x", encoding="utf-8") as output:
                output.write(service.TEMPLATE)
            result = {"status": "template_created", "path": str(recipe)}
        else:
            if action == "run":
                source, submitted = run_reference(reference, recipe)
            else:
                source = supplied if explicit else recipe
                if source is None or not source.is_file():
                    raise MarketForgeError(f"Unknown merge recipe: {reference}")
                submitted = False
            if submitted:
                result = service.execute(paths, source, tui=tui)
            else:
                request = service.Recipe.model_validate(tomllib.loads(source.read_text()))
                if action == "validate":
                    result = {"status": "valid", "path": str(source), **request.model_dump()}
                else:
                    result = service.prepare(paths, request, reference)
                    if action == "run":
                        result = service.execute(paths, Path(result["job_path"]), tui=tui)
    if args.as_json:
        print(json.dumps(result))
    elif "execution_id" in result:
        render_execution(context.console, result)
    else:
        context.console.print_json(data=result)
    if action == "register":
        return int(result["registration"] != "registered")
    return int(result["status"] in {"failed", "interrupted"})
