"""Named historical normalization preparation commands; never execute an engine."""

import argparse
import asyncio
import json

from forge.historical.archives.config import AcquisitionConfig
from forge.historical.normalization.batch import prepare_batch
from forge.historical.normalization.recipes import create_recipe, load_configuration, recipe_path
from forge.historical.normalization.service import prepare
from forge.shared.ownership import WorkspaceOwnership
from forge.shared.terminal.editor import edit_file
from forge.shared.terminal.normalization import render_batch
from forge.startup.context import ApplicationContext


def configure(parser: argparse.ArgumentParser) -> None:
    actions = parser.add_subparsers(dest="normalization_action", required=True)
    for action in ("new", "validate", "prepare"):
        child = actions.add_parser(action, allow_abbrev=False)
        child.add_argument("configuration_name")
        child.set_defaults(service_handler=run)
        if action in {"new", "prepare"}:
            child.add_argument(
                "--from-acquisition",
                metavar="SOURCE",
                help="Create and edit a copy of an acquisition recipe before validation.",
            )
        if action == "prepare":
            child.add_argument(
                "--testing",
                action="store_true",
                required=True,
                help="Read only the verified testing database; generate JSON without execution.",
            )
            child.add_argument(
                "--dry-run",
                action="store_true",
                help="Resolve the same plan without writing a batch or report.",
            )


def run(arguments: argparse.Namespace, context: ApplicationContext) -> int:
    action = arguments.normalization_action
    acquisition = getattr(arguments, "from_acquisition", None)
    source = recipe_path(context.paths, arguments.configuration_name)
    if action == "new" or (action == "prepare" and acquisition is not None):
        with WorkspaceOwnership(context.paths):
            source = create_recipe(context.paths, arguments.configuration_name, acquisition)
        # Editors are interactive subprocesses; never hold the workspace lock while editing.
        edit_file(source, editor=context.settings.presentation.editor)
    request = load_configuration(source)
    result: dict[str, object]
    if action in {"new", "validate"}:
        count = (
            len(request.requests) if isinstance(request, AcquisitionConfig) else len(request.inputs)
        )
        result = {
            "schema_version": 1,
            "status": "template_created" if action == "new" else "valid",
            "path": str(source),
            "input_count": count,
            "message": "Recipe structure valid; prepare resolves local archives, profiles and metadata.",
        }
    else:
        with WorkspaceOwnership(context.paths):
            if isinstance(request, AcquisitionConfig):
                result = asyncio.run(
                    prepare_batch(context.paths, request, dry_run=arguments.dry_run)
                )
            elif arguments.dry_run:
                result = asyncio.run(prepare(context.paths, request, dry_run=True))
            else:
                result = asyncio.run(prepare(context.paths, request))
    if arguments.as_json:
        print(json.dumps(result))
    elif "batch_path" in result:
        render_batch(context.console, result)
    else:
        for key, value in result.items():
            if key != "schema_version":
                context.console.print(f"{key}: ", style="mf.identifier", end="")
                context.console.print(str(value), markup=False)
    return 1 if result["status"] == "partial" else 0
