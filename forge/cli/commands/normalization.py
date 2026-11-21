"""Historical normalization preparation, execution and registration commands."""

import argparse
import asyncio
import json
from pathlib import Path

from forge.historical.normalization.batch import prepare_batch
from forge.historical.normalization.config import NormalizationRecipe
from forge.historical.normalization.datasets import register
from forge.historical.normalization.execution import execute, recover
from forge.historical.normalization.ranking import RankingPreparationError
from forge.historical.normalization.recipes import (
    create_recipe,
    load_configuration,
    recipe_path,
)
from forge.historical.normalization.service import prepare
from forge.shared.config.library import ConfigurationNameError, run_reference
from forge.shared.errors import MarketForgeError
from forge.shared.ownership import WorkspaceOwnership
from forge.shared.terminal.editor import edit_file
from forge.shared.terminal.normalization import render_batch, render_execution
from forge.startup.context import ApplicationContext


def configure(parser: argparse.ArgumentParser) -> None:
    actions = parser.add_subparsers(dest="normalization_action", required=True)
    for action in ("run", "register"):
        child = actions.add_parser(action, allow_abbrev=False)
        child.add_argument("reference")
        child.add_argument("--testing", required=True, action="store_true")
        if action == "run":
            child.add_argument("--tui", action="store_true")
            child.add_argument("--allow-partial", action="store_true")
        child.set_defaults(service_handler=run)
    for action in ("new", "validate", "prepare", "execute"):
        child = actions.add_parser(action, allow_abbrev=False)
        child.add_argument("configuration_name")
        child.set_defaults(service_handler=run)
        if action in {"new", "prepare"}:
            child.add_argument(
                "--from-acquisition",
                metavar="SOURCE",
                help="Create and edit a copy of an acquisition recipe before validation.",
            )
        if action == "execute":
            child.add_argument("--tui", action="store_true")
            child.add_argument("--allow-partial", action="store_true")
        if action in {"prepare", "execute"}:
            child.add_argument(
                "--testing",
                action="store_true",
                required=True,
                help="Use the verified testing database.",
            )
            child.add_argument(
                "--dry-run",
                action="store_true",
                help="Resolve the same plan without writing a batch or report.",
            )


def recipe_path_if_valid(context: ApplicationContext, reference: str) -> Path | None:
    try:
        return recipe_path(context.paths, reference)
    except ConfigurationNameError:
        return None


def run(arguments: argparse.Namespace, context: ApplicationContext) -> int:
    action = arguments.normalization_action
    supplied: Path | None = None

    if action == "run":
        reference = arguments.reference
        supplied, submitted = run_reference(reference, recipe_path_if_valid(context, reference))
        if not submitted:
            action = "execute"
            arguments.configuration_name = str(supplied)
            arguments.dry_run = False
    if getattr(arguments, "tui", False) and arguments.as_json:
        raise MarketForgeError("--tui is incompatible with global --json.")
    if action in {"run", "register"}:
        with WorkspaceOwnership(context.paths):
            result = (
                execute(
                    context.paths,
                    supplied,
                    tui=arguments.tui,
                    allow_partial=arguments.allow_partial,
                )
                if supplied is not None and action == "run"
                else asyncio.run(
                    (
                        recover
                        if Path(arguments.reference).name == "execution-intent.json"
                        else register
                    )(context.paths, Path(arguments.reference))
                )
            )
        if arguments.as_json:
            print(json.dumps(result))
        elif "execution_id" in result:
            render_execution(context.console, result)
        else:
            context.console.print_json(data=result)
        return (
            0
            if result.get("status") in {"complete", "partial"}
            or result.get("registration") == "registered"
            else 1
        )
    requested_name = (
        arguments.reference
        if arguments.normalization_action == "run"
        else arguments.configuration_name
    )
    acquisition = getattr(arguments, "from_acquisition", None)
    source = recipe_path(context.paths, arguments.configuration_name)
    if action == "new" or (action == "prepare" and acquisition is not None):
        with WorkspaceOwnership(context.paths):
            source = create_recipe(context.paths, arguments.configuration_name, acquisition)
        # Editors are interactive subprocesses; never hold the workspace lock while editing.
        edit_file(source, editor=context.settings.presentation.editor)
    if not source.is_file():
        raise MarketForgeError(f"Normalization recipe file is unavailable: {source}")
    request = load_configuration(source)
    result: dict[str, object]
    if action in {"new", "validate"}:
        count = (
            len(request.requests)
            if isinstance(request, NormalizationRecipe)
            else len(request.inputs)
        )
        result = {
            "schema_version": 1,
            "status": "template_created" if action == "new" else "valid",
            "path": str(source),
            "input_count": count,
            "message": "Recipe structure valid; prepare resolves archives and instrument identity. Engine resolves archive contents.",
        }
    else:
        try:
            with WorkspaceOwnership(context.paths):
                if isinstance(request, NormalizationRecipe):
                    result = asyncio.run(
                        prepare_batch(
                            context.paths,
                            request,
                            dry_run=arguments.dry_run,
                            http=context.settings.http,
                        )
                    )
                elif arguments.dry_run:
                    result = asyncio.run(
                        prepare(
                            context.paths,
                            request,
                            dry_run=True,
                            http=context.settings.http,
                        )
                    )
                else:
                    result = asyncio.run(
                        prepare(context.paths, request, http=context.settings.http)
                    )
        except RankingPreparationError as exc:
            result = {
                "schema_version": 1,
                "status": "failed",
                "ranking_resolution": exc.details,
                "error": str(exc),
            }
    if action == "execute" and not arguments.dry_run and result.get("status") == "prepared":
        with WorkspaceOwnership(context.paths):
            result = execute(
                context.paths,
                Path(str(result["batch_path"])),
                tui=arguments.tui,
                allow_partial=arguments.allow_partial,
                recipe=requested_name,
            )
    if (
        action == "execute"
        and not arguments.dry_run
        and result["status"] == "partial"
        and "execution_id" not in result
    ):
        result["preparation_status"] = "partial"
        result["status"] = "failed"
    if "batch_path" in result:
        result["recipe"] = requested_name
    if arguments.as_json:
        print(json.dumps(result))
    elif "execution_id" in result:
        render_execution(context.console, result)
    elif "batch_path" in result:
        render_batch(context.console, result)
    else:
        for key, value in result.items():
            if key != "schema_version":
                context.console.print(f"{key}: ", style="mf.identifier", end="")
                context.console.print(str(value), markup=False)
    return (
        1
        if result["status"] in {"failed", "interrupted", "incomplete"}
        or (result["status"] == "partial" and "registrations" not in result)
        else 0
    )
