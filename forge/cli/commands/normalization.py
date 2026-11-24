"""Historical normalization preparation, execution and registration commands."""

import argparse
import asyncio
import hashlib
import json
import tomllib
from contextlib import nullcontext
from pathlib import Path

from forge.cli.commands.normalization_runtime import configure as configure_runtime
from forge.cli.commands.profiles import configure as configure_profiles
from forge.cli.commands.recipe_deletion import configure_delete
from forge.cli.environment import add_database_options
from forge.cli.sorting import add_sorting
from forge.historical.normalization.batch import blocked_execution, prepare_batch
from forge.historical.normalization.config import ExactRecipe, NormalizationRecipe
from forge.historical.normalization.datasets import register
from forge.historical.normalization.execution import execute, recover
from forge.historical.normalization.ranking import RankingPreparationError
from forge.historical.normalization.recipes import (
    create_recipe,
    load_configuration,
    recipe_path,
)
from forge.historical.normalization.service import prepare
from forge.orchestration.registry import checkpoint, cli_execution, finish_cli, rows
from forge.orchestration.run_summary import mapping
from forge.shared.config.library import (
    ConfigurationNameError,
    list_config_names,
    resolve_config_file,
    run_reference,
)
from forge.shared.config.loader import ConfigurationLoadError
from forge.shared.database.connection import database_purpose
from forge.shared.errors import MarketForgeError
from forge.shared.identity import RunOperation
from forge.shared.ownership import WorkspaceOwnership
from forge.shared.presentation.normalization import (
    NormalizationOutcome,
    PreparationView,
    RecipeView,
)
from forge.shared.sorting import NAMES
from forge.shared.terminal.editor import edit_file
from forge.shared.terminal.normalization import (
    render_batch,
    render_inventory,
    render_normalization_outcome,
    render_recipe,
)
from forge.startup.context import ApplicationContext


def execution_manifests(result: dict[str, object]) -> dict[str, dict[str, object]]:
    """Load optional display metrics from the exact verified publication manifests."""
    manifests: dict[str, dict[str, object]] = {}
    max_bytes = 16 * 1024 * 1024
    for job in rows(result.get("jobs")):
        path, digest = job.get("manifest_path"), job.get("manifest_sha256")
        if (
            "processing_counters" in job
            or job.get("verification") != "verified"
            or not isinstance(path, str)
            or not isinstance(digest, str)
        ):
            continue
        try:
            with Path(path).open("rb") as stream:
                data = stream.read(max_bytes + 1)
            if len(data) <= max_bytes and hashlib.sha256(data).hexdigest() == digest:
                manifests[str(job["dataset_id"])] = dict(mapping(json.loads(data)))
        except (OSError, ValueError, KeyError):
            continue
    return manifests


def configure(parser: argparse.ArgumentParser) -> None:
    actions = parser.add_subparsers(dest="normalization_action", required=True)
    configure_runtime(
        actions.add_parser(
            "config", help="Inspect, validate and change the Engine runtime.", allow_abbrev=False
        )
    )
    configure_profiles(
        actions.add_parser("profiles", help="Author and manage normalization profiles.")
    )
    configure_delete(
        actions.add_parser("delete", help="Preview or delete a managed normalization recipe.")
    )
    listing = actions.add_parser("list", help="List managed recipes and their structural status.")
    listing.set_defaults(service_handler=run)
    add_sorting(listing, NAMES)
    for action, description in (
        ("show", "Inspect a managed recipe."),
        ("edit", "Edit and structurally validate a managed recipe."),
    ):
        child = actions.add_parser(action, help=description, allow_abbrev=False)
        child.add_argument(
            "configuration_name", metavar="NAME", help="Managed normalization recipe name."
        )
        child.set_defaults(service_handler=run)
    for action in ("run", "register"):
        child = actions.add_parser(action, allow_abbrev=False)
        child.add_argument(
            "reference",
            help=(
                "Managed recipe name, TOML path or prepared batch JSON path."
                if action == "run"
                else "Committed output or retained execution intent."
            ),
        )
        add_database_options(child)
        if action == "run":
            child.add_argument(
                "--tui", action="store_true", help="Open the Engine progress monitor."
            )
            child.add_argument(
                "--allow-partial",
                action="store_true",
                help="Accept partial execution when eligible outputs can be verified and registered.",
            )
        child.set_defaults(service_handler=run)
    for action in ("new", "validate", "prepare", "execute"):
        child = actions.add_parser(action, allow_abbrev=False)
        child.add_argument(
            "configuration_name",
            metavar="NAME",
            help="Managed normalization recipe name"
            + ("." if action == "new" else " or explicit TOML path."),
        )
        child.set_defaults(service_handler=run)
        if action in {"new", "prepare"}:
            child.add_argument(
                "--from-acquisition",
                metavar="SOURCE",
                help="Create and edit a copy of an acquisition recipe before validation.",
            )
        if action == "execute":
            child.add_argument(
                "--tui", action="store_true", help="Open the Engine progress monitor."
            )
            child.add_argument(
                "--allow-partial",
                action="store_true",
                help="Accept partial execution when eligible outputs can be verified and registered.",
            )
        if action in {"prepare", "execute"}:
            add_database_options(child)
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


def local_recipe(arguments: argparse.Namespace, context: ApplicationContext, action: str) -> int:
    paths = context.paths
    directory = paths.normalization_recipe_dir
    if action == "list":
        items: list[RecipeView] = []
        for name in list_config_names(directory, sort=arguments.sort, order=arguments.order):
            source = directory / f"{name}.toml"
            try:
                source = resolve_config_file(directory, name)
                items.append(RecipeView(name, source, load_configuration(source)))
            except (ConfigurationLoadError, ConfigurationNameError, OSError) as error:
                items.append(RecipeView(name, source, error=str(error)))
        if arguments.as_json:
            print(
                json.dumps(
                    {
                        "schema_version": 1,
                        "recipes": [
                            {
                                "name": item.name,
                                "path": str(item.source),
                                "valid": item.configuration is not None,
                                "type": (
                                    "source_selection"
                                    if isinstance(item.configuration, NormalizationRecipe)
                                    else "explicit_inputs"
                                )
                                if item.configuration is not None
                                else None,
                                "input_count": item.count,
                                "interval": item.configuration.interval.model_dump(mode="json")
                                if isinstance(item.configuration, NormalizationRecipe)
                                else None,
                                **({"error": item.error} if item.error else {}),
                            }
                            for item in items
                        ],
                    }
                )
            )
        else:
            render_inventory(
                context.console,
                tuple(items),
                ordering=NAMES.resolve(arguments.sort, arguments.order).description,
            )
        return 0

    name: str = arguments.configuration_name
    if action == "edit" and arguments.as_json:
        raise MarketForgeError("Interactive editing is incompatible with --json.")
    if action == "new":
        with WorkspaceOwnership(paths):
            source = create_recipe(paths, name, arguments.from_acquisition)
    elif action in {"edit", "show"}:
        source = resolve_config_file(directory, name)
    else:
        source = recipe_path(paths, name)
    if not source.is_file():
        if action == "validate":
            raise MarketForgeError(f"Normalization recipe file is unavailable: {source}")
        raise MarketForgeError(f"Normalization recipe {name!r} was not found.")
    if action in {"new", "edit"}:
        source = edit_file(source, editor=context.settings.presentation.editor)
    try:
        config = load_configuration(source)
    except (ConfigurationLoadError, OSError) as error:
        if arguments.as_json:
            raise
        message = (
            str(error.__context__)
            if isinstance(error.__context__, tomllib.TOMLDecodeError)
            else str(error)
        )
        render_recipe(
            context.error_console,
            RecipeView(source.stem, source, error=message),
            action=action,
            root=paths.root,
        )
        return 2

    item = RecipeView(source.stem, source, config)
    if arguments.as_json:
        if action == "show":
            payload = {
                "schema_version": 1,
                "name": item.name,
                "path": str(source),
                "type": "source_selection"
                if isinstance(config, NormalizationRecipe)
                else "explicit_inputs",
                "configuration": config.model_dump(mode="json"),
            }
        else:
            payload = {
                "schema_version": 1,
                "status": "template_created" if action == "new" else "valid",
                "path": str(source),
                "input_count": item.count,
                "message": "Recipe structure valid; prepare resolves archives and instrument identity. Engine resolves archive contents.",
            }
        print(json.dumps(payload))
    else:
        render_recipe(context.console, item, action=action, root=paths.root)
    return 0


def run(arguments: argparse.Namespace, context: ApplicationContext) -> int:
    action = arguments.normalization_action
    if action in {"list", "show", "edit", "new", "validate"}:
        return local_recipe(arguments, context, action)
    if action == "register":
        with (
            WorkspaceOwnership(context.paths),
            cli_execution(context.paths, RunOperation.NORMALIZE, action, arguments.reference),
        ):
            return run_operation(arguments, context, owns_workspace=True)
    if action in {"prepare", "run", "execute", "register"} and not getattr(
        arguments, "dry_run", False
    ):
        reference = getattr(arguments, "reference", None) or getattr(
            arguments, "configuration_name", ""
        )
        with cli_execution(context.paths, RunOperation.NORMALIZE, action, reference):
            return run_operation(arguments, context)
    return run_operation(arguments, context)


def run_operation(
    arguments: argparse.Namespace, context: ApplicationContext, *, owns_workspace: bool = False
) -> int:
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
        with nullcontext() if owns_workspace else WorkspaceOwnership(context.paths):
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
        result = finish_cli(result)
        if arguments.as_json:
            print(json.dumps(result))
        else:
            render_normalization_outcome(
                context.console,
                NormalizationOutcome.from_result(
                    result,
                    registering=action == "register",
                    manifests=execution_manifests(result) if action == "run" else None,
                ),
                root=context.paths.root,
                registering=action == "register",
                recovery=action == "register"
                and Path(arguments.reference).name == "execution-intent.json",
            )
        if result.get("status") == "interrupted":
            return 130
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
    if action == "prepare" and acquisition is not None:
        with WorkspaceOwnership(context.paths):
            source = create_recipe(context.paths, arguments.configuration_name, acquisition)
        # Editors are interactive subprocesses; never hold the workspace lock while editing.
        edit_file(source, editor=context.settings.presentation.editor)
    if not source.is_file():
        raise MarketForgeError(f"Normalization recipe file is unavailable: {source}")
    request = load_configuration(source)
    result: dict[str, object]
    try:
        with WorkspaceOwnership(context.paths):
            if isinstance(request, (NormalizationRecipe, ExactRecipe)):
                result = asyncio.run(
                    prepare_batch(
                        context.paths,
                        request,
                        recipe_source=source,
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
                result = asyncio.run(prepare(context.paths, request, http=context.settings.http))
    except RankingPreparationError as exc:
        result = {
            "schema_version": 1,
            "status": "failed",
            "ranking_resolution": exc.details,
            "error": str(exc),
        }
    if action == "execute" and not arguments.dry_run and result.get("status") == "prepared":
        checkpoint(result)
        with WorkspaceOwnership(context.paths):
            result = execute(
                context.paths,
                Path(str(result["batch_path"])),
                tui=arguments.tui,
                allow_partial=arguments.allow_partial,
                recipe=requested_name,
            )
    elif action == "execute" and not arguments.dry_run and "execution_id" not in result:
        result = blocked_execution(result, recipe=requested_name)
    if "batch_path" in result or result.get("disposition") == "reused":
        result["recipe"] = requested_name
    result = finish_cli(result)
    if arguments.as_json:
        print(json.dumps(result))
    elif "execution_id" in result:
        render_normalization_outcome(
            context.console,
            NormalizationOutcome.from_result(
                result, registering=False, manifests=execution_manifests(result)
            ),
            root=context.paths.root,
            registering=False,
        )
    else:
        if warning := result.get("execution_registry_warning"):
            context.error_console.print(str(warning), markup=False)
        render_batch(
            context.console,
            PreparationView.from_result(
                result,
                environment=database_purpose(),
                dry_run=arguments.dry_run,
                recipe=requested_name,
            ),
            root=context.paths.root,
        )
    if result["status"] == "interrupted":
        return 130
    return (
        1
        if result["status"] in {"failed", "incomplete", "blocked", "invalid"}
        or (result["status"] == "partial" and "registrations" not in result)
        else 0
    )
