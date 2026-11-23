"""Managed historical conversion recipe, preparation and submission commands."""

import argparse
import asyncio
import json
import sys
from contextlib import nullcontext
from pathlib import Path
from time import monotonic
from typing import Any

from pydantic import TypeAdapter, ValidationError

from forge.cli.commands.normalization_runtime import configure as configure_runtime
from forge.cli.commands.recipe_deletion import configure_delete
from forge.cli.commands.recipe_deletion import run as delete_recipe
from forge.cli.environment import add_database_options
from forge.historical.conversion import recipes, service
from forge.historical.conversion.contract import Currency
from forge.historical.conversion.evidence import (
    check_source_environment,
    inspect_publication_source,
)
from forge.historical.conversion.execution import execute
from forge.historical.conversion.recipes import PublicationSource
from forge.historical.merge.config import DatabaseID
from forge.orchestration.registry import checkpoint, cli_execution, finish_cli
from forge.shared.config.library import (
    ConfigurationNameError,
    list_config_names,
    resolve_config_file,
    run_reference,
)
from forge.shared.config.loader import ConfigurationLoadError
from forge.shared.config.recipe_editing import edit_recipe
from forge.shared.errors import MarketForgeError
from forge.shared.identity import RunOperation
from forge.shared.ownership import WorkspaceOwnership
from forge.shared.paths import ProjectPaths
from forge.shared.presentation.conversion import ConversionView, inspect_recipe
from forge.shared.terminal.conversion import render, render_inventory, render_recipe
from forge.startup.context import ApplicationContext


def configure(parser: argparse.ArgumentParser) -> None:
    actions = parser.add_subparsers(dest="convert_action", required=True)
    configure_runtime(
        actions.add_parser("config", help="Manage the base converter runtime."), operation="convert"
    )
    deletion = actions.add_parser("delete", help="Preview or delete a managed recipe.")
    configure_delete(deletion, family="conversion")
    deletion.set_defaults(service_handler=run)
    actions.add_parser("list", help="List managed conversion recipes.").set_defaults(
        service_handler=run
    )
    for action in ("new", "show", "edit", "validate", "prepare", "run"):
        child = actions.add_parser(action, allow_abbrev=False)
        child.add_argument(
            "reference",
            metavar="NAME" if action == "new" else None,
            help=(
                "Name for the new managed conversion recipe."
                if action == "new"
                else "Managed conversion recipe name or explicit path."
            ),
        )
        if action == "new":
            upstream = child.add_mutually_exclusive_group(required=True)
            for kind, label in (
                ("merge", "merge"),
                ("normalize", "normalization"),
                ("acquisition", "acquisition"),
            ):
                upstream.add_argument(
                    f"--from-{kind}",
                    metavar="SOURCE",
                    help=f"Existing {label} recipe name or TOML path.",
                )
            upstream.add_argument(
                "--from-publication",
                metavar="MERGED_PUBLICATION_ID",
                help="Registry publication ID of an eligible committed merge in the selected environment.",
            )
            child.add_argument(
                "--target-quote",
                metavar="CURRENCY",
                required=True,
                help="Target quote currency, such as USDT.",
            )
            add_database_options(child)
        if action in {"prepare", "run"}:
            add_database_options(child)
        if action == "prepare":
            child.add_argument(
                "--dry-run",
                action="store_true",
                help="Resolve the preparation plan without writing job artifacts.",
            )
        if action == "run":
            child.add_argument(
                "--tui", action="store_true", help="Open the Engine progress monitor."
            )
        child.set_defaults(service_handler=run)


def run(args: argparse.Namespace, context: ApplicationContext) -> int:
    try:
        return dispatch(args, context)
    except (
        MarketForgeError,
        ConfigurationLoadError,
        OSError,
        ValueError,
        ValidationError,
    ) as error:
        result = {"schema_version": 1, "status": "failed", "error": {"message": str(error)}}
        if args.as_json:
            print(json.dumps(result))
        else:
            render(
                context.error_console,
                ConversionView.from_result(
                    {"status": "failed", "error": str(error)},
                    args.convert_action,
                    getattr(args, "reference", "Managed recipes"),
                    0,
                ),
                context.paths.root,
            )
        return 2


def resolve_reference(paths: ProjectPaths, reference: str) -> tuple[Path, bool]:
    provided = Path(reference).expanduser()
    managed = None
    if "/" not in reference:
        try:
            managed = resolve_config_file(recipes.library(paths), reference)
        except ConfigurationNameError:
            if not provided.is_file():
                raise
    return run_reference(reference, managed)


def dispatch(args: argparse.Namespace, context: ApplicationContext) -> int:
    action = args.convert_action
    if action == "delete":
        return delete_recipe(args, context)
    if getattr(args, "tui", False):
        if args.as_json:
            raise MarketForgeError("--tui is incompatible with global --json.")
        if not interactive():
            raise MarketForgeError("--tui requires an interactive terminal.")
    if action in {"prepare", "run"} and not getattr(args, "dry_run", False):
        reference = args.reference
        resolved = resolve_reference(context.paths, reference)
        path, submitted = resolved
        if not submitted:
            recipe, _ = recipes.read_recipe(path)
            if isinstance(recipe.source, PublicationSource):
                check_source_environment(recipe.source)
        elif action == "run":
            _, document, _ = service.retained_collection(path)
            if declaration := document["pins"].get("publication_source"):
                check_source_environment(PublicationSource.model_validate(declaration))
        with cli_execution(context.paths, RunOperation.CONVERT, action, reference):
            return run_operation(args, context, resolved)
    return run_operation(args, context)


def run_operation(
    args: argparse.Namespace,
    context: ApplicationContext,
    resolved: tuple[Path, bool] | None = None,
) -> int:
    paths = context.paths
    action = args.convert_action
    tui = getattr(args, "tui", False)
    started = monotonic()
    result: dict[str, Any]
    if action == "list":
        library = recipes.library(paths)
        inventory = [
            inspect_recipe(paths, resolve_config_file(library, name))
            for name in list_config_names(library)
        ]
        if args.as_json:
            print(
                json.dumps(
                    {"schema_version": 1, "recipes": [item.document() for item in inventory]}
                )
            )
        else:
            render_inventory(context.console, inventory)
        return 0
    if action == "new":
        try:
            target = TypeAdapter(Currency).validate_python(args.target_quote)
        except ValidationError as error:
            raise MarketForgeError(
                "--target-quote (conversion.target_quote) requires a currency code using "
                "uppercase ASCII letters or digits, such as USDT."
            ) from error
        with WorkspaceOwnership(paths):
            if args.from_publication is not None:
                try:
                    if not args.from_publication.isascii() or not args.from_publication.isdecimal():
                        raise ValueError("Expected a decimal registry ID.")
                    publication_id = TypeAdapter(DatabaseID).validate_python(
                        int(args.from_publication)
                    )
                except (ValueError, ValidationError) as error:
                    raise MarketForgeError(
                        "--from-publication requires a positive registry publication ID in signed 64-bit range."
                    ) from error
                binding = asyncio.run(inspect_publication_source(paths, publication_id))
                result = recipes.create_bound(
                    paths, args.reference, binding, binding.interval, target
                )
            else:
                sources: tuple[tuple[recipes.SourceKind, str | None], ...] = (
                    ("merge", args.from_merge),
                    ("normalize", args.from_normalize),
                    ("acquisition", args.from_acquisition),
                )
                kind, reference = next(pair for pair in sources if pair[1] is not None)
                assert reference is not None
                result = recipes.create(paths, args.reference, kind, reference, target)
        if not args.as_json:
            saved = Path(str(result["path"]))
            if interactive():
                edit(context, saved)
            render_recipe(context.console, inspect_recipe(paths, saved), paths.root, action="new")
            return 0
    else:
        source, submitted = resolved or resolve_reference(paths, args.reference)
        if action in {"show", "edit"}:
            if submitted and source.suffix != ".toml":
                raise MarketForgeError(f"convert {action} requires a conversion TOML recipe.")
            if action == "edit":
                if args.as_json or not interactive():
                    raise MarketForgeError(
                        "Conversion recipe editing requires a terminal and is incompatible with --json."
                    )
                edit(context, source)
            view = inspect_recipe(paths, source)
            if args.as_json:
                print(json.dumps(view.document()))
            else:
                render_recipe(context.console, view, paths.root, action=action)
            return int(view.recipe is None)
        if submitted:
            if action == "validate":
                result = service.validate_reference(paths, source)
            elif action == "run":
                with WorkspaceOwnership(paths):
                    result = asyncio.run(execute(paths, source, tui=tui))
            else:
                raise MarketForgeError(f"convert {action} requires a conversion TOML recipe.")
        elif action == "validate":
            recipe, _, _ = recipes.load_recipe(paths, source, retain=False)
            result = {
                "status": "valid",
                "path": str(source),
                "validation": (
                    "recipe structure and local publication binding; registry not checked"
                    if isinstance(recipe.source, PublicationSource)
                    else "recipe structure, local constraints, and upstream TOML fingerprint"
                ),
                "recipe": recipe.model_dump(mode="json", exclude_none=True),
            }
        else:
            dry_run = getattr(args, "dry_run", False)
            with nullcontext() if dry_run else WorkspaceOwnership(paths):
                result = asyncio.run(service.prepare(paths, source, dry_run=dry_run))
                if action == "run" and result["status"] == "generated":
                    checkpoint(result)
                    result = asyncio.run(execute(paths, Path(result["job_path"]), tui=tui))
    result = finish_cli(result)
    if warning := result.get("execution_registry_warning"):
        context.error_console.print(str(warning), markup=False)
    if args.as_json:
        print(json.dumps(result))
    else:
        render(
            context.console,
            ConversionView.from_result(result, action, args.reference, monotonic() - started),
            paths.root,
        )
    return int(
        result["status"] in {"blocked", "failed", "interrupted"}
        or (
            action == "prepare"
            and not getattr(args, "dry_run", False)
            and result["status"] == "generated"
            and result["compatibility"] != "validated"
        )
    )


def interactive() -> bool:
    return all(stream.isatty() for stream in (sys.stdin, sys.stdout, sys.stderr))


def edit(context: ApplicationContext, path: Path) -> None:
    edit_recipe(
        context.paths,
        path,
        context.settings.presentation.editor,
        lambda draft: recipes.load_recipe(context.paths, draft, retain=False),
    )
