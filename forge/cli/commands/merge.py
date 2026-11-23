"""Managed historical merge commands."""

import argparse
import asyncio
import json
import os
import sys
from contextlib import nullcontext
from pathlib import Path
from typing import Any

from forge.cli.commands.normalization_runtime import configure as configure_runtime
from forge.cli.commands.recipe_deletion import configure_delete
from forge.cli.environment import add_database_options
from forge.historical.engine.runtime_config import directory_fd
from forge.historical.merge import service
from forge.historical.merge.config import DerivedRecipe, Selection, normalization_source
from forge.orchestration.registry import checkpoint, cli_execution, finish_cli
from forge.shared.config.library import (
    ConfigurationNameError,
    config_filename,
    list_config_names,
    resolve_config_file,
    run_reference,
)
from forge.shared.config.loader import ConfigurationLoadError
from forge.shared.config.recipe_editing import edit_recipe as edit_recipe_draft
from forge.shared.config.toml import dumps
from forge.shared.errors import MarketForgeError
from forge.shared.identity import RunOperation
from forge.shared.ownership import WorkspaceOwnership
from forge.shared.presentation.merge import MergeView, RecipeView
from forge.shared.terminal.merge import render_inventory, render_merge, render_recipe
from forge.startup.context import ApplicationContext


def configure(parser: argparse.ArgumentParser) -> None:
    actions = parser.add_subparsers(dest="merge_action", required=True)
    configure_runtime(
        actions.add_parser("config", help="Manage the base merge Engine runtime."),
        operation="merge",
    )
    configure_delete(
        actions.add_parser("delete", help="Preview or delete one managed merge recipe."),
        family="merge",
    )
    actions.add_parser(
        "list", help="List managed merge recipes and their structural state."
    ).set_defaults(service_handler=run)
    reference_help = {
        "new": "Name for the new managed merge recipe.",
        "validate": "Managed merge recipe name or TOML path.",
        "show": "Managed merge recipe name or TOML path.",
        "edit": "Managed merge recipe name or TOML path.",
        "prepare": "Managed merge recipe name or TOML path.",
        "run": "Managed recipe name, TOML path or prepared job JSON path.",
        "register": "Retained execution-intent.json, committed publication, manifest or primary-file path.",
    }
    for action in ("new", "show", "edit", "validate", "prepare", "run", "register"):
        child = actions.add_parser(action, allow_abbrev=False)
        child.add_argument("reference", help=reference_help[action])
        if action in {"prepare", "run", "register"}:
            add_database_options(child)
        if action == "new":
            child.add_argument(
                "--from-normalize",
                metavar="SOURCE",
                help="Derive selection from a normalization recipe name or path.",
            )
            add_database_options(child)
        if action == "prepare":
            child.add_argument(
                "--dry-run",
                action="store_true",
                help="Inspect selection without writing a job, report, or run record.",
            )
        if action == "run":
            child.add_argument(
                "--tui", action="store_true", help="Open the Engine progress monitor."
            )
        child.set_defaults(service_handler=run)


def run(args: argparse.Namespace, context: ApplicationContext) -> int:
    action = args.merge_action
    if action == "list":
        return inventory(context, as_json=args.as_json)
    if args.as_json and getattr(args, "tui", False):
        raise MarketForgeError("--tui is incompatible with global --json.")
    if action == "register":
        with (
            WorkspaceOwnership(context.paths),
            cli_execution(context.paths, RunOperation.MERGE, action, args.reference),
        ):
            return run_operation(args, context, owns_workspace=True)
    if action in {"prepare", "run"} and not getattr(args, "dry_run", False):
        reference = getattr(args, "reference", None) or getattr(args, "configuration_name", "")
        with cli_execution(context.paths, RunOperation.MERGE, action, reference):
            return run_operation(args, context)
    return run_operation(args, context)


def run_operation(
    args: argparse.Namespace, context: ApplicationContext, *, owns_workspace: bool = False
) -> int:
    paths = context.paths
    action = args.merge_action
    reference = args.reference
    supplied = Path(reference).expanduser()
    tui = getattr(args, "tui", False)
    result: dict[str, Any]
    if action == "register":
        with nullcontext() if owns_workspace else WorkspaceOwnership(paths):
            result = asyncio.run(service.register(paths, supplied))
    else:
        library = paths.workflow_dir("merge")
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
            recipe = library / config_filename(reference)
            if recipe.exists() or recipe.is_symlink():
                raise MarketForgeError(f"Merge recipe already exists: {recipe}")
            result = {"status": "template_created", "path": str(recipe)}
            document = service.TEMPLATE
            if args.from_normalize:
                source_path, config, fingerprint = normalization_source(paths, args.from_normalize)
                derived = DerivedRecipe(
                    version=1,
                    interval=config.interval,
                    selection=Selection(
                        source=str(source_path)
                        if "/" in args.from_normalize
                        else args.from_normalize
                    ),
                )
                result.update(service.source_selection(paths, derived), recipe=reference)
                if result["source"]["sha256"] != fingerprint:
                    raise MarketForgeError(
                        "Normalization configuration changed during recipe creation."
                    )
                document = (
                    "# Narrow this selection before preparing if discovery reports blockers.\n"
                    "version = 1\n\n[interval]\n"
                    + dumps(derived.interval.model_dump())
                    + "\n[selection]\n"
                    + dumps(derived.selection.model_dump(exclude_none=True))
                )
            try:
                with WorkspaceOwnership(paths):
                    with directory_fd(recipe.parent, create=True) as fd:
                        handle = os.open(
                            recipe.name,
                            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                            0o666,
                            dir_fd=fd,
                        )
                        with os.fdopen(handle, "w", encoding="utf-8") as output:
                            output.write(document)
            except FileExistsError as error:
                raise MarketForgeError(f"Merge recipe already exists: {recipe}") from error
            if not args.as_json and all(
                stream.isatty() for stream in (sys.stdin, sys.stdout, sys.stderr)
            ):
                edit_recipe(context, recipe)
            if not args.as_json:
                view = RecipeView(recipe.stem, recipe, service.load_recipe(recipe, retain=False))
                if "requested_count" in result:
                    render_merge(
                        context.console,
                        MergeView.from_result(result, action="discovery", reference=reference),
                        root=paths.root,
                    )
                else:
                    render_recipe(context.console, view, action=action, root=paths.root)
                return 0
        else:
            if action == "run":
                source, submitted = run_reference(reference, recipe)
            else:
                source = supplied if explicit else recipe
                if source is None or not source.is_file():
                    raise MarketForgeError(
                        f"Merge recipe file is unavailable: {source or library / reference}"
                    )
                submitted = False
            if submitted:
                with WorkspaceOwnership(paths):
                    result = service.execute(paths, source, tui=tui)
            else:
                if action == "edit":
                    if not explicit:
                        source = library / config_filename(reference)
                    if args.as_json or not all(
                        stream.isatty() for stream in (sys.stdin, sys.stdout, sys.stderr)
                    ):
                        raise MarketForgeError(
                            "Merge recipe editing requires a terminal and is incompatible with --json."
                        )
                    edit_recipe(context, source)
                if action in {"validate", "show", "edit"}:
                    try:
                        request = service.load_recipe(source, retain=False)
                        if action != "show":
                            service.validate_recipe(paths, request)
                    except (ConfigurationLoadError, MarketForgeError, OSError) as error:
                        if args.as_json:
                            raise
                        render_recipe(
                            context.error_console,
                            RecipeView(source.stem, source, error=str(error)),
                            action=action,
                            root=paths.root,
                        )
                        return 2
                    if not args.as_json:
                        render_recipe(
                            context.console,
                            RecipeView(source.stem, source, request),
                            action=action,
                            root=paths.root,
                        )
                        return 0
                    result = {
                        "status": "valid",
                        "path": str(source),
                        **request.model_dump(mode="json", exclude_none=True),
                    }
                else:
                    dry = getattr(args, "dry_run", False)
                    request = service.load_recipe(source)
                    with nullcontext() if dry else WorkspaceOwnership(paths):
                        result = (
                            service.prepare(paths, request, reference, dry_run=True)
                            if dry
                            else service.prepare(paths, request, reference)
                        )
                        if action == "run" and result["status"] == "prepared":
                            checkpoint(result)
                            result = service.execute(paths, Path(result["job_path"]), tui=tui)
    result = finish_cli(result)
    if args.as_json:
        print(json.dumps(result))
    else:
        render_merge(
            context.console,
            MergeView.from_result(result, action=action, reference=reference),
            root=paths.root,
        )
    if action == "register":
        return int(result["registration"] != "registered")
    return int(
        result["status"] in {"failed", "interrupted", "blocked"}
        or (action == "run" and result["status"] == "generated")
    )


def inventory(context: ApplicationContext, *, as_json: bool) -> int:
    library = context.paths.workflow_dir("merge")
    items: list[RecipeView] = []
    for name in list_config_names(library):
        path = library / f"{name}.toml"
        try:
            items.append(RecipeView(name, path, service.load_recipe(path, retain=False)))
        except (ConfigurationLoadError, OSError) as error:
            items.append(RecipeView(name, path, error=str(error)))
    if as_json:
        print(json.dumps({"schema_version": 1, "recipes": [item.machine() for item in items]}))
    else:
        render_inventory(context.console, items)
    return 0


def edit_recipe(context: ApplicationContext, source: Path) -> None:
    def validate(draft: Path) -> None:
        request = service.load_recipe(draft, retain=False)
        service.validate_recipe(context.paths, request)

    edit_recipe_draft(context.paths, source, context.settings.presentation.editor, validate)
