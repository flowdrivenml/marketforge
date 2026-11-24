"""Shared CLI entry point for managed recipe deletion."""

import argparse
import json
import shlex
from dataclasses import asdict

from forge.historical.recipe_dependencies import inspect_references
from forge.shared.config.deletion import RecipeFamily, delete_recipe
from forge.shared.terminal.presentation import (
    display_path,
    header,
    reference,
    section,
    status_label,
    summary,
)
from forge.startup.context import ApplicationContext


def configure_delete(
    parser: argparse.ArgumentParser, *, family: RecipeFamily | None = None
) -> None:
    parser.add_argument(
        "name", metavar="NAME", help="Managed recipe name with an optional .toml suffix."
    )
    parser.add_argument(
        "--apply", action="store_true", help="Delete the recipe after checking live references."
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Allow breaking the listed references. Requires --apply.",
    )
    parser.set_defaults(service_handler=run, recipe_family=family)


def run(args: argparse.Namespace, context: ApplicationContext) -> int:
    family: RecipeFamily = args.recipe_family or (
        "acquisition" if args.command == "acquisition" else "normalization"
    )
    result = delete_recipe(
        context.paths,
        family,
        args.name,
        apply=args.apply,
        force=args.force,
        dependencies=lambda path: inspect_references(context.paths, family, path),
    )
    if args.as_json:
        print(
            json.dumps(
                {"schema_version": 1, "operation": "recipe_deletion", **asdict(result)}, default=str
            )
        )
    else:
        console, root = context.console, context.paths.root
        header(console, f"Delete {family.title()} Recipe")
        summary(
            console,
            (
                ("Recipe", result.name),
                (
                    "Status",
                    status_label(result.status, label=result.status.replace("_", " ").upper()),
                ),
            ),
        )
        reference(console, "Location", display_path(result.path, root))
        if result.dependencies.references:
            section(
                console, "Live References" if result.status != "deleted" else "References Broken"
            )
            for path in result.dependencies.references:
                console.print(display_path(path, root))
        if result.dependencies.unreadable:
            section(console, "Recipes That Could Not Be Checked")
            for path in result.dependencies.unreadable:
                console.print(display_path(path, root), style="mf.warning")
        if result.dependencies.executions:
            section(console, "Executions Requiring Original Configuration")
            for path in result.dependencies.executions:
                console.print(display_path(path, root))
        console.print()
        console.print(
            "The recipe was archived and removed from active discovery."
            if result.status == "deleted"
            else "No files have been deleted."
        )
        console.print("Historical data and retained evidence are outside the scope of deletion.")
        if result.archived_path:
            reference(console, "Archived copy", display_path(result.archived_path, root))
        scope = "merge and conversion" if family == "normalization" else "conversion"
        if family != "conversion":
            console.print(f"Reference check covered managed {scope} recipes.", style="mf.secondary")
        if result.status != "deleted" and not result.dependencies.executions:
            group = {"normalization": "normalize", "conversion": "convert"}.get(family, family)
            force = " --force" if result.status == "blocked" else ""
            reference(
                console, "Apply", f"mf {group} delete {shlex.quote(result.name)} --apply{force}"
            )
    return 1 if args.apply and result.status == "blocked" else 0
