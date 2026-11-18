"""Managed historical merge commands."""

import argparse
import asyncio
import json
import tomllib
from pathlib import Path
from typing import Any

from rich.table import Table

from forge.historical.merge import service
from forge.historical.merge.config import DerivedRecipe, Selection, normalization_source
from forge.shared.config.library import ConfigurationNameError, resolve_config_file, run_reference
from forge.shared.config.toml import dumps
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
        if action == "new":
            child.add_argument("--from-normalize", metavar="SOURCE")
            child.add_argument("--testing", action="store_true")
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
    result: dict[str, Any]
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
            if recipe.exists():
                raise MarketForgeError(f"Merge recipe already exists: {recipe}")
            result = {"status": "template_created", "path": str(recipe)}
            document = service.TEMPLATE
            if args.from_normalize:
                if not args.testing:
                    raise MarketForgeError("--from-normalize requires --testing.")
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
            recipe.parent.mkdir(parents=True, exist_ok=True)
            try:
                with recipe.open("x", encoding="utf-8") as output:
                    output.write(document)
            except FileExistsError as error:
                raise MarketForgeError(f"Merge recipe already exists: {recipe}") from error
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
                data = tomllib.loads(source.read_text())
                request = (
                    DerivedRecipe.model_validate(data)
                    if {"version", "interval", "selection"} & data.keys()
                    else service.Recipe.model_validate(data)
                )
                if action == "validate":
                    result = {
                        "status": "valid",
                        "path": str(source),
                        **request.model_dump(mode="json", exclude_none=True),
                    }
                else:
                    result = service.prepare(paths, request, reference)
                    if action == "run" and result["status"] == "prepared":
                        result = service.execute(paths, Path(result["job_path"]), tui=tui)
    if args.as_json:
        print(json.dumps(result))
    elif "execution_id" in result:
        render_execution(context.console, result)
    elif "requested_count" in result:
        render_selection(context, result)
    else:
        context.console.print_json(data=result)
    if action == "register":
        return int(result["registration"] != "registered")
    return int(result["status"] in {"failed", "interrupted", "blocked"})


def render_selection(context: ApplicationContext, result: dict[str, Any]) -> None:
    console = context.console
    created = result["status"] == "template_created"
    console.print(
        "MarketForge / Merge recipe" if created else "MarketForge / Merge preparation",
        style="mf.heading",
    )
    table = Table.grid(padding=(0, 2))
    table.add_column(style="mf.identifier")
    table.add_column(overflow="fold")
    for label, value in (
        ("Recipe", result["recipe"]),
        ("Source", result["source"]["reference"]),
        ("Interval", result["interval_display"]),
        ("Requested", result["requested_count"]),
        ("Matched files", result["matched_count"]),
        ("Selected files", result["selected_count"]),
        ("Publications", result["publication_count"]),
        ("Configuration" if created else "Report", result.get("path", result.get("report_path"))),
        ("Compatibility", result.get("compatibility", "discovery only").upper()),
        ("Job", result.get("job_path")),
    ):
        if value is not None:
            table.add_row(label, str(value))
    console.print(table, markup=False)
    for problem in result["blockers"][:6]:
        reference = problem.get("publication_id") or problem.get("publication_ids")
        prefix = f"Publication {reference} · " if reference else ""
        console.print(prefix + problem["reason"], style="mf.warning", markup=False)
        if problem.get("suggestion"):
            console.print(problem["suggestion"], markup=False)
    if result["missing"]:
        console.print(
            f"{len(result['missing'])} requested selections have no approved files.",
            style="mf.warning",
        )
    console.print("Recipe created." if created else "No merge executed.", style="mf.body")
