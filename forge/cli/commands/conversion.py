"""Managed historical conversion recipe, preparation and submission commands."""

import argparse
import asyncio
import json
from pathlib import Path
from typing import Any

from forge.historical.conversion import recipes, service
from forge.historical.conversion.execution import execute
from forge.historical.engine.managed import resolve
from forge.shared.config.library import ConfigurationNameError, resolve_config_file, run_reference
from forge.shared.errors import MarketForgeError
from forge.startup.context import ApplicationContext


def configure(parser: argparse.ArgumentParser) -> None:
    actions = parser.add_subparsers(dest="convert_action", required=True)
    for action in ("new", "validate", "prepare", "run"):
        child = actions.add_parser(action, allow_abbrev=False)
        child.add_argument("reference")
        if action == "new":
            upstream = child.add_mutually_exclusive_group(required=True)
            for kind in ("merge", "normalize", "acquisition"):
                upstream.add_argument(f"--from-{kind}", metavar="SOURCE")
            child.add_argument("--target-quote", required=True)
            child.add_argument("--testing", action="store_true")
        if action in {"prepare", "run"}:
            child.add_argument("--testing", required=True, action="store_true")
        if action == "prepare":
            child.add_argument("--dry-run", action="store_true")
        if action == "run":
            child.add_argument("--tui", action="store_true")
        child.set_defaults(service_handler=run)


def run(args: argparse.Namespace, context: ApplicationContext) -> int:
    paths = context.paths
    action = args.convert_action
    tui = getattr(args, "tui", False)
    if args.as_json and tui:
        raise MarketForgeError("--tui is incompatible with global --json.")
    result: dict[str, Any]
    if action == "new":
        sources: tuple[tuple[recipes.SourceKind, str | None], ...] = (
            ("merge", args.from_merge),
            ("normalize", args.from_normalize),
            ("acquisition", args.from_acquisition),
        )
        kind, reference = next(pair for pair in sources if pair[1] is not None)
        assert reference is not None
        result = recipes.create(paths, args.reference, kind, reference, args.target_quote)
    else:
        reference = args.reference
        provided = Path(reference).expanduser()
        managed = None
        if "/" not in reference:
            try:
                managed = resolve_config_file(recipes.library(paths), reference)
            except ConfigurationNameError:
                if not provided.is_file():
                    raise
        source, submitted = run_reference(reference, managed)
        if submitted:
            if action == "validate":
                result = {
                    "status": "valid",
                    "path": str(source),
                    "validation": service.validate_prepared_job(resolve(paths, "convert"), source),
                }
            elif action == "run":
                result = asyncio.run(execute(paths, source, tui=tui))
            else:
                raise MarketForgeError(f"convert {action} requires a conversion TOML recipe.")
        elif action == "validate":
            recipe, _, _ = recipes.load_recipe(paths, source)
            result = {
                "status": "valid",
                "path": str(source),
                "validation": "recipe structure, local constraints, and upstream TOML fingerprint",
                "recipe": recipe.model_dump(exclude_none=True),
            }
        else:
            result = asyncio.run(
                service.prepare(paths, source, dry_run=getattr(args, "dry_run", False))
            )
            if action == "run" and result["status"] == "generated":
                result = asyncio.run(execute(paths, Path(result["job_path"]), tui=tui))
    if args.as_json:
        print(json.dumps(result))
    else:
        render(context, result)
    return int(result["status"] in {"blocked", "failed", "interrupted"})


def render(context: ApplicationContext, result: dict[str, Any]) -> None:
    console = context.console
    console.print(f"Conversion {result['status']}", style="mf.heading", markup=False)
    for key in ("path", "validation", "report_path", "job_path", "collection_path"):
        if result.get(key):
            console.print(str(result[key]), markup=False)
    for publication in result.get("sources", []):
        console.print(
            f"Registry publication {publication['registry_publication_id']} · "
            f"engine {publication['engine_publication_id']} · "
            f"{publication['events']:,} events",
            markup=False,
        )
    for job in result.get("jobs", []):
        if validation := job.get("validation_output"):
            console.print(str(validation).strip(), markup=False)
        if path := job.get("result_path"):
            console.print(path, markup=False)
    for message in [*result.get("blockers", []), *result.get("compatibility_blockers", [])]:
        console.print(str(message), style="mf.warning", markup=False)
