"""Validate, register and inspect manual catalogue declarations."""

import argparse
import asyncio
from contextlib import nullcontext
from pathlib import Path

from rich.text import Text

from forge.catalog import models, service
from forge.catalog.sorting import CATALOG, SPEC_SHOW
from forge.cli.commands.markets import emit
from forge.cli.environment import add_database_options
from forge.cli.sorting import add_sorting
from forge.shared.config.loader import configuration_errors
from forge.shared.database.connection import database_connection, database_dsn, database_purpose
from forge.shared.errors import MarketForgeError
from forge.shared.presentation.catalog import NAMES, project
from forge.shared.terminal.catalog import render
from forge.shared.terminal.presentation import display_path, header, reference
from forge.startup.context import ApplicationContext


def configure(parser: argparse.ArgumentParser) -> None:
    entities = parser.add_subparsers(dest="catalog_entity", required=True)
    for entity in models.MODELS:
        actions = entities.add_parser(entity).add_subparsers(dest="catalog_action", required=True)
        for action in ("add", "list", "show", "validate"):
            child = actions.add_parser(action)
            child.set_defaults(service_handler=run)
            if action != "validate":
                add_database_options(child)
            if action == "add":
                child.add_argument("--from", dest="source", type=Path, required=True)
            if action == "validate":
                child.add_argument("--file", dest="source", type=Path, required=True)
            if action == "list" or (entity == "specs" and action == "show"):
                add_sorting(
                    child,
                    SPEC_SHOW if action == "show" else CATALOG[entity],
                    notes="time means the retained source observation timestamp. Sorting applies to historical evidence."
                    if entity == "specs"
                    else "Ordering applies to the whole catalogue selection.",
                )
            if action == "show":
                child.add_argument("reference")


def run(args: argparse.Namespace, context: ApplicationContext) -> int:
    if args.as_json:
        return run_operation(args, context)
    try:
        return run_operation(args, context)
    except (MarketForgeError, OSError) as error:
        environment = "" if args.catalog_action == "validate" else database_purpose()
        header(
            context.error_console,
            f"Catalog · {NAMES[args.catalog_entity]} {args.catalog_action.title()} Failed",
            environment=environment,
        )
        message = str(error)
        if source := getattr(args, "source", None):
            reference(
                context.error_console,
                "Source declaration",
                display_path(source, context.paths.root),
            )
            message = message.removeprefix(f"{source}: ")
        context.error_console.print(Text(message, style="mf.failure"))
        return 2


def run_operation(args: argparse.Namespace, context: ApplicationContext) -> int:
    source: Path | None = getattr(args, "source", None)
    declaration = None
    if source is not None:
        with nullcontext() if args.as_json else configuration_errors(source):
            declaration = models.load(args.catalog_entity, source)

    sort, order = getattr(args, "sort", None), getattr(args, "order", None)
    ordering = None
    if args.catalog_action == "list" or (
        args.catalog_entity == "specs" and args.catalog_action == "show"
    ):
        spec = SPEC_SHOW if args.catalog_action == "show" else CATALOG[args.catalog_entity]
        ordering = spec.resolve(sort, order)

    async def execute() -> dict[str, object]:
        async with database_connection(database_dsn(context.paths.root)) as c:
            if declaration is not None:
                return await service.add(c, declaration)
            return await service.browse(
                c, args.catalog_entity, getattr(args, "reference", None), sort=sort, order=order
            )

    if args.catalog_action == "validate":
        assert declaration is not None
        result = {"valid": True, "declaration": declaration.model_dump(mode="json")}
    else:
        result = asyncio.run(execute())
    if args.as_json:
        emit(context, result, True)
    else:
        render(
            context.console,
            project(
                args.catalog_entity,
                args.catalog_action,
                result,
                environment="" if args.catalog_action == "validate" else database_purpose(),
                source=source,
                declaration=declaration.model_dump(mode="json")
                if declaration is not None
                else None,
                explicit_fields=frozenset(declaration.model_fields_set)
                if declaration is not None
                else frozenset(),
                compact=context.console.width < 100,
            ),
            context.paths.root,
            ordering=ordering.description if ordering else "",
        )
    return 0
