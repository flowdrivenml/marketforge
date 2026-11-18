"""Manual catalogue declarations and testing registry inspection."""

import argparse
import asyncio
from pathlib import Path

from forge.catalog import models, service
from forge.cli.commands.markets import emit
from forge.shared.database.testing import testing_connection, testing_dsn
from forge.startup.context import ApplicationContext


def configure(parser: argparse.ArgumentParser) -> None:
    entities = parser.add_subparsers(dest="catalog_entity", required=True)
    for entity in models.MODELS:
        actions = entities.add_parser(entity).add_subparsers(dest="catalog_action", required=True)
        for action in ("add", "list", "show", "validate"):
            child = actions.add_parser(action)
            child.set_defaults(service_handler=run)
            if action != "validate":
                child.add_argument("--testing", action="store_true", required=True)
            if action == "add":
                child.add_argument("--from", dest="source", type=Path, required=True)
            if action == "validate":
                child.add_argument("--file", dest="source", type=Path, required=True)
            if action == "show":
                child.add_argument("reference")


def run(args: argparse.Namespace, context: ApplicationContext) -> int:
    declaration = (
        models.load(args.catalog_entity, args.source)
        if args.catalog_action in {"validate", "add"}
        else None
    )

    async def execute() -> dict[str, object]:
        async with testing_connection(testing_dsn(context.paths.root)) as c:
            if declaration is not None:
                return await service.add(c, declaration)
            return await service.browse(c, args.catalog_entity, getattr(args, "reference", None))

    if args.catalog_action == "validate":
        assert declaration is not None
        result = {"valid": True, "declaration": declaration.model_dump(mode="json")}
    else:
        result = asyncio.run(execute())
    emit(context, result, args.as_json)
    return 0
