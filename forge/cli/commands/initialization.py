"""Initialize selected workspace services."""

import argparse
import asyncio
from dataclasses import asdict

from forge.cli.environment import add_database_options
from forge.historical.metadata.service import enrich_registry
from forge.markets.ranking.coordination import ranking_lock
from forge.markets.ranking.workflow import reason, refresh_and_publish
from forge.markets.service import collect
from forge.shared.database.connection import database_connection, database_dsn
from forge.shared.database.migrations import migrate_testing_database
from forge.shared.database.status import require_initialized_production
from forge.shared.ownership import WorkspaceOwnership
from forge.shared.terminal.markets import MarketCollectionDisplay, render_workflow_result
from forge.startup.context import ApplicationContext


def configure(parser: argparse.ArgumentParser) -> None:
    add_database_options(parser)
    parser.add_argument("--silent", action="store_true")
    parser.set_defaults(service_handler=run)


def run(arguments: argparse.Namespace, context: ApplicationContext) -> int:
    from forge.cli.commands.markets import emit

    stages: list[dict[str, object]] = []
    value: dict[str, object]
    active_stage = "directories"
    display = MarketCollectionDisplay(
        context.error_console, silent=arguments.silent or arguments.as_json
    )

    async def initialize(dsn: str) -> dict[str, object]:
        nonlocal active_stage
        sync = await collect(dsn, context.settings.http, metadata=True, progress=display.update)
        stages.append({"stage": "catalogue", **sync})
        if sync.get("status") == "cancelled":
            return {
                "schema_version": 1,
                "status": "cancelled",
                "rankings": [
                    {"policy": name, "status": "failed", "error": "Metadata collection cancelled."}
                    for name in ("turnover", "volume")
                ],
            }
        active_stage = "historical_metadata"
        try:
            enrichment = await enrich_registry(dsn, context.paths)
        except Exception as error:
            enrichment = {"status": "failed", "error": reason(error)}
        stages.append({"stage": "historical_metadata", **enrichment})
        active_stage = "rankings"
        # Track scope completion independently for each collection phase.
        display.reported.clear()
        async with database_connection(dsn) as ranking_owner:
            async with ranking_lock(ranking_owner):
                value = await refresh_and_publish(
                    dsn,
                    context.settings.http,
                    context.paths.config_dir / "markets/rankings",
                    progress=display.update,
                )
        if sync.get("status") != "complete" or enrichment.get("status") != "complete":
            value["status"] = "partial"
        return value

    with WorkspaceOwnership(context.paths), display:
        try:
            context.paths.ensure_application_directories()
            stages.append({"stage": "directories", "status": "complete"})
            active_stage = "migrations"
            if context.testing:
                dsn = database_dsn(context.paths.root)
                migration = migrate_testing_database(dsn=dsn)
                stages.append({"stage": "migrations", "status": "complete", **asdict(migration)})
            else:
                require_initialized_production(context.paths.root)
                dsn = database_dsn(context.paths.root)
            active_stage = "catalogue"
            value = asyncio.run(initialize(dsn))
        except Exception as error:
            stages.append(
                {
                    "stage": active_stage,
                    "status": "failed",
                    "error": reason(error),
                }
            )
            value = {
                "schema_version": 1,
                "status": "failed",
                "rankings": [
                    {
                        "policy": name,
                        "status": "failed",
                        "error": "Initialization prerequisite failed; see stages.",
                    }
                    for name in ("turnover", "volume")
                ],
            }
    value["stages"] = stages
    if arguments.as_json:
        emit(context, value, True)
    else:
        render_workflow_result(context.console, "Initialization", value)
    return 130 if value["status"] == "cancelled" else 0 if value["status"] == "complete" else 1
