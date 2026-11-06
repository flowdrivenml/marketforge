"""Testing-only application initialization using services directly."""

import argparse
import asyncio
from dataclasses import asdict

from forge.markets.ranking.coordination import ranking_lock
from forge.markets.ranking.workflow import reason, refresh_and_publish
from forge.markets.service import collect
from forge.shared.database.migrations import migrate_testing_database
from forge.shared.database.testing import testing_connection, testing_dsn
from forge.shared.ownership import WorkspaceOwnership
from forge.shared.terminal.markets import MarketCollectionDisplay, render_workflow_result
from forge.startup.context import ApplicationContext


def configure(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--testing", required=True, action="store_true")
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
        # Track scope completion independently for each collection phase.
        display.reported.clear()
        async with testing_connection(dsn) as ranking_owner:
            async with ranking_lock(ranking_owner):
                value = await refresh_and_publish(
                    dsn,
                    context.settings.http,
                    context.paths.config_dir / "markets/rankings",
                    progress=display.update,
                )
        if sync.get("status") != "complete":
            value["status"] = "partial"
        return value

    with WorkspaceOwnership(context.paths), display:
        try:
            context.paths.ensure_application_directories()
            stages.append({"stage": "directories", "status": "complete"})
            active_stage = "migrations"
            dsn = testing_dsn(context.paths.root)
            migration = migrate_testing_database(dsn=dsn)
            stages.append({"stage": "migrations", "status": "complete", **asdict(migration)})
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
