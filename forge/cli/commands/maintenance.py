"""Engine-independent planning, storage maintenance, run inspection and doctor."""

import argparse
import asyncio
import os
import shutil
import sys
from pathlib import Path

from pydantic import BaseModel, ConfigDict

from forge.cli.commands.markets import emit
from forge.historical.archives.config import load_acquisition
from forge.historical.archives.planning import plan
from forge.orchestration.inspection import list_runs, read_logs, read_report
from forge.shared.config.library import resolve_config_file
from forge.shared.database.backup import backup, restore
from forge.shared.database.migrations import load_migrations
from forge.shared.database.testing import testing_connection, testing_dsn
from forge.shared.errors import MarketForgeError
from forge.shared.ownership import WorkspaceOwnership
from forge.startup.context import ApplicationContext
from forge.storage.service import inventory, maintain


class Options(BaseModel):
    model_config = ConfigDict(extra="ignore")
    command: str
    maintenance_action: str = ""
    configuration_name: str | None = None
    archive_reference: str | None = None
    run_id: str | None = None
    limit: int = 100
    status: str | None = None
    origin: str | None = None
    verify_local: bool = False
    as_json: bool = False
    destination: str | None = None
    backup_path: Path | None = None
    database_command: str | None = None


def configure_plan(parser: argparse.ArgumentParser) -> None:
    parser.set_defaults(service_handler=run, maintenance_action="plan")
    parser.add_argument("configuration_name")
    parser.add_argument("--testing", action="store_true", required=True)
    parser.add_argument(
        "--cache-only",
        action="store_true",
        required=True,
        help="No HTTP requests, registry writes or file changes.",
    )
    parser.add_argument(
        "--verify-local",
        action="store_true",
        help="Read-only integrity verification of the bounded plan sample.",
    )
    parser.add_argument("--limit", type=int, default=100)


def configure_storage(parser: argparse.ArgumentParser) -> None:
    from forge.cli.commands.storage import configure

    configure(parser)


def configure_runs(parser: argparse.ArgumentParser) -> None:
    actions = parser.add_subparsers(dest="maintenance_action", required=True)
    for action in ("list", "show", "logs", "report"):
        child = actions.add_parser(action)
        child.set_defaults(service_handler=run)
        if action != "list":
            child.add_argument("run_id")
        child.add_argument("--limit", type=int, default=100)


def configure_doctor(parser: argparse.ArgumentParser) -> None:
    parser.set_defaults(service_handler=run)
    parser.add_argument("--testing", required=True, action="store_true")


def configure_backup(parser: argparse.ArgumentParser, *, restoring: bool) -> None:
    parser.set_defaults(service_handler=run)
    parser.add_argument("--testing", required=True, action="store_true")
    parser.add_argument(
        "backup_path",
        type=Path,
        help="New backup directory or existing verified backup for restore.",
    )
    if restoring:
        parser.add_argument(
            "--destination",
            required=True,
            help="Fresh marketforge_restore_<hex> disposable database only.",
        )
        parser.add_argument("--disposable-testing", required=True, action="store_true")


def run(arguments: argparse.Namespace, context: ApplicationContext) -> int:
    o = Options.model_validate(vars(arguments))
    if o.command == "runs":
        if o.maintenance_action == "list":
            value: object = {"runs": list_runs(context.paths.runs_dir, o.limit)}
        elif o.run_id is None:
            raise MarketForgeError("Run ID required.")
        elif o.maintenance_action == "logs":
            value = read_logs(context.paths.runs_dir, o.run_id, o.limit)
        else:
            value = read_report(context.paths.runs_dir, o.run_id)
        emit(context, value, o.as_json)
        return 0
    dsn = testing_dsn(context.paths.root)
    if o.command == "database":
        if o.backup_path is None:
            raise MarketForgeError("Backup path is required.")
        with WorkspaceOwnership(context.paths):
            if o.database_command == "backup":
                value = backup(dsn, context.paths, o.backup_path)
            else:
                if o.destination is None:
                    raise MarketForgeError("Disposable destination is required.")
                value = restore(dsn, o.backup_path, o.destination)
        emit(context, value, o.as_json)
        return 0

    async def execute() -> dict[str, object]:
        async with testing_connection(dsn) as c:
            if o.command == "doctor":
                cur = await c.execute(
                    "SELECT version FROM forge_meta.schema_migrations WHERE stream='core' ORDER BY version"
                )
                applied = [r[0] for r in await cur.fetchall()]
                from forge.shared.database.migrations import AppliedMigration, validate_history

                history = await c.execute(
                    "SELECT version,name,checksum FROM forge_meta.schema_migrations WHERE stream='core' ORDER BY version"
                )
                validate_history(
                    load_migrations(),
                    tuple(AppliedMigration(*row) for row in await history.fetchall()),
                )
                expected = [m.version for m in load_migrations()]
                base = context.paths.data_root
                while not base.exists():
                    base = base.parent
                return {
                    "python": sys.version.split()[0],
                    "python_ready": sys.version_info >= (3, 12),
                    "testing_database": "verified marketforge_test / testing",
                    "applied_migrations": applied,
                    "pending_migrations": [v for v in expected if v not in applied],
                    "storage_parent": str(base),
                    "storage_writable": os.access(base, os.W_OK),
                    "free_bytes": shutil.disk_usage(base).free,
                    "postgres_tools": {
                        "pg_dump": shutil.which("pg_dump") is not None,
                        "pg_restore": shutil.which("pg_restore") is not None,
                    },
                    "engine_readiness": "pending: Rust contracts and standalone profile validation are being refactored; no engine execution attempted.",
                }
            if o.maintenance_action == "plan":
                if o.configuration_name is None:
                    raise MarketForgeError("Acquisition name is required.")
                config = load_acquisition(
                    resolve_config_file(context.paths.acquisition_config_dir, o.configuration_name)
                )
                return await plan(
                    c, config, context.paths.raw_dir, verify_local=o.verify_local, limit=o.limit
                )
            archive_id = None
            if o.archive_reference is not None:
                try:
                    archive_id = int(o.archive_reference.removeprefix("local:"))
                except ValueError:
                    raise MarketForgeError(
                        "Archive reference must be a local registry ID."
                    ) from None
            if o.maintenance_action in {"verify", "reconcile"}:
                return await maintain(
                    c,
                    root=context.paths.raw_dir,
                    reconcile=o.maintenance_action == "reconcile",
                    limit=o.limit,
                    archive_id=archive_id,
                )
            return {
                "archives": await inventory(
                    c,
                    root=context.paths.raw_dir,
                    limit=o.limit,
                    archive_id=archive_id,
                    status=o.status,
                    origin=o.origin,
                ),
                "verification": "Stored registry records; not reverified during browsing.",
            }

    if o.maintenance_action in {"verify", "reconcile"}:
        with WorkspaceOwnership(context.paths):
            value = asyncio.run(execute())
    else:
        value = asyncio.run(execute())
    emit(context, value, o.as_json)
    if o.maintenance_action in {"verify", "reconcile"}:
        from forge.markets.models import array, obj

        document = value
        for raw in array(document.get("outcomes")):
            outcome = obj(raw)
            if (
                "error" in outcome
                or outcome.get("status") in {"failed", "corrupt", "missing"}
                or outcome.get("verification")
                in {
                    "missing",
                    "corrupt",
                    "unverifiable: no committed checksum",
                }
            ):
                return 1
    return 0
