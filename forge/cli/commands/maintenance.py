"""Engine-independent planning, storage maintenance, run inspection and doctor."""

import argparse
import asyncio
import json
from pathlib import Path
from typing import Any, TextIO, cast

from pydantic import BaseModel, ConfigDict

from forge.cli.commands.markets import emit
from forge.cli.environment import add_database_options
from forge.cli.sorting import add_sorting
from forge.historical.archives.config import load_acquisition
from forge.historical.archives.planning import plan
from forge.orchestration import run_delete
from forge.orchestration.registry import list_index
from forge.orchestration.registry_maintenance import MaintenanceEvidence, index_legacy, reconcile
from forge.orchestration.runs import inspect_run_evidence
from forge.orchestration.sorting import RUNS
from forge.shared.config.library import resolve_config_file
from forge.shared.database.backup import backup, restore
from forge.shared.database.connection import database_connection, database_dsn, database_purpose
from forge.shared.errors import MarketForgeError
from forge.shared.identity import RunOperation
from forge.shared.ownership import WorkspaceOwnership
from forge.shared.presentation import runs as run_views
from forge.shared.terminal import runs as run_terminal
from forge.shared.terminal.acquisition_result import render_plan
from forge.shared.terminal.console import create_console
from forge.shared.terminal.doctor import render_doctor
from forge.startup.context import ApplicationContext
from forge.startup.diagnostics import doctor
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
    parser.add_argument("configuration_name", help="Saved acquisition recipe name.")
    add_database_options(parser)
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
    parser.add_argument(
        "--limit", type=int, default=100, help="Maximum sampled archives (1–10000)."
    )


def configure_storage(parser: argparse.ArgumentParser) -> None:
    from forge.cli.commands.storage import configure

    configure(parser)


def configure_runs(parser: argparse.ArgumentParser) -> None:
    actions = parser.add_subparsers(dest="maintenance_action", required=True)
    deletion = actions.add_parser("delete", help="Review or apply an exact run deletion plan.")
    deletion.set_defaults(service_handler=run)
    add_database_options(deletion)
    deletion.add_argument(
        "run_ids", nargs="*", metavar="RUN_ID", help="Exact indexed run references for preview."
    )
    deletion.add_argument(
        "--with-children", action="store_true", help="Review indexed descendants too."
    )
    deletion.add_argument(
        "--history-only", action="store_true", help="Hide history and preserve evidence."
    )
    deletion.add_argument("--apply", action="store_true", help="Apply the exact saved plan.")
    deletion.add_argument("--plan", type=Path, help="Saved managed deletion plan.")
    for action, description in (
        ("list", "List runs in the selected registry."),
        ("show", "Inspect a run, its attempts and resources."),
        ("logs", "Read bounded retained logs."),
        ("report", "Read the retained detailed report."),
        ("index-legacy", "Preview or index verified historical evidence."),
        ("reconcile", "Preview or finalize retained execution outcomes."),
    ):
        child = actions.add_parser(action, help=description)
        child.set_defaults(service_handler=run)
        add_database_options(child)
        if action in {"show", "logs", "report"}:
            child.add_argument("run_id")
        child.add_argument(
            "--limit",
            type=int,
            default=100,
            help="Maximum indexed rows or event-prefix records, where applicable.",
        )
        if action == "report":
            child.add_argument(
                "--section",
                choices=("all", "summary", "inputs", "outputs", "diagnostics", "artifacts"),
                default="all",
                help="Focus human output on one section. JSON retains the full report.",
            )
        if action in {"list", "index-legacy", "reconcile"}:
            child.add_argument("--offset", type=int, default=0)
        if action == "list":
            add_sorting(
                child,
                RUNS,
                notes="time is the recorded run start. Sorting is global before paging.",
            )
            child.add_argument(
                "--operation", choices=[str(operation) for operation in RunOperation]
            )
            for key in ("action", "status", "recipe", "start", "end"):
                child.add_argument(f"--{key}")
        elif action in {"index-legacy", "reconcile"}:
            if action == "reconcile":
                child.add_argument(
                    "--run-id", help="Review the latest attempt of one exact indexed run."
                )
            mode = child.add_mutually_exclusive_group()
            mode.add_argument(
                "--dry-run", action="store_true", help="Preview without writing (default)."
            )
            mode.add_argument(
                "--apply", action="store_true", help="Apply verified metadata changes."
            )


def configure_doctor(parser: argparse.ArgumentParser) -> None:
    add_database_options(parser)


def run_doctor(
    *, project_root: Path | None, config_file: Path | None, testing: bool, as_json: bool
) -> int:
    report = doctor(project_root=project_root, config_file=config_file, testing=testing)
    if as_json:
        print(json.dumps(report.payload(), indent=2, ensure_ascii=False))
    else:
        render_doctor(create_console(report.presentation), report)
    return report.exit_code


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


def run_history(
    arguments: argparse.Namespace, context: ApplicationContext, options: Options
) -> int:
    action = options.maintenance_action
    environment = database_purpose()
    value: dict[str, Any]
    console = None if options.as_json else context.console
    if (
        console is not None
        and console.color_system is not None
        and (not console.file.isatty() or console.is_dumb_terminal)
    ):
        settings = context.settings.presentation.model_copy(update={"color": "never"})
        console = create_console(settings, stream=cast(TextIO, console.file))
    if action == "delete":
        if arguments.apply:
            if (
                not arguments.plan
                or arguments.run_ids
                or arguments.history_only
                or arguments.with_children
            ):
                raise MarketForgeError(
                    "Apply requires --plan and takes selection and mode from that plan."
                )
            value = asyncio.run(run_delete.apply(context.paths, arguments.plan))
        else:
            if arguments.plan:
                raise MarketForgeError("--plan requires --apply.")
            value = asyncio.run(
                run_delete.preview(
                    context.paths,
                    arguments.run_ids,
                    children=arguments.with_children,
                    history_only=arguments.history_only,
                )
            )
        if options.as_json:
            emit(context, value, True)
        elif console is not None:
            run_terminal.render_deletion(console, value)
        return int(value["status"] in {"blocked", "partial", "in_progress"})
    if action == "list":
        names = ("operation", "action", "status", "recipe", "start", "end")
        filters = {name: getattr(arguments, name) for name in names}
        value = asyncio.run(
            list_index(
                context.paths,
                limit=options.limit,
                offset=arguments.offset,
                sort=arguments.sort,
                order=arguments.order,
                **filters,
            )
        )
        if console is not None:
            view = run_views.inventory(
                value,
                tuple((name, val) for name, val in filters.items() if val is not None),
                environment,
                options.limit,
                sort=arguments.sort,
                order=arguments.order,
            )
            run_terminal.render_inventory(console, view, environment)
    elif action in {"index-legacy", "reconcile"}:
        service = index_legacy if action == "index-legacy" else reconcile
        collected = MaintenanceEvidence() if console is not None else None
        extra: dict[str, Any] = {"evidence": collected} if collected is not None else {}
        if action == "reconcile":
            extra["run_id"] = arguments.run_id
        if arguments.apply:
            with WorkspaceOwnership(context.paths):
                value = asyncio.run(
                    service(
                        context.paths,
                        limit=options.limit,
                        offset=arguments.offset,
                        apply=True,
                        **extra,
                    )
                )
        else:
            value = asyncio.run(
                service(
                    context.paths,
                    limit=options.limit,
                    offset=arguments.offset,
                    apply=False,
                    **extra,
                )
            )
        if console is not None:
            run_terminal.render_maintenance(
                console, run_views.maintenance(value, action, collected), environment
            )
    elif options.run_id is None:
        raise MarketForgeError("Run ID required.")
    else:
        inspection = asyncio.run(
            inspect_run_evidence(
                context.paths,
                options.run_id,
                action=action,
                limit=options.limit,
                human=not options.as_json,
            )
        )
        value = inspection.evidence
        if console is not None:
            if inspection.removal and inspection.removal["mode"] == "full":
                run_terminal.render_deleted_run(
                    console, inspection.name, inspection.removal, environment
                )
                return 0
            if action == "logs":
                run_terminal.render_logs(console, run_views.logs(inspection), environment)
            elif action == "show" and inspection.record is not None:
                run_terminal.render_detail(
                    console,
                    run_views.detail(inspection, context.paths.root, environment),
                    environment,
                )
            else:
                run_terminal.render_report(
                    console,
                    run_views.report(inspection, context.paths.root, environment),
                    environment,
                    selected=getattr(arguments, "section", "all"),
                )
    if options.as_json:
        emit(context, value, True)
    if action == "reconcile" and arguments.run_id is not None:
        return int(
            any(
                item["status"] not in {"reconciled", "already_reconciled", "retained_outcome"}
                for item in value["outcomes"]
            )
        )
    return 0


def run(arguments: argparse.Namespace, context: ApplicationContext) -> int:
    o = Options.model_validate(vars(arguments))
    if o.command == "runs":
        return run_history(arguments, context, o)
    dsn = database_dsn(context.paths.root)
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
        async with database_connection(dsn) as c:
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
                    status=o.status,
                    origin=o.origin,
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
    if o.maintenance_action == "plan" and not o.as_json:
        render_plan(
            context.console, value, name=o.configuration_name or "", environment=database_purpose()
        )
    else:
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
