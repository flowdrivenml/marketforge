"""Reviewed cleanup previews and explicit, guarded application."""

import argparse
import asyncio
import shlex
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict
from rich.prompt import Confirm

from forge.cli.commands.markets import emit
from forge.cli.commands.storage import filters, local_identity, utc_json
from forge.markets.models import obj
from forge.shared.database.testing import testing_connection, testing_dsn
from forge.shared.errors import MarketForgeError
from forge.shared.ownership import OwnershipConflictError, WorkspaceOwnership
from forge.shared.terminal.storage_cleanup import render_cleanup
from forge.startup.context import ApplicationContext
from forge.storage.cleanup import apply, preview, totals, validate_selection
from forge.storage.cleanup_plans import load_plan, save_plan
from forge.storage.dependencies import DEPENDENCY_CHECKS
from forge.storage.models import StorageFilters


class Options(BaseModel):
    model_config = ConfigDict(extra="ignore")
    storage_action: Literal["prune", "delete"]
    testing: bool = False
    archive_reference: str | None = None
    apply: bool = False
    preview: Path | None = None
    all_targets: bool = False
    overlap: bool = False
    as_json: bool = False


def configure(add_parser: Callable[..., argparse.ArgumentParser]) -> None:
    for action in ("prune", "delete"):
        child = add_parser(
            action,
            argument_default=argparse.SUPPRESS,
            allow_abbrev=False,
            help="Preview retirement of failed downloads and cleanup of owned partials."
            if action == "prune"
            else "Preview deletion of selected managed archive copies.",
        )
        child.set_defaults(service_handler=run, storage_action=action)
        child.add_argument("--testing", action="store_true")
        filters(child, cleanup=True, containment=action == "delete")
        child.add_argument(
            "--status",
            choices=(
                "pending",
                "downloading",
                "complete",
                "missing",
                "corrupt",
                "failed",
                "deleted",
            ),
            help="Failed attempts by default; active/published attempts stay blocked."
            if action == "prune"
            else "Optional status filter; not sufficient as a bulk selector.",
        )
        child.add_argument(
            "--apply",
            action="store_true",
            help="Prune: preview and confirm once in a terminal. Scripts/delete: apply the exact saved --preview file.",
        )
        child.add_argument(
            "--preview",
            type=Path,
            help="Saved managed preview for scripted --apply; delete always requires it.",
        )
        child.add_argument(
            "--all",
            dest="all_targets",
            action="store_true",
            help="Show and review every selected target. Pagination never narrows target selection.",
        )
        if action == "delete":
            child.description = "Preview-only by default. Date filters require full archive containment unless --overlap is explicit. Apply requires a fully reviewed saved preview. Concrete dependencies, active managed operations and unsafe/changed targets remain blocked."
            child.add_argument(
                "archive_reference",
                nargs="?",
                help="Single managed local:ID; other filters combine with AND.",
            )
            child.add_argument(
                "--overlap",
                action="store_true",
                help="Use half-open overlap instead of containment; requires a date bound.",
            )
        else:
            child.description = "Preview failed-download retirement by default. --apply shows all targets and asks one terminal confirmation, then retires eligible failed records with or without partials. For scripts use --apply --preview PATH."
            child.add_argument("--archive", dest="archive_reference")


def interactive_terminal() -> bool:
    return sys.stdin.isatty() and sys.stdout.isatty()


def confirm_prune(context: ApplicationContext, counts: dict[str, object]) -> bool:
    try:
        return Confirm.ask(
            f"Retire {counts['eligible']} failed records and remove {counts['eligible_partial_files']} owned partial files?",
            console=context.console,
            default=False,
        )
    except EOFError:
        return False


def run(arguments: argparse.Namespace, context: ApplicationContext) -> int:
    options = Options.model_validate(vars(arguments))
    if not options.testing:
        raise MarketForgeError("Storage cleanup requires --testing; no production fallback.")
    submitted = {key: value for key, value in vars(arguments).items() if value is not None}
    selected = StorageFilters.model_validate(submitted)
    immediate = options.apply and options.preview is None
    interactive = (
        immediate
        and options.storage_action == "prune"
        and not options.as_json
        and interactive_terminal()
    )
    if (
        options.storage_action == "prune"
        and selected.status is None
        and (not options.apply or immediate)
    ):
        selected = selected.model_copy(update={"status": "failed"})
    archive_id = local_identity(options.archive_reference)
    if immediate and options.storage_action != "prune":
        raise MarketForgeError(
            "Storage delete --apply requires --preview PATH from a fully reviewed preview."
        )
    if options.preview is not None and not options.apply:
        raise MarketForgeError(
            "--preview PATH is used with --apply. Run a fresh preview without these options to review current selection."
        )
    if options.preview is None:
        validate_selection(options.storage_action, selected, archive_id, options.overlap)

    async def execute_saved(source: Path, expected_digest: str | None = None) -> dict[str, object]:
        loaded, plan = load_plan(context.paths, source, options.storage_action)
        if expected_digest is not None and plan.digest != expected_digest:
            raise MarketForgeError(
                "Preview changed while awaiting confirmation; create a new preview."
            )
        actual = selected.model_dump(mode="json")
        for key in selected.model_fields_set - {"limit", "offset"}:
            if actual[key] != plan.filters.get(key):
                raise MarketForgeError(
                    "Apply uses the exact saved selection; do not change preview filters."
                )
        if archive_id is not None and archive_id != plan.filters.get("archive_id"):
            raise MarketForgeError("Apply local ID differs from the reviewed preview.")
        if options.overlap and not plan.overlap:
            raise MarketForgeError("Apply date mode differs from preview.")
        async with testing_connection(testing_dsn(context.paths.root)) as connection:
            return await apply(connection, context.paths, loaded, plan)

    async def create_preview() -> dict[str, object]:
        owner = WorkspaceOwnership(context.paths)
        inactive = False
        review_all = options.all_targets or options.as_json or interactive
        try:
            try:
                owner.acquire()
                inactive = True
            except OwnershipConflictError:
                pass
            async with testing_connection(testing_dsn(context.paths.root)) as connection:
                async with connection.transaction():
                    await connection.execute(
                        "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"
                    )
                    plan = await preview(
                        connection,
                        context.paths,
                        selected,
                        options.storage_action,
                        archive_id=archive_id,
                        overlap=options.overlap,
                        inactive=inactive,
                        review_all=review_all,
                    )
            counts = totals(plan.targets)
            source = save_plan(context.paths, plan) if counts["eligible"] else None
        finally:
            owner.release()
        result: dict[str, object] = {
            "schema_version": 1,
            "operation": options.storage_action,
            "mode": "preview",
            "status": "ready" if counts["eligible"] else "no_eligible_targets",
            "preview_path": str(source) if source is not None else None,
            "preview_digest": plan.digest if source is not None else None,
            "review_complete": plan.review_complete,
            "interactive_review": interactive,
            "totals": counts,
            "targets": [target.model_dump(mode="json") for target in plan.targets],
            "pagination": {
                "limit": None if review_all else selected.limit,
                "offset": 0 if review_all else selected.offset,
                "total": len(plan.targets),
            },
            "date_mode": "overlap"
            if options.storage_action == "prune" or options.overlap
            else "containment",
            "dependency_check": DEPENDENCY_CHECKS,
            "byte_semantics": "Reclaimable bytes are eligible file lengths observed on disk, not registry estimates; filesystem free-space effects can differ.",
        }
        if source is not None:
            result["apply_command"] = shlex.join(
                [
                    "marketforge",
                    "--project-root",
                    str(context.paths.root),
                    "storage",
                    options.storage_action,
                    "--testing",
                    "--apply",
                    "--preview",
                    str(source),
                ]
            )
        return result

    def present(value: dict[str, object]) -> None:
        if options.as_json:
            emit(context, utc_json(value), True)
        else:
            render_cleanup(context.console, value)

    if options.preview is not None:
        value = asyncio.run(execute_saved(options.preview))
        present(value)
        return 1 if value.get("status") != "complete" else 0
    value = asyncio.run(create_preview())
    present(value)
    counts = obj(value["totals"])
    if not immediate or not counts["eligible"]:
        return 0
    if not interactive:
        raise MarketForgeError(
            "Interactive prune confirmation requires terminal input and output. For scripts, review the JSON preview and use --apply --preview PATH."
        )
    # All targets have been displayed, and preview ownership is released while
    # waiting. Apply reacquires ownership and revalidates the exact frozen set.
    if not confirm_prune(context, counts):
        context.console.print(
            "Cleanup cancelled; no files removed or records retired.", style="mf.secondary"
        )
        return 0
    source = Path(str(value["preview_path"]))
    applied = asyncio.run(execute_saved(source, str(value["preview_digest"])))
    present(applied)
    return 1 if applied.get("status") != "complete" else 0
