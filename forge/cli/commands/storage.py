"""Read-only storage navigation; maintenance keeps its existing service boundary."""

import argparse
import asyncio
from datetime import UTC, datetime

from pydantic import BaseModel, ConfigDict

from forge.cli.commands.markets import emit
from forge.cli.market_filters import configure_identity_filters
from forge.markets.capabilities import EXCHANGES
from forge.markets.models import array
from forge.shared.database.testing import testing_connection, testing_dsn
from forge.shared.errors import MarketForgeError
from forge.shared.terminal.storage import render_storage
from forge.startup.context import ApplicationContext
from forge.storage.browsing import coverage, list_archives
from forge.storage.models import StorageFilters


class Options(BaseModel):
    model_config = ConfigDict(extra="ignore")
    storage_action: str = "coverage"
    testing: bool = False
    archive_reference: str | None = None
    as_json: bool = False
    wide: bool = False
    gaps: bool = False


def filters(
    parser: argparse.ArgumentParser, *, cleanup: bool = False, containment: bool = False
) -> None:
    configure_identity_filters(parser)
    parser.add_argument("--exchange", choices=EXCHANGES)
    parser.add_argument("--market", choices=("spot", "linear", "inverse", "option"))
    parser.add_argument("--instrument-type", choices=("spot", "perpetual", "future", "option"))
    parser.add_argument(
        "--instrument", help="Exact registered native instrument/family/scope identifier."
    )
    parser.add_argument("--scope-kind", choices=("instrument", "family", "base_coin"))
    parser.add_argument("--data-type", choices=("trades", "order_book_l2"))
    parser.add_argument(
        "--feed",
        choices=("incremental", "snapshot", "unknown"),
        help="Registered feed; unknown selects missing feed identities.",
    )
    parser.add_argument("--origin", choices=("remote", "external"))
    parser.add_argument(
        "--start",
        help="Timezone-aware inclusive start, e.g. 2026-10-27T00:00:00Z; "
        + (
            "archive start must be at or after this bound unless --overlap."
            if containment
            else "interval overlap."
        ),
    )
    parser.add_argument(
        "--end",
        help="Timezone-aware exclusive end; "
        + (
            "archive end must be at or before this bound unless --overlap."
            if containment
            else "interval overlap."
        ),
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Preview rows displayed only; never narrows target selection."
        if cleanup
        else "Rows/groups displayed, 1..10000 (default 100); coverage aggregates all matching files.",
    )
    parser.add_argument("--offset", type=int, help="Zero-based result offset (default 0).")


def configure(parser: argparse.ArgumentParser) -> None:
    from forge.cli.commands.maintenance import run as maintenance

    # Root arguments support the bare overview; suppressed child defaults allow
    # the same filters before or after an explicit browsing subcommand.
    parser.description = "Browse nominal local archive coverage by default. Read-only list/details use the configured raw root and require --testing. Maintenance is explicit."
    parser.add_argument(
        "--testing",
        action="store_true",
        help="Use the verified testing database; no production fallback.",
    )
    filters(parser)
    parser.add_argument("--gaps", action="store_true", help="Show missing half-open UTC intervals.")
    parser.set_defaults(service_handler=run, storage_action="coverage")
    actions = parser.add_subparsers(dest="storage_action")
    for action in ("coverage", "list", "show", "verify", "reconcile"):
        child = actions.add_parser(action, argument_default=argparse.SUPPRESS, allow_abbrev=False)
        child.set_defaults(service_handler=run, storage_action=action)
        child.add_argument(
            "--testing", action="store_true", required=action in {"verify", "reconcile"}
        )
        if action in {"coverage", "list"}:
            filters(child)
            if action == "coverage":
                child.add_argument(
                    "--gaps", action="store_true", help="Show missing half-open UTC intervals."
                )
            else:
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
                )
                child.add_argument(
                    "--wide",
                    action="store_true",
                    help="Include registered filenames; full paths/checksums remain in show and JSON.",
                )
                child.add_argument("--archive", dest="archive_reference")
        elif action == "show":
            child.add_argument("archive_reference", help="Local registry ID: local:536 or 536.")
        else:
            # Preserve maintenance dispatch and accepted options. Browsing never
            # invokes these mutation-capable handlers.
            child.set_defaults(service_handler=maintenance, maintenance_action=action)
            child.add_argument("--archive", dest="archive_reference")
            child.add_argument("--limit", type=int, default=100)
            child.add_argument("--status")
            child.add_argument("--origin", choices=("remote", "external"))

    from forge.cli.commands.storage_cleanup import configure as configure_cleanup

    configure_cleanup(actions.add_parser)
    parser.set_defaults(storage_action="coverage")


def local_identity(reference: str | None) -> int | None:
    if reference is None:
        return None
    try:
        identity = int(reference.removeprefix("local:"))
    except ValueError:
        raise MarketForgeError(
            "Archive reference must be a local registry ID (local:536 or 536)."
        ) from None
    if not 1 <= identity <= 2**63 - 1:
        raise MarketForgeError("Local archive ID must be a positive PostgreSQL bigint.")
    return identity


def utc_json(value: object) -> object:
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
    if isinstance(value, dict):
        from typing import cast

        return {key: utc_json(item) for key, item in cast(dict[str, object], value).items()}
    if isinstance(value, (tuple, list)):
        from typing import cast

        return [utc_json(item) for item in cast(list[object], value)]
    return value


def run(arguments: argparse.Namespace, context: ApplicationContext) -> int:
    options = Options.model_validate(vars(arguments))
    if not options.testing:
        raise MarketForgeError("Storage browsing requires --testing; no production fallback.")
    selected = StorageFilters.model_validate(
        {key: value for key, value in vars(arguments).items() if value is not None}
    )
    identity = local_identity(options.archive_reference)

    async def execute() -> dict[str, object]:
        async with testing_connection(testing_dsn(context.paths.root)) as connection:
            async with connection.transaction():
                await connection.execute(
                    "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"
                )
                if options.storage_action == "coverage":
                    return await coverage(
                        connection, context.paths.raw_dir, selected, include_gaps=options.gaps
                    )
                if options.storage_action == "show":
                    result = await list_archives(
                        connection,
                        context.paths.raw_dir,
                        StorageFilters(limit=1),
                        archive_id=identity,
                    )
                    if not array(result["archives"]):
                        raise MarketForgeError(
                            "Local archive not found in the configured raw root."
                        )
                    return result
                return await list_archives(
                    connection, context.paths.raw_dir, selected, archive_id=identity
                )

    value = asyncio.run(execute())
    if options.as_json:
        emit(context, utc_json(value), True)
    else:
        render_storage(context.console, options.storage_action, value, wide=options.wide)
    return 0
