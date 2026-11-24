"""Markets command registration and tested service dispatch."""

import argparse
import asyncio
import json

from pydantic import BaseModel, ConfigDict, Field

from forge.cli.environment import add_database_options
from forge.cli.market_filters import configure_identity_filters
from forge.cli.sorting import add_sorting
from forge.markets.capabilities import EXCHANGES, MARKETS
from forge.markets.models import Exchange, Market, MarketDataError
from forge.markets.repository import browse
from forge.markets.service import collect
from forge.markets.sorting import MARKETS as MARKET_SORT
from forge.shared.database.connection import database_connection, database_dsn
from forge.shared.ownership import WorkspaceOwnership
from forge.shared.terminal.markets import MarketCollectionDisplay, render_market_result
from forge.startup.context import ApplicationContext


class Options(BaseModel):
    model_config = ConfigDict(extra="ignore")
    market_action: str
    as_json: bool = False
    exchanges: list[Exchange] | None = None
    markets: list[Market] | None = None
    instrument: str | None = None
    reference: str | None = None
    base_asset: str | None = None
    quote_asset: str | None = None
    match: str | None = None
    silent: bool = False
    limit: int = Field(default=50, ge=1, le=10000)
    sort: str | None = None
    order: str | None = None


def emit(context: ApplicationContext, value: object, as_json: bool) -> None:
    text = json.dumps(value, default=str, indent=2, ensure_ascii=False)
    if as_json:
        print(text)
    else:
        context.console.print(text)


def configure(parser: argparse.ArgumentParser) -> None:
    actions = parser.add_subparsers(dest="market_action", required=True)
    for action in ("sync", "refresh", "list", "show"):
        child = actions.add_parser(
            action,
            help={
                "sync": "Collect all supported scopes concurrently, including dated futures; filters are optional. Absent instruments are retained.",
                "refresh": "Refresh all supported scopes concurrently, including dated futures; filters are optional. Native units are retained.",
                "list": "Browse stored markets without network access.",
                "show": "Inspect a stored exchange.market.symbol reference.",
            }[action],
        )
        if action in {"sync", "refresh"}:
            child.description = "Collect every supported exchange/market scope when filters are omitted (14 scopes; options and Bitget inverse excluded). Linear/inverse scopes include supported perpetual and dated futures endpoints. Independent scopes run concurrently with bounded workers and shared upstream budgets. Failures do not stop independent scopes; partial results exit 1, cancellation exits 130. Refresh uses a completed current metadata sync; run sync first."
        if action == "list":
            configure_identity_filters(child)
        child.set_defaults(service_handler=run)
        child.add_argument(
            "--silent",
            action="store_true",
            help="Suppress collection progress; retain the final result and failures.",
        )
        add_database_options(child)
        if action == "show":
            child.add_argument("reference")
        child.add_argument(
            "--exchange",
            dest="exchanges",
            action="append",
            choices=EXCHANGES,
            help="Optional, repeatable exchange filter; omitted means every supported exchange.",
        )
        child.add_argument(
            "--market",
            dest="markets",
            action="append",
            choices=MARKETS,
            help="Optional, repeatable market filter; derivatives include perpetuals and dated futures.",
        )
        child.add_argument(
            "--instrument", help="Exact native symbol, without changing market identity."
        )
        child.add_argument(
            "--limit",
            type=int,
            default=50,
            help="Maximum stored instruments displayed (1..10000); no network access."
            if action == "list"
            else argparse.SUPPRESS,
        )
        if action == "list":
            add_sorting(
                child,
                MARKET_SORT,
                notes="time means retained source observation time; freshness uses the recorded metric window end. Turnover sorts within native denominations.",
            )
        else:
            child.add_argument(
                "--sort", choices=("identity", "turnover", "freshness"), help=argparse.SUPPRESS
            )

    from forge.cli.commands.rankings import configure as configure_rankings

    configure_rankings(
        actions.add_parser("rank", help="Named turnover policies and immutable rankings.")
    )


def run(arguments: argparse.Namespace, context: ApplicationContext) -> int:
    options = Options.model_validate(vars(arguments))
    ordering = (
        MARKET_SORT.resolve(options.sort, options.order)
        if options.market_action == "list"
        else None
    )
    dsn = database_dsn(context.paths.root)

    display = MarketCollectionDisplay(
        context.error_console, silent=options.silent or options.as_json
    )

    async def execute() -> dict[str, object]:
        if options.market_action in {"sync", "refresh"}:
            return await collect(
                dsn,
                context.settings.http,
                metadata=options.market_action == "sync",
                exchanges=options.exchanges,
                markets=options.markets,
                symbol=options.instrument,
                progress=display.update,
            )
        async with database_connection(dsn) as c:
            rows = await browse(
                c,
                exchanges=list(options.exchanges) if options.exchanges else None,
                markets=list(options.markets) if options.markets else None,
                symbol=options.instrument,
                limit=2 if options.market_action == "show" else options.limit + 1,
                sort=options.sort,
                order=options.order,
                reference=options.reference,
                base_asset=options.base_asset,
                quote_asset=options.quote_asset,
                match=options.match,
            )
        if options.market_action == "show" and not rows:
            raise MarketDataError("Instrument reference was not found.")
        return {
            "instruments": rows[: options.limit] if options.market_action == "list" else rows,
            "truncated": options.market_action == "list" and len(rows) > options.limit,
            "semantics": "Stored current observations; null means missing; stale is based on one-hour freshness. Turnover units are native.",
        }

    if options.market_action in {"sync", "refresh"}:
        with WorkspaceOwnership(context.paths), display:
            value = asyncio.run(execute())
    else:
        value = asyncio.run(execute())
    if options.as_json:
        emit(context, value, True)
    else:
        filters = ", ".join(
            [
                *("exchange=" + e for e in options.exchanges or []),
                *("market=" + m for m in options.markets or []),
                *(["instrument=" + options.instrument] if options.instrument else []),
                *(["base_asset=" + options.base_asset] if options.base_asset else []),
                *(["quote_asset=" + options.quote_asset] if options.quote_asset else []),
                *(["match=" + options.match] if options.match is not None else []),
            ]
        )
        render_market_result(
            context.console,
            options.market_action,
            value,
            filters=filters,
            exchange_filtered=len(options.exchanges or []) == 1,
            market_filtered=len(options.markets or []) == 1,
            ordering=ordering.description if ordering else "",
            time_field={
                "measured": "measured_at",
                "observed": "source_observed_at",
                "fetched": "fetched_at",
            }.get(ordering.field)
            if ordering
            else None,
        )
    return (
        130 if value.get("status") == "cancelled" else 1 if value.get("status") == "partial" else 0
    )
