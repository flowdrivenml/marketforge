"""Markets command registration and tested service dispatch."""

import argparse
import asyncio
import json

from pydantic import BaseModel, ConfigDict

from forge.markets.models import Exchange, Market, MarketDataError
from forge.markets.repository import browse
from forge.markets.service import EXCHANGES, MARKETS, collect
from forge.shared.database.testing import testing_connection, testing_dsn
from forge.startup.context import ApplicationContext


class Options(BaseModel):
    model_config = ConfigDict(extra="ignore")
    market_action: str
    as_json: bool = False
    exchanges: list[Exchange] | None = None
    markets: list[Market] | None = None
    instrument: str | None = None
    reference: str | None = None
    limit: int = 50
    sort: str = "identity"


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
                "sync": "Collect metadata without deleting absent instruments.",
                "refresh": "Collect retained native-unit current statistics.",
                "list": "Browse stored markets without network access.",
                "show": "Inspect a stored exchange.market.symbol reference.",
            }[action],
        )
        child.set_defaults(service_handler=run)
        child.add_argument(
            "--silent",
            action="store_true",
            help="Retain the final result; collection emits no animated progress.",
        )
        child.add_argument("--testing", action="store_true", required=True)
        if action == "show":
            child.add_argument("reference")
        child.add_argument("--exchange", dest="exchanges", action="append", choices=EXCHANGES)
        child.add_argument("--market", dest="markets", action="append", choices=MARKETS)
        child.add_argument(
            "--instrument", help="Exact native symbol, without changing market identity."
        )
        child.add_argument("--limit", type=int, default=50)
        child.add_argument(
            "--sort",
            choices=("identity", "turnover", "freshness"),
            default="identity",
            help="Turnover sorts within native denomination groups, never across currencies.",
        )

    from forge.cli.commands.rankings import configure as configure_rankings

    configure_rankings(
        actions.add_parser("rank", help="Named turnover policies and immutable rankings.")
    )


def run(arguments: argparse.Namespace, context: ApplicationContext) -> int:
    options = Options.model_validate(vars(arguments))
    dsn = testing_dsn(context.paths.root)

    async def execute() -> dict[str, object]:
        if options.market_action in {"sync", "refresh"}:
            return await collect(
                dsn,
                context.settings.http,
                metadata=options.market_action == "sync",
                exchanges=options.exchanges or list(EXCHANGES),
                markets=options.markets or list(MARKETS),
                symbol=options.instrument,
            )
        async with testing_connection(dsn) as c:
            rows = await browse(
                c,
                exchanges=list(options.exchanges) if options.exchanges else None,
                markets=list(options.markets) if options.markets else None,
                symbol=options.instrument,
                limit=options.limit,
                sort=options.sort,
                reference=options.reference,
            )
        if options.market_action == "show" and not rows:
            raise MarketDataError("Instrument reference was not found.")
        return {
            "instruments": rows,
            "semantics": "Stored current observations; null means missing; stale is based on one-hour freshness. Turnover units are native.",
        }

    value = asyncio.run(execute())
    emit(context, value, options.as_json)
    return 1 if value.get("status") == "partial" else 0
