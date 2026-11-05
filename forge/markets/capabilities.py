"""Public collection scopes and endpoint products, shared by services and CLI."""

from forge.markets.models import Exchange, Market, MarketDataError

EXCHANGES: tuple[Exchange, ...] = ("binance", "bybit", "okx", "gateio", "bitget")
MARKETS: tuple[Market, ...] = ("spot", "linear", "inverse")
SCOPES: tuple[tuple[Exchange, Market], ...] = tuple(
    (e, m) for e in EXCHANGES for m in MARKETS if (e, m) != ("bitget", "inverse")
)
COLLECTION_VERSION = "markets-futures-v3"


def resolve_scopes(
    exchanges: list[Exchange] | None = None, markets: list[Market] | None = None
) -> tuple[tuple[Exchange, Market], ...]:
    result: tuple[tuple[Exchange, Market], ...] = tuple(
        (e, m)
        for e, m in SCOPES
        if (exchanges is None or e in exchanges) and (markets is None or m in markets)
    )
    if not result:
        raise MarketDataError("No supported exchange/market scopes match the filters.")
    return result


def products(exchange: Exchange, market: Market) -> tuple[str, ...]:
    if (exchange, market) not in SCOPES:
        raise MarketDataError("Unsupported exchange/market scope.")
    if market == "spot":
        return ("spot",)
    if exchange == "okx":
        return ("perpetual", "future")
    if exchange == "gateio":
        # Gate documents delivery settlement as USDT only; inverse stays perpetual.
        return ("perpetual", "future") if market == "linear" else ("perpetual",)
    return ("mixed",)
