"""Async public metadata/tickers. Native denominations never become implicit USD."""

import json
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from forge.markets.models import (
    Exchange,
    Instrument,
    Market,
    MarketDataError,
    Observation,
    array,
    number,
    obj,
    string,
)
from forge.shared.http.client import HttpClient


class PublicProvider:
    def __init__(self, exchange: Exchange, http: HttpClient) -> None:
        self.exchange = exchange
        self.http = http

    async def get(self, url: str, params: dict[str, str] | None = None) -> object:
        response = await self.http.get(url, params=params, max_response_bytes=16 * 1024 * 1024)
        value: object = json.loads(response.content, parse_float=Decimal)
        return value

    async def pages(
        self, market: Market, *, metadata: bool
    ) -> AsyncIterator[list[dict[str, object]]]:
        e = self.exchange
        if e == "bitget" and market == "inverse":
            raise MarketDataError("Bitget inverse is outside initial markets coverage.")
        cursor: str | None = None
        seen: set[str] = set()
        while True:
            params: dict[str, str] = {}
            if e == "binance":
                base, path = {
                    "spot": ("https://api.binance.com", "/api/v3/"),
                    "linear": ("https://fapi.binance.com", "/fapi/v1/"),
                    "inverse": ("https://dapi.binance.com", "/dapi/v1/"),
                }[market]
                payload = await self.get(
                    base + path + ("exchangeInfo" if metadata else "ticker/24hr")
                )
                items = array(obj(payload).get("symbols")) if metadata else array(payload)
            elif e == "bybit":
                params = {"category": market, "limit": "1000"} if metadata else {"category": market}
                if cursor:
                    params["cursor"] = cursor
                payload = obj(
                    await self.get(
                        "https://api.bybit.com/v5/market/"
                        + ("instruments-info" if metadata else "tickers"),
                        params,
                    )
                )
                if payload.get("retCode") != 0:
                    raise MarketDataError("Bybit rejected public market collection.")
                result = obj(payload.get("result"))
                if result.get("category") != market:
                    raise MarketDataError("Bybit returned a different market category.")
                items = array(result.get("list"))
                cursor_value = (
                    result.get("nextPageCursor") if metadata and market != "spot" else None
                )
                cursor = string(cursor_value) if cursor_value else None
            elif e == "okx":
                params = {"instType": "SPOT" if market == "spot" else "SWAP"}
                payload = obj(
                    await self.get(
                        "https://www.okx.com/api/v5/"
                        + ("public/instruments" if metadata else "market/tickers"),
                        params,
                    )
                )
                if payload.get("code") != "0":
                    raise MarketDataError("OKX rejected public market collection.")
                items = array(payload.get("data"))
            elif e == "gateio":
                prefix = (
                    "spot/"
                    if market == "spot"
                    else "futures/" + ("usdt" if market == "linear" else "btc") + "/"
                )
                payload = await self.get(
                    "https://api.gateio.ws/api/v4/"
                    + prefix
                    + (
                        "currency_pairs"
                        if market == "spot" and metadata
                        else "contracts"
                        if metadata
                        else "tickers"
                    )
                )
                items = array(payload)
            else:
                params = {"category": "SPOT" if market == "spot" else "USDT-FUTURES"}
                payload = obj(
                    await self.get(
                        "https://api.bitget.com/api/v3/market/"
                        + ("instruments" if metadata else "tickers"),
                        params,
                    )
                )
                if str(payload.get("code")) != "00000":
                    raise MarketDataError("Bitget rejected public market collection.")
                items = array(payload.get("data"))
            records = [obj(item) for item in items]
            if metadata:
                # Validate a full page before any caller writes its records.
                parsed = [self.instrument(market, r) for r in records]
                yield [r for r, p in zip(records, parsed, strict=True) if p is not None]
            else:
                yield records
            if not cursor:
                break
            if cursor in seen:
                raise MarketDataError("Repeated metadata pagination cursor.")
            seen.add(cursor)

    def instrument(self, market: Market, r: dict[str, object]) -> Instrument | None:
        e = self.exchange
        extra: dict[str, object] = {}
        if e == "binance":
            if market != "spot" and string(r.get("contractType")) != "PERPETUAL":
                return None
            symbol = string(r.get("symbol"))
            base = string(r.get("baseAsset"))
            quote = string(r.get("quoteAsset"))
            filters = {string(obj(v).get("filterType")): obj(v) for v in array(r.get("filters"))}
            extra = {
                "tick_size": number(filters.get("PRICE_FILTER", {}).get("tickSize")),
                "qty_step": number(filters.get("LOT_SIZE", {}).get("stepSize")),
            }
            if market == "inverse":
                extra.update(
                    quantity_type="contracts",
                    contract_value=number(r.get("contractSize")),
                    contract_unit=quote,
                )
            settlement = r.get("marginAsset")
            active = (
                string(r.get("contractStatus") if market == "inverse" else r.get("status"))
                == "TRADING"
            )
        elif e == "bybit":
            if market != "spot":
                contract = string(r.get("contractType"))
                if contract in {"LinearFutures", "InverseFutures"}:
                    return None
                if contract != ("LinearPerpetual" if market == "linear" else "InversePerpetual"):
                    raise MarketDataError("Bybit contract type does not match selected market.")
            symbol = string(r.get("symbol"))
            base = string(r.get("baseCoin"))
            quote = string(r.get("quoteCoin"))
            lot = obj(r.get("lotSizeFilter"))
            price = obj(r.get("priceFilter"))
            extra = {
                "tick_size": number(price.get("tickSize")),
                "qty_step": number(
                    lot.get("basePrecision") if market == "spot" else lot.get("qtyStep")
                ),
            }
            if market == "inverse":
                extra["quantity_type"] = "quote"
            settlement = r.get("settleCoin")
            active = string(r.get("status")) == "Trading"
        elif e == "okx":
            if market != "spot" and string(r.get("ctType")) != market:
                return None
            symbol = string(r.get("instId"))
            if market != "spot" and not symbol.endswith("-SWAP"):
                raise MarketDataError("OKX metadata is not a perpetual instrument.")
            if market == "spot":
                base = string(r.get("baseCcy"))
                quote = string(r.get("quoteCcy"))
            else:
                base, quote = string(r.get("instFamily")).rsplit("-", 1)
                value = number(r.get("ctVal"))
                mult = number(r.get("ctMult"))
                if value is None or mult is None:
                    raise MarketDataError("Missing OKX contract multiplier.")
                extra.update(
                    quantity_type="contracts",
                    contract_value=value * mult,
                    contract_unit=string(r.get("ctValCcy")),
                )
            extra.update(tick_size=number(r.get("tickSz")), qty_step=number(r.get("lotSz")))
            settlement = r.get("settleCcy")
            active = string(r.get("state")) == "live"
        elif e == "gateio":
            if market == "spot":
                symbol = string(r.get("id"))
                base = string(r.get("base"))
                quote = string(r.get("quote"))
                precision = number(r.get("precision"))
                amount = number(r.get("amount_precision"))
                extra = {
                    "tick_size": Decimal(10) ** -int(precision) if precision is not None else None,
                    "qty_step": Decimal(10) ** -int(amount) if amount is not None else None,
                }
                active = string(r.get("trade_status")) == "tradable"
                settlement = None
            else:
                symbol = string(r.get("name"))
                base, quote = symbol.rsplit("_", 1)
                if r.get("type") not in {"direct", "inverse"}:
                    raise MarketDataError("Unknown Gate contract semantics.")
                if (r["type"] == "inverse") != (market == "inverse"):
                    raise MarketDataError("Wrong Gate market returned.")
                extra = {
                    "quantity_type": "contracts",
                    "contract_value": Decimal(1)
                    if market == "inverse"
                    else number(r.get("quanto_multiplier")),
                    "contract_unit": quote if market == "inverse" else base,
                    "tick_size": number(r.get("order_price_round")),
                    "qty_step": Decimal(1),
                }
                settlement = base if market == "inverse" else quote
                active = not bool(r.get("in_delisting", False))
        else:
            if string(r.get("category")).upper() != (
                "SPOT" if market == "spot" else "USDT-FUTURES"
            ):
                raise MarketDataError("Bitget returned a different market category.")
            if market != "spot":
                contract_type = string(r.get("type"))
                if contract_type == "delivery":
                    return None
                if contract_type != "perpetual":
                    raise MarketDataError("Unknown Bitget contract semantics.")
            symbol = string(r.get("symbol"))
            base = string(r.get("baseCoin"))
            quote = string(r.get("quoteCoin"))
            p = number(r.get("pricePrecision"))
            q = number(r.get("quantityPrecision"))
            extra = {
                "tick_size": Decimal(10) ** -int(p) if p is not None else None,
                "qty_step": number(r.get("sizeMultiplier"))
                if market != "spot"
                else Decimal(10) ** -int(q)
                if q is not None
                else None,
            }
            settlement = r.get("settleCoin")
            active = string(r.get("status", r.get("symbolStatus"))) in {
                "online",
                "normal",
                "listed",
            }
        return Instrument.model_validate(
            {
                "exchange": e,
                "market": market,
                "symbol": symbol,
                "base": base,
                "quote": quote,
                "settlement": settlement or None,
                "active": active,
                "raw": r,
                **extra,
            }
        )

    def observation(
        self, instrument: Instrument, r: dict[str, object], *, fetched: datetime
    ) -> Observation:
        e = self.exchange
        m = instrument.market
        p: object = None
        b: object = None
        q: object = None
        c: object = None
        t: Decimal | None = None
        estimate = False
        oi: object = None
        oi_unit: str | None = None
        funding: object = None
        observed: object = None
        if e == "binance":
            p = r.get("lastPrice")
            observed = r.get("closeTime")
            if m == "inverse":
                c = r.get("volume")
                b = r.get("baseVolume")
                cv = number(c)
                if cv is not None and instrument.contract_value is not None:
                    t = cv * instrument.contract_value
                q = t
            else:
                b = r.get("volume")
                q = r.get("quoteVolume")
                t = number(q)
        elif e == "bybit":
            p = r.get("lastPrice")
            funding = r.get("fundingRate")
            oi = r.get("openInterest")
            if m == "inverse":
                b = r.get("turnover24h")
                q = r.get("volume24h")
                t = number(q)
                oi_unit = instrument.quote
            else:
                b = r.get("volume24h")
                q = r.get("turnover24h")
                t = number(q)
                oi_unit = instrument.base
        elif e == "okx":
            p = r.get("last")
            observed = r.get("ts")
            if m == "spot":
                b = r.get("vol24h")
                q = r.get("volCcy24h")
                t = number(q)
            else:
                c = r.get("vol24h")
                b = r.get("volCcy24h")
                cv = number(c)
                if m == "inverse" and cv is not None and instrument.contract_value is not None:
                    if instrument.contract_unit != instrument.quote:
                        raise MarketDataError(
                            "Inverse turnover contract value is not denominated in quote currency."
                        )
                    t = cv * instrument.contract_value
                elif m == "linear" and number(b) is not None and number(p) is not None:
                    base_value = number(b)
                    price_value = number(p)
                    if base_value is not None and price_value is not None:
                        t = base_value * price_value
                        estimate = True
                q = t
        elif e == "gateio":
            p = r.get("last")
            if m == "spot":
                b = r.get("base_volume")
                q = r.get("quote_volume")
            else:
                b = r.get("volume_24h_base")
                q = r.get("volume_24h_quote")
                c = r.get("volume_24h")
                oi = r.get("total_size")
                oi_unit = "contracts"
                funding = r.get("funding_rate")
            t = number(q)
        else:
            p = r.get("lastPrice")
            b = r.get("volume24h")
            q = r.get("turnover24h")
            t = number(q)
            oi = r.get("openInterest")
            oi_unit = instrument.base
            funding = r.get("fundingRate")
            observed = r.get("ts")
        source_time = None
        if observed not in (None, ""):
            value = number(observed)
            if value is not None:
                source_time = datetime.fromtimestamp(float(value / 1000), UTC)
        end = source_time or fetched
        open_value = number(oi)
        return Observation(
            instrument=instrument,
            fetched_at=fetched,
            source_observed_at=source_time,
            window_start=end - timedelta(hours=24),
            window_end=end,
            window_basis="source" if source_time else "fetch_time",
            last_price=number(p),
            base_volume=number(b),
            quote_volume=number(q),
            contract_volume=number(c),
            turnover=t,
            turnover_unit=instrument.quote if t is not None else None,
            turnover_estimated=estimate,
            open_interest=open_value,
            open_interest_unit=oi_unit if open_value is not None else None,
            funding_rate=number(funding, signed=True),
            raw=r,
        )
