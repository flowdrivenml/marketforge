"""Async public metadata/tickers. Native denominations never become implicit USD."""

import json
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from forge.markets.capabilities import products
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
from forge.shared.http.client import HttpClient, ResponseSizeLimitError


class PublicProvider:
    def __init__(self, exchange: Exchange, http: HttpClient) -> None:
        self.exchange: Exchange = exchange
        self.http = http

    async def get(self, url: str, params: dict[str, str] | None = None) -> object:
        try:
            response = await self.http.get(url, params=params, max_response_bytes=32 * 1024 * 1024)
        except ResponseSizeLimitError as error:
            raise MarketDataError(
                f"{self.exchange} market metadata/statistics exceeds the decoded response size limit ({error.limit:,} bytes). Narrow the scope where supported or review the bounded provider allowance."
            ) from None
        value: object = json.loads(response.content, parse_float=Decimal)
        return value

    async def pages(
        self, market: Market, *, metadata: bool
    ) -> AsyncIterator[list[dict[str, object]]]:
        for product in products(self.exchange, market):
            async for page in self._pages(market, metadata=metadata, product=product):
                yield page

    async def _pages(
        self, market: Market, *, metadata: bool, product: str
    ) -> AsyncIterator[list[dict[str, object]]]:
        e = self.exchange
        if e == "bitget" and market == "inverse":
            raise MarketDataError("Bitget inverse is outside initial markets coverage.")
        cursor: str | None = None
        offset = 0
        page_signatures: set[tuple[str, ...]] = set()
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
                params = {
                    "instType": {
                        "spot": "SPOT",
                        "perpetual": "SWAP",
                        "future": "FUTURES",
                    }[product]
                }
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
                    else ("delivery/" if product == "future" else "futures/")
                    + ("usdt" if market == "linear" else "btc")
                    + "/"
                )
                if metadata and product == "perpetual":
                    params = {"limit": "100", "offset": str(offset)}
                payload = await self.get(
                    "https://api.gateio.ws/api/v4/"
                    + prefix
                    + (
                        "currency_pairs"
                        if market == "spot" and metadata
                        else "contracts"
                        if metadata
                        else "tickers"
                    ),
                    params=params,
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
            records = [dict(obj(item)) for item in items]
            if product in {"perpetual", "future"}:
                for row in records:
                    row["_collection_type"] = product
            if metadata:
                # Validate a full page before any caller writes its records.
                parsed = [self.instrument(market, r) for r in records]
                yield [r for r, p in zip(records, parsed, strict=True) if p is not None]
            else:
                yield records
            if e == "gateio" and metadata and product == "perpetual":
                signature = tuple(string(r.get("name")) for r in records)
                if signature and signature in page_signatures:
                    raise MarketDataError("Repeated Gate metadata pagination page.")
                page_signatures.add(signature)
                if len(items) == 100:
                    offset += 100
                    continue
            if not cursor:
                break
            if cursor in seen:
                raise MarketDataError("Repeated metadata pagination cursor.")
            seen.add(cursor)

    def instrument(self, market: Market, r: dict[str, object]) -> Instrument | None:
        e = self.exchange
        if (
            r.get("instType") == "OPTION"
            or r.get("contractType") in {"Option", "OPTION"}
            or r.get("category") == "OPTION"
        ):
            return None
        extra: dict[str, object] = {}
        if e == "binance":
            if market != "spot":
                contract = string(r.get("contractType"))
                if contract in {"PERPETUAL", "TRADIFI_PERPETUAL"}:
                    extra["instrument_type"] = "perpetual"
                elif contract in {
                    "CURRENT_MONTH",
                    "NEXT_MONTH",
                    "CURRENT_QUARTER",
                    "NEXT_QUARTER",
                    "CURRENT_QUARTER_DELIVERING",
                    "NEXT_QUARTER_DELIVERING",
                }:
                    extra["instrument_type"] = "future"
                else:
                    raise MarketDataError("Unknown Binance contract type.")
            symbol = string(r.get("symbol"))
            base = string(r.get("baseAsset"))
            quote = string(r.get("quoteAsset"))
            filters = {string(obj(v).get("filterType")): obj(v) for v in array(r.get("filters"))}
            extra.update(
                {
                    "min_qty": positive(filters.get("LOT_SIZE", {}).get("minQty")),
                    "max_qty": positive(filters.get("LOT_SIZE", {}).get("maxQty")),
                    "min_notional": positive(
                        filters.get("NOTIONAL", filters.get("MIN_NOTIONAL", {})).get(
                            "minNotional",
                            filters.get("MIN_NOTIONAL", {}).get("notional"),
                        )
                    ),
                    "launch_time_ns": epoch_ns(r.get("onboardDate")),
                    "expiry_ns": (
                        epoch_ns(r.get("deliveryDate"))
                        if extra.get("instrument_type") == "future"
                        else None
                    ),
                    "tick_size": number(filters.get("PRICE_FILTER", {}).get("tickSize")),
                    "qty_step": number(filters.get("LOT_SIZE", {}).get("stepSize")),
                }
            )
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
                expected = "Linear" if market == "linear" else "Inverse"
                known = {
                    "LinearPerpetual",
                    "LinearFutures",
                    "InversePerpetual",
                    "InverseFutures",
                }
                if contract not in known:
                    raise MarketDataError("Unknown Bybit contract type.")
                if contract not in {expected + "Perpetual", expected + "Futures"}:
                    return None  # baseCoin queries legitimately return both market categories.
                extra["instrument_type"] = "future" if contract.endswith("Futures") else "perpetual"
            symbol = string(r.get("symbol"))
            base = string(r.get("baseCoin"))
            quote = string(r.get("quoteCoin"))
            lot = obj(r["lotSizeFilter"]) if r.get("lotSizeFilter") is not None else {}
            price = obj(r["priceFilter"]) if r.get("priceFilter") is not None else {}
            extra.update(
                {
                    "launch_time_ns": epoch_ns(r.get("launchTime")),
                    "expiry_ns": (
                        epoch_ns(r.get("deliveryTime"))
                        if extra.get("instrument_type") == "future"
                        else None
                    ),
                    "min_qty": positive(lot.get("minOrderQty")),
                    "max_qty": positive(lot.get("maxOrderQty")),
                    "min_notional": positive(lot.get("minNotionalValue", lot.get("minOrderAmt"))),
                    "tick_size": number(price.get("tickSize")),
                    "qty_step": number(
                        lot.get("basePrecision") if market == "spot" else lot.get("qtyStep")
                    ),
                }
            )
            if market == "inverse":
                extra["quantity_type"] = "quote"
            settlement = r.get("settleCoin")
            active = string(r.get("status")) == "Trading"
        elif e == "okx":
            if market != "spot" and string(r.get("ctType")) != market:
                return None
            symbol = string(r.get("instId"))
            kind = r.get("instType")
            if kind == "OPTION":
                return None
            if kind is None:  # Earlier saved SWAP fixtures supplied the endpoint type only.
                kind = (
                    "SPOT"
                    if market == "spot"
                    else ("FUTURES" if r.get("_collection_type") == "future" else "SWAP")
                )
            endpoint_kind = {"perpetual": "SWAP", "future": "FUTURES"}.get(
                str(r.get("_collection_type"))
            )
            if endpoint_kind is not None and kind != endpoint_kind:
                raise MarketDataError("OKX response instrument type disagrees with its endpoint.")
            if kind not in ({"SPOT"} if market == "spot" else {"SWAP", "FUTURES"}):
                raise MarketDataError("OKX instrument type disagrees with selected market.")
            if kind == "SWAP" and not symbol.endswith("-SWAP"):
                raise MarketDataError("OKX SWAP metadata has an inconsistent instrument ID.")
            if market != "spot" and (
                symbol == r.get("instFamily") or "futureschain" in symbol.lower()
            ):
                raise MarketDataError("An OKX archive family is not a concrete contract.")
            extra.update(
                instrument_type={
                    "SPOT": "spot",
                    "SWAP": "perpetual",
                    "FUTURES": "future",
                }[str(kind)],
                launch_time_ns=epoch_ns(r.get("listTime")),
                expiry_ns=epoch_ns(r.get("expTime")) if kind == "FUTURES" else None,
            )
            if market == "spot":
                base = string(r.get("baseCcy"))
                quote = string(r.get("quoteCcy"))
            else:
                base, quote = string(r.get("instFamily")).rsplit("-", 1)
                if "_" in quote:
                    # Family routing suffixes (USD_UM, USD_UM_XPERP) are not currencies.
                    # Use the separately supplied underlying, never relabel USDT/USDC.
                    base, quote = string(r.get("uly")).rsplit("-", 1)
                value = number(r.get("ctVal"))
                mult = number(r.get("ctMult"))

                extra.update(
                    quantity_type="contracts",
                    contract_value=(
                        value * mult if value is not None and mult is not None else None
                    ),
                    contract_unit=r.get("ctValCcy") or None,
                )
            extra.update(
                tick_size=number(r.get("tickSz")),
                qty_step=number(r.get("lotSz")),
                min_qty=positive(r.get("minSz")),
                max_qty=positive(r.get("maxLmtSz")),
            )
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
                    "tick_size": (
                        Decimal(10) ** -int(precision) if precision is not None else None
                    ),
                    "qty_step": (Decimal(10) ** -int(amount) if amount is not None else None),
                }
                active = string(r.get("trade_status")) == "tradable"
                settlement = None
            else:
                symbol = string(r.get("name"))
                underlying = (
                    string(r.get("underlying")) if r.get("_collection_type") == "future" else symbol
                )
                base, quote = underlying.rsplit("_", 1)
                if r.get("type") not in {"direct", "inverse"}:
                    raise MarketDataError("Unknown Gate contract semantics.")
                if (r["type"] == "inverse") != (market == "inverse"):
                    raise MarketDataError("Wrong Gate market returned.")
                extra = {
                    "instrument_type": (
                        "future" if r.get("_collection_type") == "future" else "perpetual"
                    ),
                    "expiry_ns": (
                        epoch_ns(r.get("expire_time"), seconds=True)
                        if r.get("_collection_type") == "future"
                        else None
                    ),
                    "launch_time_ns": epoch_ns(r.get("create_time"), seconds=True),
                    "min_qty": positive(r.get("order_size_min")),
                    "max_qty": positive(r.get("order_size_max")),
                    "quantity_type": "contracts",
                    "contract_value": (
                        Decimal(1) if market == "inverse" else number(r.get("quanto_multiplier"))
                    ),
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
                if contract_type not in {"perpetual", "delivery"}:
                    raise MarketDataError("Unknown Bitget contract semantics.")
                extra["instrument_type"] = "future" if contract_type == "delivery" else "perpetual"
            symbol = string(r.get("symbol"))
            base = string(r.get("baseCoin"))
            quote = string(r.get("quoteCoin"))
            p = number(r.get("pricePrecision"))
            q = number(r.get("quantityPrecision"))
            extra.update(
                {
                    "launch_time_ns": epoch_ns(r.get("launchTime")),
                    "expiry_ns": (
                        epoch_ns(r.get("deliveryTime"))
                        if extra.get("instrument_type") == "future"
                        else None
                    ),
                    "min_qty": positive(r.get("minOrderQty")),
                    "max_qty": positive(r.get("maxOrderQty")),
                    "min_notional": positive(r.get("minOrderAmount")),
                    "tick_size": positive(r.get("priceMultiplier"))
                    or (Decimal(10) ** -int(p) if p is not None else None),
                    "qty_step": (
                        positive(r.get("quantityMultiplier", r.get("sizeMultiplier")))
                        if market != "spot"
                        else Decimal(10) ** -int(q)
                        if q is not None
                        else None
                    ),
                }
            )
            settlement = r.get("settleCoin")
            active = string(r.get("status", r.get("symbolStatus"))) in {
                "online",
                "normal",
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
                "source_status": r.get("status")
                or r.get("contractStatus")
                or r.get("state")
                or r.get("trade_status")
                or r.get("symbolStatus"),
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
        evidence: dict[str, object] = {
            "activity_scope": "exchange_local",
            "base_volume_scope": "exchange_local",
        }
        turnover_field: str | None = None
        if e == "binance":
            p = r.get("lastPrice")
            observed = r.get("closeTime")
            turnover_field = (
                "quoteVolume"
                if m != "inverse" or r.get("quoteVolume") is not None
                else "volume * contract_value"
            )
            if m == "inverse":
                c = r.get("volume")
                b = r.get("baseVolume")
                cv = number(c)
                native_quote = number(r.get("quoteVolume"))
                if native_quote is not None:
                    t = native_quote
                elif (
                    cv is not None
                    and instrument.contract_value is not None
                    and instrument.contract_unit == instrument.quote
                ):
                    t = cv * instrument.contract_value
                q = t
            else:
                b = r.get("volume")
                q = r.get("quoteVolume")
                t = number(q)
        elif e == "bybit":
            turnover_field = "volume24h" if m == "inverse" else "turnover24h"
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
            turnover_field = (
                "volCcy24h"
                if m == "spot"
                else "vol24h * contract_value"
                if m == "inverse"
                else "volCcy24h * last"
            )
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
                    if instrument.contract_unit == instrument.quote:
                        t = cv * instrument.contract_value
                elif m == "linear" and number(b) is not None and number(p) is not None:
                    base_value = number(b)
                    price_value = number(p)
                    if base_value is not None and price_value is not None:
                        t = base_value * price_value
                        estimate = True
                q = t
        elif e == "gateio":
            turnover_field = "quote_volume" if m == "spot" else "volume_24h_quote"
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
            turnover_field = "turnover24h"
            reality = instrument.raw.get("isReality")
            if m == "spot" and reality == "yes":
                turnover_field = "platformTurnover24h"
                evidence["base_volume_scope"] = "broader_source_not_platform_local; omitted"
                evidence["classification"] = "instrument.isReality=yes"
                try:
                    t = number(r.get(turnover_field))
                except MarketDataError:
                    evidence["turnover_missing_reason"] = (
                        "invalid platform-local turnover (platformTurnover24h); refresh Bitget statistics"
                    )
                if t is None:
                    evidence.setdefault(
                        "turnover_missing_reason",
                        "missing platform-local turnover (platformTurnover24h); refresh Bitget statistics",
                    )
                q = t
            elif m == "spot" and reality != "no":
                evidence["turnover_missing_reason"] = (
                    "unknown Bitget spot activity scope; refresh Bitget metadata then statistics"
                )
                evidence["activity_scope"] = "unknown"
            else:
                b = r.get("volume24h")
                q = r.get("turnover24h")
                t = number(q)
            oi = r.get("openInterest")
            oi_unit = instrument.base
            funding = r.get("fundingRate")
            observed = r.get("ts")
        if instrument.instrument_type == "future":
            funding = None
        source_time = None
        if observed not in (None, ""):
            value = number(observed)
            if value is not None:
                source_time = datetime.fromtimestamp(float(value / 1000), UTC)
        end = source_time or fetched
        start = end - timedelta(hours=24)
        interval_basis = (
            "inferred_24h_from_source_timestamp" if source_time else "inferred_24h_from_fetch_time"
        )
        if e == "binance" and r.get("openTime") not in (None, ""):
            if source_time is None:
                raise MarketDataError("Binance interval start has no interval end.")
            opening = number(r["openTime"])
            assert opening is not None
            start = datetime.fromtimestamp(float(opening / 1000), UTC)
            interval_basis = "source_supplied_openTime_closeTime"
        evidence.update(
            {
                "turnover_field": turnover_field,
                "turnover_kind": "volume_derived_estimate"
                if estimate
                else "reported_or_contract_notional",
                "calculation": turnover_field,
                "interval_basis": interval_basis,
                "source_timestamp_field": "closeTime"
                if e == "binance"
                else "ts"
                if e in {"okx", "bitget"}
                else None,
                "source_timestamp_meaning": "statistics_interval_end; last-price timestamp not separately supplied"
                if e == "binance"
                else "ticker_generation_time"
                if source_time
                else "not_supplied",
                "price_timestamp_basis": "source ticker time proxy"
                if source_time
                else "fetch-time proxy; price timestamp not supplied",
            }
        )
        open_value = number(oi)
        return Observation(
            instrument=instrument,
            fetched_at=fetched,
            source_observed_at=source_time,
            window_start=start,
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
            interpretation=evidence,
            mapping_version="current-metrics-v3-activity-scope",
        )


def positive(value: object) -> Decimal | None:
    parsed = number(value)
    return parsed if parsed is not None and parsed > 0 else None


def epoch_ns(value: object, *, seconds: bool = False) -> int | None:
    if value in (None, "", "0", 0, "-1", -1):
        return None
    parsed = number(value)
    if parsed is None:
        return None
    result = parsed * (1_000_000_000 if seconds else 1_000_000)
    if result != result.to_integral_value() or result > 9223372036854775807:
        raise MarketDataError("Invalid contract timestamp.")
    return int(result)
