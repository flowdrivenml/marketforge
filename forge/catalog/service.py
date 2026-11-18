"""Testing catalogue operations over the existing PostgreSQL authority."""

from datetime import datetime
from decimal import Decimal
from typing import Any, LiteralString

from psycopg import AsyncConnection
from psycopg.rows import TupleRow, dict_row
from psycopg.types.json import Jsonb

from forge.catalog.models import Exchange, Instrument, Specification, reference
from forge.historical.imports.models import HistoricalSpecs, ImportIdentity
from forge.historical.metadata.policy import NAMESPACE, economic_signature, fingerprint
from forge.shared.errors import MarketForgeError

Connection = AsyncConnection[TupleRow]
IDENTITIES: LiteralString = """
 SELECT i.id, e.code AS exchange, i.instrument_type, i.market_category, i.symbol,
        i.base_asset, i.quote_asset, i.settlement_asset, i.status,
        a.instrument_id AS historical_instrument_id, a.namespace, a.definition AS authority
 FROM catalog.instruments i JOIN catalog.exchanges e ON e.id=i.exchange_id
 LEFT JOIN historical.instrument_authority a ON a.namespace=%s AND
 a.identity_key=e.code||'/'||i.instrument_type||'/'||i.market_category||'/'||i.symbol
"""
SPECS: LiteralString = """
 SELECT id, fingerprint, evidence_kind, source_observed_at, definition
 FROM historical.specification_evidence WHERE namespace=%s AND instrument_id=%s ORDER BY id
"""
COLUMNS = (
    "quantity_type",
    "contract_value",
    "contract_value_asset",
    "tick_size",
    "qty_step",
    "min_qty",
    "max_qty",
    "min_notional",
)


async def rows(
    c: Connection, query: LiteralString, args: tuple[Any, ...] = ()
) -> list[dict[str, Any]]:
    async with c.cursor(row_factory=dict_row) as cur:
        await cur.execute(query, args)
        return list(await cur.fetchall())


async def exchange(c: Connection, code: str) -> dict[str, Any]:
    found = await rows(c, "SELECT id,code,name FROM catalog.exchanges WHERE code=%s", (code,))
    if not found:
        raise MarketForgeError(
            f"Exchange {code!r} is missing. Register it with mf catalog exchange add."
        )
    return found[0]


async def instrument(
    c: Connection, identity: ImportIdentity, *, lock: bool = False
) -> dict[str, Any]:
    await exchange(c, identity.exchange)
    fields = identity.model_dump(
        include={"exchange", "instrument_type", "market_category", "symbol"}
    )
    found = await rows(
        c,
        IDENTITIES
        + " WHERE (e.code,i.instrument_type,i.market_category,i.symbol)=(%s,%s,%s,%s)"
        + (" FOR UPDATE OF i" if lock else ""),
        (
            NAMESPACE,
            identity.exchange,
            identity.instrument_type,
            identity.market_category,
            identity.symbol,
        ),
    )
    if not found:
        raise MarketForgeError(
            f"Instrument {identity.symbol!r} is missing under {identity.exchange}. Register it with mf catalog instrument add."
        )
    result = found[0]
    authority = result.pop("authority")
    if result["historical_instrument_id"] is None or any(
        authority.get(key) != value for key, value in fields.items()
    ):
        raise MarketForgeError("Instrument is missing a matching historical authority mapping.")
    return result


async def add(c: Connection, declaration: Exchange | Instrument | Specification) -> dict[str, Any]:
    async with c.transaction():
        if isinstance(declaration, Exchange):
            await c.execute(
                "INSERT INTO catalog.exchanges(code,name) VALUES(%s,%s) ON CONFLICT(code) DO NOTHING",
                (declaration.code, declaration.name),
            )
            result = await exchange(c, declaration.code)
            if result["name"] != declaration.name:
                raise MarketForgeError("Exchange declaration conflicts with the existing name.")
            return result
        if isinstance(declaration, Instrument):
            venue = await exchange(c, declaration.exchange)
            await c.execute(
                """INSERT INTO catalog.instruments(exchange_id,instrument_type,market_category,symbol,
                base_asset,quote_asset,settlement_asset,status) VALUES(%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT(exchange_id,instrument_type,market_category,symbol) DO NOTHING""",
                (
                    venue["id"],
                    declaration.instrument_type,
                    declaration.market_category,
                    declaration.symbol,
                    declaration.base_asset,
                    declaration.quote_asset,
                    declaration.settlement_asset,
                    declaration.status,
                ),
            )
            result = await instrument(c, declaration)
            if any(result[k] != v for k, v in declaration.model_dump().items()):
                raise MarketForgeError(
                    "Instrument declaration conflicts with existing catalogue metadata."
                )
            return result
        return await add_specification(c, declaration)


def bounds(definition: dict[str, Any]) -> tuple[datetime, datetime] | None:
    start, end = definition.get("valid_from"), definition.get("valid_to")
    return (datetime.fromisoformat(start), datetime.fromisoformat(end)) if start and end else None


def signature(definition: dict[str, Any]) -> tuple[str, Any]:
    facts: dict[str, Any] = definition.get("instrument_facts") or {}
    return economic_signature(definition), (
        definition.get("settlement_asset") or facts.get("settlement_asset")
    )


async def add_specification(c: Connection, declaration: Specification) -> dict[str, Any]:
    # Serialize specification checks and writes for this catalogue instrument.
    entry = await instrument(c, reference(declaration.instrument), lock=True)
    values = declaration.model_dump(exclude={"instrument"})
    assets = ("base_asset", "quote_asset", "settlement_asset")
    for key in assets:
        if values[key] is not None and entry[key] is not None and values[key] != entry[key]:
            raise MarketForgeError(f"Specification {key} conflicts with the catalogue instrument.")
        values[key] = values[key] or entry[key]
    if not values["base_asset"] or not values["quote_asset"]:
        raise MarketForgeError("Specification requires known base and quote assets.")
    definition = HistoricalSpecs.model_validate(values).model_dump(mode="json")
    identity = (NAMESPACE, entry["historical_instrument_id"])
    revision = bounds(definition)
    definition["historically_verified"] = False
    definition["record_kind"] = "historical_declaration" if revision else "current_observation"
    key = fingerprint(definition)
    for prior in await rows(c, SPECS, identity):
        if prior["fingerprint"] == key:
            return prior
        old = prior["definition"]
        if (
            revision is None
            and old.get("record_kind") == "current_observation"
            and prior["source_observed_at"] == declaration.fetched_at
            and signature(old) != signature(definition)
        ):
            raise MarketForgeError(
                "Current specification conflicts with an observation at that timestamp."
            )
        old_bounds = bounds(old)
        if (
            revision
            and old_bounds
            and prior["evidence_kind"] in {"historically_verified", "historical_declaration"}
            and revision[0] < old_bounds[1]
            and old_bounds[0] < revision[1]
            and signature(old) != signature(definition)
        ):
            raise MarketForgeError(
                f"Historical specification conflicts with overlapping evidence {prior['id']}."
            )
    if revision is None:
        previous = await rows(
            c,
            "SELECT * FROM catalog.instrument_specs WHERE instrument_id=%s FOR UPDATE",
            (entry["id"],),
        )
        observation = declaration.fetched_at
        assert observation is not None
        if (
            previous
            and previous[0]["fetched_at"] == observation
            and any(
                previous[0][key]
                != (
                    Decimal(values[key]) if key == "contract_value" and values[key] else values[key]
                )
                for key in COLUMNS
            )
        ):
            raise MarketForgeError(
                "Current specification conflicts with the existing observation at that timestamp."
            )
        await c.execute(
            """INSERT INTO catalog.instrument_specs(instrument_id,quantity_type,contract_value,
            contract_value_asset,tick_size,qty_step,min_qty,max_qty,min_notional,fetched_at)
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT(instrument_id) DO UPDATE SET quantity_type=excluded.quantity_type,
            contract_value=excluded.contract_value,contract_value_asset=excluded.contract_value_asset,
            tick_size=excluded.tick_size,qty_step=excluded.qty_step,min_qty=excluded.min_qty,
            max_qty=excluded.max_qty,min_notional=excluded.min_notional,fetched_at=excluded.fetched_at
            WHERE catalog.instrument_specs.fetched_at<excluded.fetched_at""",
            (entry["id"], *(values[key] for key in COLUMNS), observation),
        )
    return (
        await rows(
            c,
            """INSERT INTO historical.specification_evidence(namespace,instrument_id,fingerprint,definition,
        source_instrument_id,source_observed_at) VALUES(%s,%s,%s,%s,%s,%s)
        RETURNING id,fingerprint,evidence_kind,source_observed_at,definition""",
            (*identity, key, Jsonb(definition), entry["id"], declaration.fetched_at),
        )
    )[0]


async def browse(c: Connection, kind: str, key: str | None) -> dict[str, Any]:
    if kind == "exchange":
        return (
            await exchange(c, key)
            if key
            else {
                "exchanges": await rows(
                    c, "SELECT id,code,name FROM catalog.exchanges ORDER BY code"
                )
            }
        )
    if key is not None:
        try:
            identity = reference(key)
        except ValueError as error:
            raise MarketForgeError(
                "Expected exchange.instrument_type.market_category.symbol."
            ) from error
        entry = await instrument(c, identity)
        if kind == "instrument":
            return entry
        return {
            "instrument": entry,
            "current": next(
                iter(
                    await rows(
                        c,
                        "SELECT * FROM catalog.instrument_specs WHERE instrument_id=%s",
                        (entry["id"],),
                    )
                ),
                None,
            ),
            "evidence": await rows(c, SPECS, (NAMESPACE, entry["historical_instrument_id"])),
        }
    if kind == "instrument":
        entries = await rows(
            c,
            IDENTITIES + " ORDER BY e.code,i.instrument_type,i.market_category,i.symbol",
            (NAMESPACE,),
        )
        for entry in entries:
            entry.pop("authority")
        return {"instruments": entries}
    return {
        "current_specifications": await rows(
            c,
            """SELECT e.code AS exchange,i.instrument_type,i.market_category,i.symbol,s.*
            FROM catalog.instrument_specs s JOIN catalog.instruments i ON i.id=s.instrument_id
            JOIN catalog.exchanges e ON e.id=i.exchange_id
            ORDER BY e.code,i.instrument_type,i.market_category,i.symbol""",
        ),
        "specifications": await rows(
            c,
            """SELECT a.identity_key,s.id,s.fingerprint,s.evidence_kind,s.definition
        FROM historical.specification_evidence s JOIN historical.instrument_authority a
        ON (a.namespace,a.instrument_id)=(s.namespace,s.instrument_id)
        WHERE s.namespace=%s ORDER BY a.identity_key,s.id""",
            (NAMESPACE,),
        ),
    }
