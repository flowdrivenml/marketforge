"""Resolve requested archive identity without opening or interpreting its container."""

from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import LiteralString

from psycopg import AsyncConnection
from psycopg.rows import TupleRow, dict_row

from forge.historical.archives.storage.files import contained
from forge.historical.normalization.resolution import positive_decimal, require
from forge.markets.models import obj
from forge.shared.errors import MarketForgeError
from forge.shared.paths import ProjectPaths


@dataclass
class ResolvedArchive:
    task: dict[str, object]
    instrument_id: int | None
    warnings: list[str] = field(default_factory=lambda: list[str]())
    identity_resolution: dict[str, object] = field(default_factory=lambda: dict[str, object]())


def asset(value: object, name: str) -> str:
    require(
        isinstance(value, str) and bool(value.strip()), f"Missing authoritative {name} metadata."
    )
    return str(value)


# Both lookups load the same catalogue specifications; only the identity predicate differs.
INSTRUMENT_SELECT: LiteralString = """SELECT i.*, e.code AS exchange, s.tick_size,
    s.contract_value, s.contract_value_asset, s.quantity_type
    FROM catalog.instruments i JOIN catalog.exchanges e ON e.id=i.exchange_id
    LEFT JOIN catalog.instrument_specs s ON s.instrument_id=i.id """


def bitget_spot_pair(identifier: str) -> tuple[str, str]:
    parts = identifier.split("/")
    require(
        len(parts) == 2 and all(part and not any(c.isspace() for c in part) for part in parts),
        f"Malformed Bitget spot archive identifier {identifier!r}; expected BASE/QUOTE with exactly two nonempty assets.",
    )
    return parts[0], parts[1]


async def resolve_archive(
    connection: AsyncConnection[TupleRow], paths: ProjectPaths, row: dict[str, object], content: str
) -> ResolvedArchive:
    require(row["status"] == "complete", f"Local archive {row['id']} is not complete.")
    path = contained(paths.raw_dir, str(row["relative_path"])).resolve()
    require(path.is_file(), f"Input file is missing: {path}")
    require(
        path.stat().st_size == row["bytes_written"], f"Input size differs from registry: {path}"
    )
    for key in ("exchange", "instrument_type", "market_category", "scope_kind", "scope_identifier"):
        require(
            isinstance(row.get(key), str) and bool(str(row[key]).strip()), f"Missing archive {key}."
        )
    require(
        row["data_type"] == ("trades" if content == "trades" else "order_book_l2"),
        "Archive content type differs from job.",
    )
    require(
        row["instrument_type"] != "spot" or row["market_category"] == "spot",
        "Spot archive market_category must be spot.",
    )
    kind = str(row["scope_kind"])
    require(kind in {"instrument", "family", "base_coin"}, f"Unsupported requested scope: {kind}.")
    async with connection.cursor(row_factory=dict_row) as cursor:
        await cursor.execute(
            INSTRUMENT_SELECT
            + """
               WHERE e.code=%s AND i.market_category=%s AND i.instrument_type=%s
               AND ((%s='instrument' AND i.symbol=%s)
                 OR (%s='base_coin' AND i.base_asset=%s)
                 OR (%s='family' AND
                   (SELECT raw.raw_json->>'instFamily' FROM catalog.instrument_api_raw raw
                    WHERE raw.instrument_id=i.id ORDER BY raw.fetched_at DESC,raw.id DESC LIMIT 1)=%s))
               ORDER BY i.id""",
            (
                row["exchange"],
                row["market_category"],
                row["instrument_type"],
                kind,
                row["scope_identifier"],
                kind,
                row["scope_identifier"],
                kind,
                row["scope_identifier"],
            ),
        )
        instruments = list(await cursor.fetchall())
        resolution: dict[str, object] = {
            "archive_identifier": row["scope_identifier"],
            "method": "exact_symbol" if kind == "instrument" else "requested_scope",
        }
        if (
            not instruments
            and kind == "instrument"
            and (row["exchange"], row["instrument_type"], row["market_category"])
            == ("bitget", "spot", "spot")
        ):
            base, quote = bitget_spot_pair(str(row["scope_identifier"]))
            # Match repository asset-filter semantics without changing registered spelling.
            await cursor.execute(
                INSTRUMENT_SELECT
                + """WHERE e.code='bitget' AND i.instrument_type='spot'
                AND i.market_category='spot' AND lower(i.base_asset)=lower(%s)
                AND lower(i.quote_asset)=lower(%s) ORDER BY i.id""",
                (base, quote),
            )
            instruments = list(await cursor.fetchall())
            identity = f"Bitget spot archive {row['scope_identifier']!r} (base_asset={base!r}, quote_asset={quote!r})"
            require(
                bool(instruments), f"{identity}: no catalogue instrument matches the asset pair."
            )
            candidates = [
                {
                    key: instrument[key]
                    for key in (
                        "id",
                        "exchange",
                        "instrument_type",
                        "market_category",
                        "symbol",
                        "base_asset",
                        "quote_asset",
                    )
                }
                for instrument in instruments
            ]
            require(
                len(instruments) == 1,
                f"{identity}: ambiguous asset-pair match; candidates: {candidates}.",
            )
            resolution.update(
                method="bitget_spot_asset_pair",
                parsed_assets={"base_asset": base, "quote_asset": quote},
            )

    task: dict[str, object] = {
        "task_id": 1,
        "input_path": str(path),
        "exchange": row["exchange"],
        "instrument_type": row["instrument_type"],
        "market_category": row["market_category"],
    }
    warnings: list[str] = []
    instrument_id: int | None = None
    if kind == "instrument":
        require(
            len(instruments) == 1,
            f"Expected one concrete catalogue instrument; found {len(instruments)}.",
        )
        instrument = instruments[0]
        instrument_id = int(instrument["id"])
        resolution["resolved_symbol"] = instrument["symbol"]
        # Explicit historical overrides cannot silently be replaced by current specs.
        overrides = obj(row["specifications_json"]) if row.get("specifications_json") else {}
        for key in (
            "base_asset",
            "quote_asset",
            "tick_size",
            "contract_value",
            "contract_value_asset",
        ):
            if key in overrides and overrides[key] is not None:
                registered = instrument[key]
                value = overrides[key]
                if key in {"tick_size", "contract_value"}:
                    try:
                        number = Decimal(str(value))
                    except InvalidOperation as exc:
                        raise MarketForgeError(
                            f"Invalid external historical {key} metadata."
                        ) from exc
                    require(
                        registered is not None and number.is_finite() and number == registered,
                        f"External historical {key} conflicts with catalogue metadata.",
                    )
                else:
                    require(
                        value == registered,
                        f"External historical {key} conflicts with catalogue metadata.",
                    )
        if row.get("identity_json") is not None:
            external_identity = obj(row["identity_json"])
            if external_identity.get("symbol") is not None:
                require(
                    external_identity["symbol"] == instrument["symbol"]
                    or (
                        resolution["method"] == "bitget_spot_asset_pair"
                        and external_identity["symbol"] == row["scope_identifier"]
                    ),
                    "External symbol conflicts with archive scope identity.",
                )
        for key in ("base_asset", "quote_asset"):
            task[key] = asset(instrument[key], key)
        task["symbol"] = instrument["symbol"]
        contract_value: str | None = None
        contract_asset: str | None = None
        if row["instrument_type"] != "spot" and (
            instrument["contract_value"] is not None
            or instrument.get("quantity_type") == "contracts"
        ):
            contract_value = positive_decimal(instrument["contract_value"], "contract_value")
            contract_asset = asset(instrument["contract_value_asset"], "contract_value_asset")
        task["instrument"] = {
            "tick_size": positive_decimal(instrument["tick_size"], "tick_size"),
            "contract_value": contract_value,
            "contract_value_asset": contract_asset,
        }
        source = str(instrument["symbol"])
    else:
        task.update(
            scope={"kind": kind, "identifier": row["scope_identifier"]},
            symbol=None,
            instrument=None,
        )
        for key in ("base_asset", "quote_asset"):
            values = {
                str(i[key]) for i in instruments if i[key] is not None and str(i[key]).strip()
            }
            complete = bool(instruments) and all(
                i[key] is not None and str(i[key]).strip() for i in instruments
            )
            task[key] = next(iter(values)) if len(values) == 1 and complete else None
            if task[key] is None:
                warnings.append(
                    f"{key} {'conflicts across' if len(values) > 1 else 'unavailable for'} requested {kind} metadata; left null."
                )
        source = f"{kind}:{row['scope_identifier']}"
    task["stream_id"] = (
        ".".join(str(row[k]) for k in ("exchange", "market_category", "instrument_type"))
        + f".{source}.{content}"
    )
    if row.get("feed") is not None:
        asset(row["feed"], "source feed")
        task["source_options"] = {"feed": row["feed"]}
        task["stream_id"] = f"{task['stream_id']}.{row['feed']}"
    return ResolvedArchive(task, instrument_id, warnings, resolution)
