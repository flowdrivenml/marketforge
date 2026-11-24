"""Bounded resource queries and stable workspace row identities."""

import json
import tomllib
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import cast

from forge.datasets import workspace_inventory
from forge.historical.archives.storage.files import disk
from forge.historical.conversion.recipes import library as conversion_library
from forge.historical.profiles.service import discover
from forge.orchestration.inspection import run_page
from forge.orchestration.registry import list_index
from forge.orchestration.run_summary import summarize
from forge.shared.config.library import list_config_names
from forge.startup.context import ApplicationContext
from forge.tui.actions import Values
from forge.tui.controller import Controller, document

FAMILIES = {
    "Pipeline": "pipeline",
    "Acquisition": "acquisition",
    "Normalization": "normalize",
    "Merge": "merge",
    "Conversion": "convert",
    "Import": "acquisition import",
    "Registration": "acquisition register",
    "Policies": "markets rank",
}
CATALOG = {"Exchanges": "exchange", "Instruments": "instrument", "Specifications": "specs"}


@dataclass(frozen=True, slots=True)
class Row:
    id: str
    name: str
    kind: str
    state: str
    value: object
    reference: str = ""
    path: Path | None = None


def records(value: object, key: str) -> list[Mapping[str, object]]:
    items = document(value).get(key)
    return (
        [document(item) for item in cast(list[object], items) if document(item)]
        if isinstance(items, list)
        else []
    )


def value_at(item: Mapping[str, object], key: str, default: str = "—") -> str:
    value = item.get(key)
    return str(value) if value is not None else default


async def load_rows(
    controller: Controller,
    area: str,
    view: str,
    search: str,
    offset: int,
    *,
    filters: Values | None = None,
    sort: str = "",
    descending: bool = False,
) -> tuple[list[Row], bool]:
    context = controller.context
    paths = context.paths
    rows: list[Row] = []
    more = False
    options = {key: item for key, item in (filters or {}).items() if item != ""}

    async def inspect(key: str, values: Values) -> object:
        return await controller.invoke(key, values, context, lambda _: None, controller.editor)

    if (family := FAMILIES.get(view)) is not None and area in {"Workflows", "Markets"}:
        directory = library(context, family)
        rows = [
            Row(
                name,
                name,
                family,
                "Saved",
                {"path": str(directory / f"{name}.toml")},
                name,
                directory / f"{name}.toml",
            )
            for name in await disk(list_config_names, directory)
        ]
    elif area == "Catalogue" and view == "Profiles":
        profiles = await disk(
            discover,
            paths,
            **{
                key: str(val)
                for key, val in options.items()
                if key in {"type", "exchange", "instrument_type", "market_category"}
            },
        )
        for item in profiles:
            reference = str(item["id"])
            exchange, instrument_type, market, kind = reference.split(".")
            definition = document(item["definition"])
            source_format = document(definition.get("source")).get("format")
            if search.casefold() not in f"{reference} {source_format or ''}".casefold():
                continue
            if options.get("format") and source_format != options["format"]:
                continue
            row = {
                **item,
                "exchange": exchange,
                "instrument_type": instrument_type,
                "market_category": market,
                "data_kind": kind,
                "format": source_format,
            }
            rows.append(
                Row(
                    reference,
                    reference,
                    "Profile",
                    "Syntax checked",
                    row,
                    reference,
                    Path(item["location"]),
                )
            )
        return rows[offset : offset + 50], len(rows) > offset + 50
    elif area == "Data":
        items, more = await workspace_inventory.stage_page(paths, view, offset, search)
        kind = {
            "Raw": "Raw Archive",
            "Normalized": "Publication",
            "Merged": "Publication",
            "Converted": "Indexed conversion output",
        }[view]
        for item in items:
            reference = str(item.get("publication") or item.get("output_ref") or "")
            if view == "Raw":
                key = json.dumps(item["archives"])
            elif view == "Normalized":
                key = json.dumps(
                    [
                        reference,
                        item["namespace"],
                        item["exchange"],
                        item["instrument"],
                        item["stream"],
                        item["data_type"],
                    ]
                )
            else:
                key = reference
            if kind == "Publication":
                item["operation"] = "normalize" if view == "Normalized" else "merge"
            rows.append(
                Row(
                    key,
                    str(item.get("symbol") or reference),
                    kind,
                    str(item["state"]),
                    item,
                    reference,
                )
            )
        return rows, more
    elif area == "Markets":
        key = "markets rank list" if view == "Rankings" else "markets list"
        payload: Values = {"limit": 50, "offset": offset, **options}
        if search and view == "Instruments":
            payload["search"] = search
        if sort and view == "Instruments":
            payload.update(
                sort={
                    "Symbol": "symbol",
                    "Exchange": "exchange",
                    "Turnover 24h": "turnover",
                    "Observed UTC": "measured",
                }.get(sort, "identity"),
                descending=descending,
            )
        if view == "Rankings":
            payload["search"] = search
            if sort:
                payload.update(sort=sort, descending=descending)
        result = await inspect(key, payload)
        for index, item in enumerate(
            records(result, "rankings" if view == "Rankings" else "instruments")
        ):
            ident = value_at(item, "reference", value_at(item, "id", str(index)))
            rows.append(
                Row(
                    ident,
                    value_at(item, "symbol", value_at(item, "policy_name", ident)),
                    view,
                    value_at(item, "status"),
                    item,
                    ident,
                )
            )
        return rows, bool(document(result).get("has_more"))
    elif area == "Catalogue":
        entity = CATALOG[view]
        payload: Values = {
            "limit": 51,
            "offset": offset,
            "search": search,
            **options,
            "sort": sort,
            "descending": descending,
        }
        result = document(await inspect(f"catalog {entity} list", payload))
        for key in result:
            entries = records(result, key)
            more |= len(entries) > 50
            for index, item in enumerate(entries[:50]):
                ident = value_at(item, "id", str(index))
                reference = ".".join(
                    value_at(item, "code", value_at(item, "identity_key", "")).split("/", 3)
                )
                if not reference:
                    reference = ".".join(
                        value_at(item, field)
                        for field in (
                            "exchange",
                            "instrument_type",
                            "market_category",
                            "symbol",
                        )
                    )
                if entity == "specs":
                    exchange, instrument_type, market, symbol = reference.split(".", 3)
                    item = {
                        **item,
                        "exchange": exchange,
                        "instrument_type": instrument_type,
                        "market_category": market,
                        "symbol": symbol,
                    }
                    if key == "current_specifications":
                        ident = value_at(item, "instrument_id")
                rows.append(
                    Row(
                        f"{key}:{ident}",
                        value_at(item, "symbol", reference),
                        entity,
                        value_at(item, "status"),
                        item,
                        reference,
                    )
                )
        return rows, more
    elif area == "Runs":
        if view == "Indexed":
            result = await list_index(
                paths,
                search=search or None,
                limit=50,
                offset=offset,
                sort=sort or None,
                order=("desc" if descending else "asc") if sort else None,
                **{
                    name: str(options[name])
                    for name in ("operation", "action", "status", "recipe", "start", "end")
                    if name in options
                },
            )
            for item in records(result, "runs"):
                identity = value_at(item, "run_id")
                rows.append(
                    Row(identity, identity, "Indexed run", value_at(item, "status"), item, identity)
                )
            return rows, result["more"] is True
        if view == "Owned":
            for op in [
                *controller.history,
                *([controller.current] if controller.active() else []),
            ]:
                if op is not None:
                    rows.append(Row(op.id, op.title, "Owned", op.state, op, op.id))
        else:
            retained, more = await disk(
                run_page,
                paths.runs_dir,
                50,
                offset,
                search,
                include_managed=True,
                filters={key: str(value) for key, value in options.items()},
                sort=sort,
                descending=descending,
            )
            for index, item in enumerate(retained):
                ident = value_at(item, "run_id", str(index))
                rows.append(
                    Row(
                        ident,
                        value_at(item, "name", value_at(item, "recipe", ident[:8])),
                        value_at(item, "kind", "Retained"),
                        (
                            "Recorded running"
                            if value_at(item, "state", value_at(item, "status")) == "running"
                            else value_at(item, "state", value_at(item, "status"))
                        ),
                        {
                            **item,
                            "run_context": item.get("run_context")
                            or asdict(summarize(item, paths.runs_dir / ident)),
                        },
                        ident,
                    )
                )
            return rows, more
    selected, more = local_page(rows, search, offset, sort, descending)
    if any(row.path is not None and row.kind != "Profile" for row in selected):
        selected = await disk(recipe_summaries, selected)
    return selected, more


def recipe_summaries(rows: list[Row]) -> list[Row]:
    from dataclasses import replace

    result: list[Row] = []
    for row in rows:
        if row.path is None or row.kind == "Profile":
            result.append(row)
            continue
        try:
            with row.path.open("rb") as stream:
                source = stream.read(65537)
            data = tomllib.loads(source.decode()) if len(source) <= 65536 else {}
            detail = document(data.get("interval"))
            result.append(
                replace(
                    row,
                    value={
                        **document(row.value),
                        "interval": f"{detail['start']} → {detail['end']}"
                        if detail.get("start") and detail.get("end")
                        else None,
                    },
                )
            )
        except (UnicodeError, tomllib.TOMLDecodeError):
            result.append(replace(row, state="Invalid TOML"))
        except OSError:
            result.append(replace(row, state="Unavailable"))
    return result


def local_page(
    rows: list[Row], search: str, offset: int, sort: str, descending: bool
) -> tuple[list[Row], bool]:
    normalized = search.casefold().strip()
    selected = [
        row
        for row in rows
        if normalized
        in f"{row.name} {row.kind} {row.reference} {row.path or document(row.value).get('manifest', '')}".casefold()
    ]
    selected.sort(key=lambda row: row.id)
    if sort in {"Physical rows", "Recorded size", "Observed UTC"}:

        def total(row: Row) -> int | None:
            detail = document(row.value)
            value = (
                document(detail.get("publication")).get("events")
                if sort == "Physical rows"
                else detail.get("bytes_written")
                if sort == "Recorded size"
                else detail.get("last_observed_ns")
            )
            return value if isinstance(value, int) else None

        known = [(value, row) for row in selected if (value := total(row)) is not None]
        known.sort(key=lambda item: item[0], reverse=descending)
        selected = [row for _, row in known] + [row for row in selected if total(row) is None]
    elif sort:
        selected.sort(
            key=lambda row: (
                row.state if sort == "State" else row.kind if sort == "Workflow" else row.name
            ).casefold(),
            reverse=descending,
        )
    return selected[offset : offset + 50], len(selected) > offset + 50


def library(context: ApplicationContext, family: str) -> Path:
    paths = context.paths
    return {
        "pipeline": paths.pipeline_config_dir,
        "acquisition": paths.acquisition_config_dir,
        "normalize": paths.normalization_recipe_dir,
        "merge": paths.workflow_dir("merge"),
        "convert": conversion_library(paths),
        "acquisition import": paths.workflow_dir("import"),
        "acquisition register": paths.workflow_dir("import") / "registration",
        "markets rank": paths.config_dir / "markets/rankings",
    }[family]
