"""Bounded resource queries and stable workspace row identities."""

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from forge.datasets.query import publications
from forge.historical.archives.storage.files import disk
from forge.historical.conversion.recipes import library as conversion_library
from forge.historical.profiles.service import discover
from forge.orchestration.inspection import list_runs, run_page
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
    controller: Controller, area: str, view: str, search: str, offset: int
) -> tuple[list[Row], bool]:
    context = controller.context
    paths = context.paths
    normalized = search.casefold().strip()
    rows: list[Row] = []
    more = False

    async def inspect(key: str, values: Values) -> object:
        return await controller.invoke(key, values, context, lambda _: None, controller.editor)

    if area == "Overview":
        rows = [
            Row(
                "project",
                paths.root.name,
                "Workspace",
                "Local",
                {
                    "workspace": str(paths.root),
                    "storage": str(paths.data_root),
                    "registry": "Explicit testing route",
                },
            ),
            Row(
                "doctor",
                "Check prerequisites",
                "Diagnostics",
                "Unchecked",
                {"action": "doctor"},
            ),
        ]
        for index, item in enumerate(await disk(list_runs, paths.runs_dir, 8)):
            run_id = value_at(item, "run_id", str(index))
            state = value_at(item, "state", value_at(item, "status"))
            rows.append(
                Row(
                    run_id,
                    value_at(item, "name", run_id[:8]),
                    "Retained run",
                    "Last recorded " + state,
                    item,
                    run_id,
                )
            )
    elif (family := FAMILIES.get(view)) is not None and area in {"Workflows", "Markets"}:
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
    elif area == "Recipes & Profiles":
        if view == "Profiles":
            for item in await disk(discover, paths):
                row = str(item["id"])
                rows.append(
                    Row(row, row, "Profile", "Installed", item, row, Path(item["location"]))
                )
        else:
            for family in FAMILIES.values():
                directory = library(context, family)
                for name in await disk(list_config_names, directory):
                    path = directory / f"{name}.toml"
                    rows.append(
                        Row(
                            f"{family}:{name}",
                            name,
                            family,
                            "Saved",
                            {"path": str(path)},
                            name,
                            path,
                        )
                    )
    elif area == "Data":
        if view == "Raw storage":
            result = await inspect(
                "storage list",
                {"limit": 50, "offset": offset, **({"match": search} if search else {})},
            )
            for item in records(result, "archives"):
                ident = value_at(item, "id", value_at(item, "local_id"))
                rows.append(
                    Row(
                        ident,
                        value_at(item, "filename", ident),
                        value_at(item, "data_type"),
                        value_at(item, "status"),
                        item,
                        ident,
                    )
                )
            more = document(document(result).get("pagination")).get("has_more") is True
        elif view == "Publications":
            result = await publications(paths, instrument=search or None, offset=offset)
            for item in records(result, "publications"):
                ident = value_at(item, "registry_publication_id")
                rows.append(
                    Row(
                        ident,
                        value_at(item, "engine_dataset_id"),
                        "Publication",
                        value_at(item, "operation"),
                        item,
                        "",
                    )
                )
            more = bool(result["has_more"])
        else:
            result = await inspect(
                "datasets list",
                {"limit": 50, "offset": offset, **({"instrument": search} if search else {})},
            )
            for item in records(result, "datasets"):
                ident = value_at(item, "id")
                rows.append(
                    Row(
                        ident,
                        ident,
                        value_at(item, "data_type"),
                        value_at(item, "status"),
                        item,
                        ident,
                    )
                )
            more = bool(document(result).get("has_more"))
        return rows, more
    elif area == "Markets":
        key = "markets rank list" if view == "Rankings" else "markets list"
        payload: Values = {"limit": 50, "offset": offset}
        if search and view == "Instruments":
            payload["match"] = search
        if search and view == "Rankings":
            payload["search"] = search
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
    elif area == "Catalog":
        entity = CATALOG[view]
        payload: Values = {"limit": 51, "offset": offset, "search": search}
        result = document(await inspect(f"catalog {entity} list", payload))
        for key in result:
            entries = records(result, key)
            more |= len(entries) > 50
            for index, item in enumerate(entries[:50]):
                ident = value_at(item, "id", str(index))
                reference = value_at(item, "code", value_at(item, "identity_key", "")).replace(
                    "/", "."
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
        if view == "Owned":
            for op in [
                *controller.history,
                *([controller.current] if controller.active() else []),
            ]:
                if op is not None:
                    rows.append(Row(op.id, op.title, "Owned", op.state, op, op.id))
        else:
            retained, more = await disk(run_page, paths.runs_dir, 50, offset, search)
            for index, item in enumerate(retained):
                ident = value_at(item, "run_id", str(index))
                rows.append(
                    Row(
                        ident,
                        value_at(item, "name", ident[:8]),
                        value_at(item, "kind", "Retained"),
                        "Last recorded " + value_at(item, "state", value_at(item, "status")),
                        item,
                        ident,
                    )
                )
            return rows, more
    elif area == "System":
        key = {
            "Engines": "engine status",
            "Database": "database status",
            "Settings": "config show",
        }[view]
        if view == "Engines":
            for item in records(await inspect(key, {}), "targets"):
                ident = value_at(item, "target")
                rows.append(
                    Row(ident, ident, "Managed engine", value_at(item, "status"), item, ident)
                )
        else:
            result = await inspect(key, {})
            for name, item in document(result).items():
                rows.append(Row(name, name, view, "Retained", item))
    return [
        row for row in rows if normalized in f"{row.name} {row.kind} {row.reference}".casefold()
    ][:100], more


def library(context: ApplicationContext, family: str) -> Path:
    paths = context.paths
    return {
        "pipeline": paths.pipeline_config_dir,
        "acquisition": paths.acquisition_config_dir,
        "normalize": paths.normalization_recipe_dir,
        "merge": paths.config_dir / "historical/merge",
        "convert": conversion_library(paths),
        "acquisition import": paths.acquisition_config_dir / "import",
        "acquisition register": paths.config_dir / "historical/registration",
        "markets rank": paths.config_dir / "markets/rankings",
    }[family]
