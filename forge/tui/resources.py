"""Bounded resource queries and stable workspace row identities."""

import json
import tomllib
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import cast

from forge.datasets.query import DatasetRequest, publications, timestamp
from forge.historical.archives.storage.files import disk
from forge.historical.conversion.recipes import library as conversion_library
from forge.historical.profiles.service import discover
from forge.orchestration.inspection import run_page
from forge.orchestration.run_summary import summarize
from forge.shared.config.library import list_config_names
from forge.startup.context import ApplicationContext
from forge.tui.actions import Values
from forge.tui.controller import Controller, document
from forge.tui.inventory import converted_from_reports

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
    raw_complete: bool = False,
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
        if view in {"Raw", "Raw storage"}:
            result = await inspect(
                "storage list",
                {
                    "limit": 50,
                    "offset": offset,
                    **({"search": search} if search else {}),
                    **options,
                    **(
                        {
                            "sort": {
                                "Archive": "relative_path",
                                "Bytes": "bytes_written",
                                "Archive UTC": "start_at",
                                "Local state": "status",
                            }.get(sort, "id"),
                            "descending": descending,
                        }
                        if sort
                        else {}
                    ),
                    **({"status": "complete"} if raw_complete else {}),
                },
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
        elif view in {"Publications", "Normalized", "Merged"}:
            if view == "Publications" and not options and not sort:
                result = await publications(paths, instrument=search or None, offset=offset)
            else:
                request = DatasetRequest(
                    "list",
                    operation={"Merged": "merge", "Normalized": "normalize"}.get(view),
                    data_type=str(options["data_type"]) if "data_type" in options else None,
                    publication=str(options["publication"]) if "publication" in options else None,
                    exchange=str(options["exchange"]) if "exchange" in options else None,
                    instrument=str(options["instrument"]) if "instrument" in options else None,
                    instrument_type=str(options["instrument_type"])
                    if "instrument_type" in options
                    else None,
                    market_category=str(options["market_category"])
                    if "market_category" in options
                    else None,
                    start=timestamp(str(options["start"])) if "start" in options else None,
                    end=timestamp(str(options["end"])) if "end" in options else None,
                )
                result = await publications(
                    paths,
                    filters=request,
                    search=search,
                    offset=offset,
                    sort=sort,
                    descending=descending if sort else True,
                )
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
        elif view == "Converted":
            retained, truncated = await disk(
                run_page,
                paths.runs_dir,
                100,
                0,
                include_managed=True,
                managed_prefix="convert-execute-",
            )
            for item in converted_from_reports(retained).records:
                identity = str(item["run_id"])
                metadata = await disk(converted_metadata, paths.datasets_dir / "converted", item)
                rows.append(
                    Row(
                        identity,
                        Path(str(metadata["manifest"])).parent.name,
                        "Converted output",
                        str(metadata["metadata_state"]),
                        {**item, **metadata, "bounded_inventory": truncated},
                        identity,
                    )
                )
            matching = [
                row
                for row in rows
                if all(document(row.value).get(key) == value for key, value in options.items())
            ]
            return local_page(matching, search, offset, sort, descending)
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
    elif area == "Catalog":
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
        "merge": paths.config_dir / "historical/merge",
        "convert": conversion_library(paths),
        "acquisition import": paths.acquisition_config_dir / "import",
        "acquisition register": paths.config_dir / "historical/registration",
        "markets rank": paths.config_dir / "markets/rankings",
    }[family]


def converted_metadata(root: Path, receipt: Mapping[str, object]) -> dict[str, object]:
    publication = document(receipt.get("publication"))
    manifest = Path(str(publication.get("manifest", "")))
    try:
        if (
            manifest.name != "manifest.json"
            or manifest.parent.parent.resolve() != root.resolve()
            or manifest.parent.name.endswith(".staging")
            or manifest.resolve() != manifest
        ):
            raise ValueError("Retained manifest is outside the managed conversion root.")
        with manifest.open("rb") as source:
            data = source.read(4 * 1024 * 1024 + 1)
        if len(data) > 4 * 1024 * 1024:
            raise ValueError("Converted manifest exceeds the 4 MiB inspection limit.")
        value: object = json.loads(data)
        metadata = document(value)
        output = document(metadata.get("output"))
        if metadata.get("protocol_version") != 1 or not output or not metadata.get("job_id"):
            raise ValueError("Converted manifest version or output metadata is unavailable.")
        if receipt.get("job_id") is not None and receipt.get("job_id") != metadata["job_id"]:
            raise ValueError("Converted manifest does not match the retained job.")
        files = records(output, "files")
        sizes = [entry.get("size_bytes") for entry in files]
        known = (
            sum(size for size in sizes if isinstance(size, int))
            if (sizes or output.get("files_written") == 0)
            and all(isinstance(size, int) for size in sizes)
            else None
        )
        return {
            "manifest": str(manifest),
            "content_type": output.get("content_type"),
            "bytes_written": known,
            "first_observed_ns": output.get("first_timestamp_ns"),
            "last_observed_ns": output.get("last_timestamp_ns"),
            "primary_files": files,
            "metadata_state": "Retained commit",
        }
    except (OSError, ValueError) as error:
        return {
            "manifest": str(manifest),
            "metadata_state": "Manifest unavailable",
            "metadata_error": str(error),
        }
