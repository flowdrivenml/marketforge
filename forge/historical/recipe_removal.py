"""Build ordered drafts for removal from supported historical recipe collections."""

from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass
from typing import cast

from forge.historical.archives.config import ArchiveRequest
from forge.shared.database.connection import pinned_registry
from forge.shared.errors import MarketForgeError
from forge.shared.paths import ProjectPaths

type Document = dict[str, object]


@dataclass(frozen=True, slots=True)
class Removal:
    path: tuple[str | int, ...]
    indices: tuple[int, ...]
    label: str

    def apply(self, original: Document) -> Document:
        draft = deepcopy(original)

        def child(parent: object, key: str | int) -> object:
            if isinstance(key, int):
                return cast(list[object], parent)[key]
            return cast(Document, parent)[key]

        parent: object = draft
        for component in self.path[:-1]:
            parent = child(parent, component)
        key = self.path[-1]
        entry = cast(Document, parent)
        assert isinstance(key, str)
        if not self.indices:
            del entry[key]
        else:
            values = cast(list[object], entry[key])
            entry[key] = [value for index, value in enumerate(values) if index not in self.indices]
        return draft


def request_identity(request: ArchiveRequest) -> tuple[str, str, str, str]:
    return (
        request.exchange,
        request.instrument_type,
        request.market_category,
        request.scope.identifier,
    )


def configured_removals(family: str, data: Mapping[str, object]) -> tuple[Removal, ...]:
    """Enumerate removals using the configured identity domain, never resolved results."""
    result: list[Removal] = []

    def choices(values: object, path: tuple[str | int, ...], label: str) -> None:
        if isinstance(values, list):
            for index, item in enumerate(cast(list[object], values)):
                result.append(Removal(path, (index,), f"{label} {item}"))

    if family in {"acquisition", "pipeline", "normalize"} and isinstance(
        data.get("requests"), list
    ):
        groups: dict[tuple[str, str, str, str], list[int]] = {}
        for index, raw in enumerate(cast(list[Document], data["requests"])):
            scope = cast(Document, raw["scope"])
            label = " · ".join(
                str(raw[k]) for k in ("exchange", "market_category", "instrument_type") if k in raw
            )
            result.append(
                Removal(
                    ("requests",),
                    (index,),
                    f"Request {index + 1} · {label} · {scope['kind']} {scope['identifier']} · {raw['data_type']}",
                )
            )
            if family != "normalize":
                request = ArchiveRequest.model_validate(raw)
                if request.scope.kind == "instrument":
                    groups.setdefault(request_identity(request), []).append(index)
        for identity, indices in groups.items():
            if len(indices) > 1:
                result.append(
                    Removal(("requests",), tuple(indices), "All requests · " + " · ".join(identity))
                )
    if family == "normalize":
        for index, pin in enumerate(cast(list[Document], data.get("archive_pins", []))):
            result.append(
                Removal(("archive_pins",), (index,), f"Registered raw archive {pin['archive_id']}")
            )
        for index, item in enumerate(cast(list[Document], data.get("inputs", []))):
            result.append(
                Removal(
                    ("inputs",), (index,), f"Archive {item['archive_id']} · Task {item['task_id']}"
                )
            )
    if family == "merge":
        selection = data.get("selection")
        if isinstance(selection, dict):
            choices(
                cast(Document, selection).get("publication_ids"),
                ("selection", "publication_ids"),
                "Normalized publication",
            )
        elif "publication_ids" in data and not data.get("file_subset"):
            choices(data["publication_ids"], ("publication_ids",), "Whole normalized publication")
        else:
            choices(data.get("dataset_ids"), ("dataset_ids",), "Registered primary file")
    if family == "convert":
        choices(data.get("reference_stream_ids"), ("reference_stream_ids",), "Reference stream")
        if "reference_publication_id" in data:
            result.append(
                Removal(
                    ("reference_publication_id",),
                    (),
                    f"External reference publication {data['reference_publication_id']} and its selected streams",
                )
            )
    if family == "pipeline" and isinstance(data.get("convert"), dict):
        choices(
            cast(Document, data["convert"]).get("reference_stream_ids"),
            ("convert", "reference_stream_ids"),
            "Conversion reference stream",
        )
        result.append(Removal(("convert",), (), "Optional Conversion stage"))
    if family == "acquisition import":
        choices(data.get("files"), ("files",), "Source file")
        choices(data.get("patterns"), ("patterns",), "Source pattern")
    if family == "acquisition register":
        for index, item in enumerate(cast(list[Document], data.get("inputs", []))):
            result.append(
                Removal(
                    ("inputs",),
                    (index,),
                    f"Input {index + 1} · {item['instrument']} · {item['content']}",
                )
            )
            for key in ("files", "patterns"):
                choices(item.get(key), ("inputs", index, key), f"Input {index + 1} · {key} ·")
    if family == "markets rank":
        for key in ("exchanges", "markets", "instruments"):
            values = data.get(key)
            if isinstance(values, list) and len(cast(list[object], values)) > 1:
                choices(data.get(key), (key,), key.capitalize())
        choices(data.get("manual_preferences"), ("manual_preferences",), "Manual preference")
    return tuple(result)


async def remove_configured_input(
    data: Document, family: str, removal: Removal, *, paths: ProjectPaths, registry: str | None
) -> Document:
    """Remove a configured entry and refresh dependent merge pins when necessary."""
    if removal not in configured_removals(family, data):
        raise MarketForgeError("The selected input is no longer configured.")
    draft = removal.apply(data)
    if family == "convert" and removal.path == ("reference_publication_id",):
        from forge.historical.archives.storage.files import disk
        from forge.historical.conversion.discovery import discover
        from forge.historical.conversion.planning import references
        from forge.historical.conversion.recipes import Recipe as ConversionRecipe
        from forge.historical.conversion.recipes import validate_binding

        conversion = ConversionRecipe.model_validate(data)
        if conversion.selection_policy(conversion.version).mode == "automatic":
            return draft
        source = await disk(validate_binding, paths, paths.workflow_dir("conversion"), conversion)
        with pinned_registry(registry):
            found = await discover(paths, conversion, source)
        if found["blockers"]:
            raise MarketForgeError("\n".join(found["blockers"]))
        selected = references(
            found["sources"][0], found["external_reference"], conversion.reference_stream_ids
        )
        draft["reference_stream_ids"] = [
            item["stream_id"] for item in selected if item["provider"] == "internal"
        ]
    if family != "merge" or "selection" in data:
        return draft

    from forge.historical.merge.service import (
        Recipe,
        assign_ranks,
        publication_selection_sha256,
        reviewed_inputs,
    )
    from forge.shared.database.connection import selected_registry_id

    recipe = Recipe.model_validate(data)
    if "publication_ids" not in data and not recipe.stream_ranks:
        return draft
    registry = registry or await selected_registry_id(paths.root)
    selected = await reviewed_inputs(
        paths, recipe, recipe.dataset_ids, registry, include_publication_ids=True
    )
    publications = cast(list[int], draft.get("publication_ids", []))
    ids = cast(list[int], draft["dataset_ids"])
    retained = (
        [item for item in selected if item["registry_publication_id"] in publications]
        if removal.path == ("publication_ids",)
        else [item for item in selected if item["database_id"] in ids]
    )
    if not retained:
        raise MarketForgeError("At least one merge input is required.")
    if recipe.publication_ids is not None:
        membership = {item["registry_publication_id"] for item in retained}
        publications = [identity for identity in publications if identity in membership]
        draft.update(
            dataset_ids=[item["database_id"] for item in retained],
            publication_ids=publications,
            selection_sha256=publication_selection_sha256(retained),
        )
    if recipe.stream_ranks:
        original_order = assign_ranks(selected, recipe.stream_ranks, {})
        streams = {item["stream_id"] for item in retained}
        ranks = {stream: rank for stream, rank in recipe.stream_ranks.items() if stream in streams}
        proposed_order = assign_ranks(retained, ranks, {})
        expected = [item for item in original_order if item["stream_id"] in streams]
        if [item["stream_id"] for item in expected] != [
            item["stream_id"] for item in proposed_order
        ]:
            ranks = {item["stream_id"]: item["stream_rank"] for item in expected}
        draft["stream_ranks"] = ranks
    return draft
