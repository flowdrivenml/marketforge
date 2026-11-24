"""Build ordered drafts for removal from supported historical recipe collections."""

from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass
from typing import cast

from forge.historical.archives.config import ArchiveRequest
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
        parent: object = draft
        for key in self.path[:-1]:
            parent = (
                cast(list[object], parent)[key]
                if isinstance(key, int)
                else cast(Document, parent)[key]
            )
        key = self.path[-1]
        entry = parent
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
                    f"External reference publication {data['reference_publication_id']}",
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
        for key in ("exchanges", "markets", "instruments", "manual_preferences"):
            choices(data.get(key), (key,), key.replace("_", " ").capitalize())
    return tuple(result)


async def remove_configured_input(
    data: Document, family: str, removal: Removal, *, paths: ProjectPaths, registry: str | None
) -> Document:
    """Remove a configured entry and refresh dependent merge pins when necessary."""
    if removal not in configured_removals(family, data):
        raise MarketForgeError("The selected input is no longer configured.")
    draft = removal.apply(data)
    if family != "merge" or "publication_ids" not in data:
        return draft
    if data["registry_id"] != registry:
        raise MarketForgeError("This recipe belongs to another registry.")
    from forge.historical.merge.service import Recipe, publication_recipe, reviewed_inputs

    recipe = Recipe.model_validate(data)
    assert registry is not None
    selected = await reviewed_inputs(
        paths, recipe, recipe.dataset_ids, registry, include_publication_ids=True
    )
    if removal.path == ("dataset_ids",):
        ids = cast(list[int], draft["dataset_ids"])
        publications = list(
            dict.fromkeys(
                int(item["registry_publication_id"])
                for item in selected
                if item["database_id"] in ids
            )
        )
    else:
        publications = cast(list[int], draft["publication_ids"])
        ids = [
            int(item["database_id"])
            for item in selected
            if item["registry_publication_id"] in publications
        ]
    if not publications or not ids:
        raise MarketForgeError("At least one merge input is required.")
    pinned, _ = await publication_recipe(
        paths,
        publications,
        dataset_ids=ids if recipe.file_subset else None,
        start_ns=recipe.start_ns,
        end_ns=recipe.end_ns,
        stream_ranks=recipe.stream_ranks,
        timeline_ordering=recipe.timeline_ordering,
    )
    if set(pinned.dataset_ids) != set(ids):
        raise MarketForgeError("The retained publication membership changed.")
    draft.update(
        dataset_ids=ids, publication_ids=publications, selection_sha256=pinned.selection_sha256
    )
    return draft
