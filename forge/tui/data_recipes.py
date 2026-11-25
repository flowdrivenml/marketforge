"""Resolve Data selections into drafts using Forge's registered source contracts."""

from collections.abc import Mapping, Sequence
from typing import cast

from forge.datasets.query import DatasetRequest, browse
from forge.historical.conversion.discovery import discover
from forge.historical.conversion.evidence import inspect_publication_source
from forge.historical.conversion.planning import references
from forge.historical.conversion.recipes import PublicationSource
from forge.historical.conversion.recipes import Recipe as ConversionRecipe
from forge.historical.merge.service import Recipe as MergeRecipe
from forge.historical.merge.service import publication_recipe, reviewed_inputs
from forge.historical.normalization.batch import inspect_exact_archives
from forge.historical.normalization.config import ExactArchive, ExactRecipe
from forge.shared.paths import ProjectPaths
from forge.tui.clipboard import plain
from forge.tui.controller import document
from forge.tui.resources import Row, records

DESTINATIONS = {"Raw": "normalize", "Normalized": "merge", "Merged": "convert"}


def archive_ids(rows: Sequence[Row]) -> tuple[int, ...]:
    return tuple(
        dict.fromkeys(
            int(str(ref).removeprefix("local:"))
            for row in rows
            for ref in cast(list[object], document(row.value)["archives"])
        )
    )


async def normalization(
    paths: ProjectPaths, ids: tuple[int, ...], registry: str, before: dict[str, object]
) -> tuple[dict[str, object], str, bool]:
    if before and "archive_pins" not in before:
        raise ValueError(
            "This recipe uses a different source contract. Choose an exact-archive recipe."
        )
    previous = ExactRecipe.model_validate(before) if before else None
    if previous and str(previous.registry_id) != registry:
        raise ValueError("This recipe is bound to another registry.")
    pins = {pin.archive_id: pin for pin in previous.archive_pins} if previous else {}
    existing = set(pins)
    resolved_registry, evidence = await inspect_exact_archives(
        paths, tuple(dict.fromkeys((*pins, *ids)))
    )
    if resolved_registry != registry:
        raise ValueError("The selected registry identity changed.")
    for item in evidence:
        identity = int(str(item["archive_id"]))
        if pin := pins.get(identity):
            if pin.selection_sha256 != item["selection_sha256"]:
                raise ValueError(f"Archive {identity} no longer matches the recipe's revision.")
            if pin.profile_id is not None and (
                pin.profile_id != item["profile_id"] or pin.profile_sha256 != item["profile_sha256"]
            ):
                raise ValueError(
                    f"Archive {identity} has an incompatible or changed profile binding."
                )
        else:
            pins[identity] = ExactArchive.model_validate(
                {
                    key: item[key]
                    for key in ("archive_id", "selection_sha256", "profile_id", "profile_sha256")
                }
            )
    data = {
        **before,
        "registry_id": registry,
        "archive_pins": [pin.model_dump(mode="json", exclude_none=True) for pin in pins.values()],
    }
    additions = [item for item in evidence if int(str(item["archive_id"])) not in existing]
    return data, plain(additions), previous is not None and not additions


async def merge(
    paths: ProjectPaths, rows: Sequence[Row], registry: str, before: dict[str, object]
) -> tuple[dict[str, object], str, bool]:
    if before and not before.get("publication_ids"):
        raise ValueError("Choose a registry-bound Merge recipe with pinned publication evidence.")
    previous = MergeRecipe.model_validate(before) if before else None
    if previous:
        await reviewed_inputs(paths, previous, previous.dataset_ids, registry)
    selected: dict[int, tuple[int, str, str | None]] = {}
    for row in rows:
        value = document(row.value)
        files = value.get("primary_files")
        stream = value.get("stream")
        engine = value.get("engine_publication")
        if not isinstance(files, list) or not files or not isinstance(stream, str) or not stream:
            raise ValueError(
                f"{row.name} has no exact registered stream and primary-file selection."
            )
        for identity in cast(list[int], files):
            scope = (int(row.reference), stream, str(engine) if engine is not None else None)
            if identity in selected and selected[identity] != scope:
                raise ValueError(f"Primary file {identity} has conflicting stream membership.")
            selected[identity] = scope
    datasets = list(
        dict.fromkeys(map(int, (*(previous.dataset_ids if previous else []), *selected)))
    )
    publications = list(
        dict.fromkeys(
            map(
                int,
                [
                    *((previous.publication_ids or []) if previous else []),
                    *(pub for pub, _, _ in selected.values()),
                ],
            )
        )
    )
    recipe, evidence = await publication_recipe(
        paths,
        publications,
        dataset_ids=datasets,
        start_ns=previous.start_ns if previous else None,
        end_ns=previous.end_ns if previous else None,
        stream_ranks=previous.stream_ranks if previous else None,
        timeline_ordering=previous.timeline_ordering if previous else None,
    )
    if str(recipe.registry_id) != registry:
        raise ValueError("The selected registry identity changed.")
    inputs = records(evidence, "inputs")
    for item in inputs:
        identity = int(str(item["database_id"]))
        if identity not in selected:
            continue
        publication, stream, engine = selected[identity]
        if (item["registry_publication_id"], item["stream_id"]) != (publication, stream) or (
            engine is not None and str(item["engine_id"]) != engine
        ):
            raise ValueError(
                f"Primary file {identity} does not match the selected publication and stream."
            )
        if any(
            member.get("stream_id") not in (None, stream) for member in records(item, "memberships")
        ):
            raise ValueError(
                f"Primary file {identity} contains additional streams. "
                "The Merge recipe cannot filter records within a primary file."
            )
    existing: set[int] = set(previous.dataset_ids) if previous else set()
    review = "\n".join(
        f"Publication {item['registry_publication_id']} · Engine {item['engine_id']} · "
        f"File {item['database_id']} · {item['stream_id']}\n"
        f"  {item['registered_path']}"
        for item in inputs
        if int(str(item["database_id"])) not in existing
    )
    duplicate = previous is not None and datasets == previous.dataset_ids
    return {**before, **recipe.model_dump(mode="json", exclude_none=True)}, review, duplicate


async def conversion(
    paths: ProjectPaths, rows: Sequence[Row], registry: str, before: dict[str, object]
) -> tuple[dict[str, object], str, bool]:
    ids = {int(row.reference or row.id) for row in rows}
    if len(ids) != 1:
        raise ValueError(
            "Conversion accepts one committed source publication. Select one Merged row."
        )
    identity = ids.pop()
    source = await inspect_publication_source(paths, identity)
    if source.registry_id != registry:
        raise ValueError("The selected registry identity changed.")
    if any(
        (engine := document(row.value).get("engine_publication")) is not None
        and str(engine) != source.engine_publication_id
        for row in rows
    ):
        raise ValueError("The selected Engine publication identity changed.")
    previous = ConversionRecipe.model_validate(before) if before else None
    if previous and not isinstance(previous.source, PublicationSource):
        raise ValueError("Choose a Conversion recipe with a pinned publication source.")
    if previous and isinstance(previous.source, PublicationSource):
        if previous.source.registry_id != registry:
            raise ValueError("This recipe is bound to another registry.")
        if previous.source == source:
            return before, "", True
        if previous.source.registry_publication_id == identity:
            raise ValueError(
                "The source evidence changed. Review the existing binding in Workflows."
            )
        if previous.interval != source.interval:
            raise ValueError(
                "The selected publication does not match the recipe's existing interval."
            )
    streams = await browse(paths, DatasetRequest("streams", publication_id=identity))
    evidence = plain(
        {"source": source.model_dump(mode="json"), "streams": records(streams, "streams")}
    )
    if previous and isinstance(previous.source, PublicationSource):
        candidate = {**before, "source": source.model_dump(mode="json")}
        ConversionRecipe.model_validate(candidate)
        return (
            candidate,
            (
                f"Replace publication {previous.source.registry_publication_id} with {identity}\n"
                + evidence
            ),
            False,
        )
    return (
        {
            "version": 1,
            "source": source.model_dump(mode="json"),
            "interval": source.interval.model_dump(mode="json"),
            "selection": {"whole_publication": True},
            "conversion": {},
        },
        evidence,
        False,
    )


async def validate_sources(
    paths: ProjectPaths, family: str, candidate: Mapping[str, object], registry: str
) -> None:
    if family == "normalize":
        recipe = ExactRecipe.model_validate(candidate)
        await normalization(
            paths, tuple(pin.archive_id for pin in recipe.archive_pins), registry, dict(candidate)
        )
    elif family == "merge":
        recipe = MergeRecipe.model_validate(candidate)
        await reviewed_inputs(paths, recipe, recipe.dataset_ids, registry)
    elif family == "convert":
        conversion_recipe = ConversionRecipe.model_validate(candidate)
        source = conversion_recipe.source
        if not isinstance(source, PublicationSource) or source != await inspect_publication_source(
            paths, source.registry_publication_id
        ):
            raise ValueError("The committed source evidence changed.")
        reference_id = conversion_recipe.reference_publication_id
        if reference_id is not None:
            found = await discover(paths, conversion_recipe, source)
            if found["blockers"]:
                raise ValueError("\n".join(found["blockers"]))
            if conversion_recipe.selection_policy(conversion_recipe.version).mode == "manual":
                references(
                    found["sources"][0],
                    found["external_reference"],
                    conversion_recipe.reference_stream_ids,
                )
        elif conversion_recipe.reference_stream_ids:
            available = await browse(
                paths,
                DatasetRequest("streams", publication_id=source.registry_publication_id),
            )
            eligible = {
                item["stream_id"]
                for item in records(available, "streams")
                if item.get("kind") in {"trade", "trades"}
                and isinstance(item.get("stream_id"), str)
            }
            if missing := set(conversion_recipe.reference_stream_ids) - eligible:
                raise ValueError(
                    f"Unavailable trade reference streams: {', '.join(sorted(missing))}."
                )


def destination_state(
    family: str, before: Mapping[str, object], seed: Mapping[str, object]
) -> tuple[bool, str]:
    """Describe the configured sources and immediate compatibility blockers."""
    if family == "normalize":
        pins = records(before, "archive_pins")
        if not pins:
            return False, "Requires exact archive pins"
        label = f"{len(pins)} pinned archives"
    elif family == "merge":
        if not before.get("publication_ids"):
            return False, "Requires pinned publications"
        label = f"Publications {before['publication_ids']}"
    else:
        previous = ConversionRecipe.model_validate(before)
        source = previous.source
        if not isinstance(source, PublicationSource):
            return False, "Requires a pinned publication source"
        selected = PublicationSource.model_validate(seed["source"])
        if source.registry_id != selected.registry_id:
            return False, "Recipe belongs to another registry"
        if source == selected:
            return True, f"Already included · publication {source.registry_publication_id}"
        if source.registry_publication_id == selected.registry_publication_id:
            return False, "Source evidence changed"
        if previous.interval != selected.interval:
            return False, "Publication interval differs from the recipe interval"
        return True, f"Replace publication {source.registry_publication_id}"
    if before.get("registry_id") != seed.get("registry_id"):
        return False, f"{label} · recipe belongs to another registry"
    return True, label
