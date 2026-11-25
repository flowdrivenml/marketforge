"""Managed recipe drafts and schema-backed conversion options."""

import tomllib
from pathlib import Path
from typing import cast

from forge.historical.archives.storage.files import disk
from forge.historical.authoring import publish_new
from forge.historical.normalization.config import ExactRecipe
from forge.shared.config.recipe_editing import update_recipe
from forge.shared.config.toml import dumps
from forge.shared.errors import MarketForgeError
from forge.shared.paths import ProjectPaths


async def save_normalization(path: Path, recipe: ExactRecipe) -> Path:
    return await disk(
        publish_new,
        path,
        dumps(recipe.model_dump(exclude_none=True, exclude_unset=True)),
    )


def validate_recipe_draft(paths: ProjectPaths, family: str, source: Path) -> None:
    """Validate an editor's private draft against the existing recipe schema."""
    import tomllib

    from forge.historical.archives.config import load_acquisition
    from forge.historical.conversion.recipes import load_recipe as load_conversion
    from forge.historical.imports.models import load_import
    from forge.historical.merge.service import load_recipe as load_merge
    from forge.historical.merge.service import validate_recipe as validate_merge
    from forge.historical.normalization.recipes import load_configuration
    from forge.historical.pipelines.config import load_recipe_file
    from forge.historical.profiles.service import validate as validate_profile
    from forge.historical.registration.models import load as load_registration

    match family:
        case "acquisition":
            load_acquisition(source)
        case "pipeline":
            load_recipe_file(source, source.read_bytes())
        case "normalize":
            load_configuration(source, retain=False)
        case "merge":
            validate_merge(paths, load_merge(source, retain=False))
        case "convert":
            load_conversion(paths, source, retain=False)
        case "markets rank":
            from forge.markets.ranking.policy import load_policy

            load_policy(source)
        case "Profile":
            validate_profile(paths, source)
        case "acquisition import":
            load_import(source)
        case "acquisition register":
            load_registration(source)
        case _:
            with source.open("rb") as stream:
                tomllib.load(stream)


def conversion_candidate(
    original: bytes,
    policy: dict[str, object],
    resources: dict[str, object],
    reference_stream_ids: list[str],
    reference_publication_id: int | None,
    reference_selection: dict[str, object] | None = None,
) -> tuple[str, dict[str, object]]:
    """Round-trip a conversion draft through Forge's authoritative recipe model."""
    from forge.historical.conversion.recipes import Recipe

    before = tomllib.loads(original.decode())
    candidate: dict[str, object] = dict(before)
    for section, updates in (("conversion", policy), ("resources", resources)):
        if not updates:
            continue
        table = before.get(section, {})
        if not isinstance(table, dict):
            raise MarketForgeError(f"Expected a TOML table for {section}.")
        candidate[section] = {**cast(dict[str, object], table), **updates}
    if reference_selection is not None:
        candidate["reference_selection"] = reference_selection
    if reference_selection and reference_selection["mode"] == "automatic":
        candidate.pop("reference_stream_ids", None)
    elif reference_stream_ids or "reference_stream_ids" in before:
        candidate["reference_stream_ids"] = reference_stream_ids
    if reference_publication_id is None:
        candidate.pop("reference_publication_id", None)
    else:
        candidate["reference_publication_id"] = reference_publication_id
    updated = update_recipe(original, before, candidate)
    validated = Recipe.model_validate(tomllib.loads(updated))
    return updated, {
        "conversion": validated.conversion.model_dump(),
        "resources": validated.resources.model_dump(),
        "reference_selection": validated.selection_policy(validated.version).model_dump(
            mode="json", exclude_none=True
        ),
        "reference_stream_ids": validated.reference_stream_ids,
        "reference_publication_id": validated.reference_publication_id,
    }
