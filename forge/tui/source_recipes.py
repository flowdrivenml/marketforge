"""Managed recipe drafts and schema-backed conversion options."""

from pathlib import Path

from forge.historical.archives.storage.files import disk
from forge.historical.authoring import publish_new
from forge.historical.normalization.config import ExactRecipe
from forge.shared.config.toml import dumps
from forge.shared.errors import MarketForgeError
from forge.shared.paths import ProjectPaths


async def save_normalization(path: Path, recipe: ExactRecipe) -> Path:
    return await disk(
        publish_new,
        path,
        dumps(recipe.model_dump(mode="json", exclude_none=True, exclude_unset=True)),
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
) -> tuple[str, dict[str, object]]:
    """Round-trip a conversion draft through Forge's authoritative recipe model."""
    import tomlkit
    from tomlkit.container import OutOfOrderTableProxy
    from tomlkit.items import InlineTable, Table

    from forge.historical.conversion.recipes import Recipe

    document = tomlkit.parse(original.decode())
    for section, updates in (("conversion", policy), ("resources", resources)):
        if not updates:
            continue
        if section not in document:
            document[section] = tomlkit.table()
        table = document[section]
        if not isinstance(table, (Table, InlineTable, OutOfOrderTableProxy)):
            raise MarketForgeError(f"Expected a TOML table for {section}.")
        for key, value in updates.items():
            table[key] = value
    if reference_stream_ids or "reference_stream_ids" in document:
        document["reference_stream_ids"] = reference_stream_ids
    if reference_publication_id is None:
        if "reference_publication_id" in document:
            del document["reference_publication_id"]
    else:
        document["reference_publication_id"] = reference_publication_id
    validated = Recipe.model_validate(document.unwrap())
    return document.as_string(), {
        "conversion": validated.conversion.model_dump(),
        "resources": validated.resources.model_dump(),
        "reference_stream_ids": validated.reference_stream_ids,
        "reference_publication_id": validated.reference_publication_id,
    }
