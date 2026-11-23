"""Inspect live references in managed historical workflow recipes."""

import tomllib
from pathlib import Path

from pydantic import ValidationError

from forge.historical.conversion.recipes import Recipe as ConversionRecipe
from forge.historical.conversion.recipes import Source, source_path
from forge.historical.merge.config import DerivedRecipe, normalization_path
from forge.shared.config.deletion import RecipeDependencies, RecipeFamily
from forge.shared.config.library import list_config_names, resolve_config_file
from forge.shared.errors import MarketForgeError
from forge.shared.paths import ProjectPaths


def inspect_references(
    paths: ProjectPaths, family: RecipeFamily, target: Path
) -> RecipeDependencies:
    if family == "conversion":
        return RecipeDependencies()
    referenced: list[Path] = []
    unreadable: list[Path] = []
    libraries = ("conversion", "merge") if family == "normalization" else ("conversion",)
    for library in libraries:
        directory = paths.workflow_dir(library)
        for name in list_config_names(directory):
            path = resolve_config_file(directory, name)
            try:
                with path.open("rb") as stream:
                    document = tomllib.load(stream)
                if library == "merge":
                    if "selection" not in document:
                        continue
                    merge = DerivedRecipe.model_validate(document)
                    source = normalization_path(paths, merge.selection.source)
                else:
                    conversion = ConversionRecipe.model_validate(document)
                    binding = conversion.source
                    kind = "normalize" if family == "normalization" else family
                    if not isinstance(binding, Source) or binding.kind != kind:
                        continue
                    source = source_path(paths, binding.kind, binding.reference)
                if source == target:
                    referenced.append(path)
            except (
                MarketForgeError,
                ValidationError,
                tomllib.TOMLDecodeError,
                OSError,
                UnicodeError,
            ):
                unreadable.append(path)
    return RecipeDependencies(tuple(referenced), tuple(unreadable))
