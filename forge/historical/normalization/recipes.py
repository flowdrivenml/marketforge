"""Acquisition-shaped recipe loading and editable copies; legacy requests stay readable."""

import tomllib
from pathlib import Path

from forge.historical.archives.config import AcquisitionConfig, load_acquisition
from forge.historical.archives.templates import ACQUISITION_TEMPLATE
from forge.historical.normalization.config import NormalizationRecipe, PreparationRequest
from forge.shared.config.library import resolve_config_file
from forge.shared.paths import ProjectPaths


def recipe_path(paths: ProjectPaths, name: str, *, existing: bool = True) -> Path:
    destination = resolve_config_file(paths.normalization_recipe_dir, name)
    legacy = resolve_config_file(paths.normalization_config_dir, name)
    return legacy if existing and not destination.exists() and legacy.exists() else destination


def load_configuration(source: Path) -> AcquisitionConfig | PreparationRequest:
    with source.open("rb") as stream:
        document = tomllib.load(stream)
    if "inputs" in document or "job_id" in document:
        return PreparationRequest.model_validate(document)
    return NormalizationRecipe.model_validate(document)


def create_recipe(paths: ProjectPaths, name: str, acquisition: str | None) -> Path:
    destination = recipe_path(paths, name, existing=False)
    # A legacy request with this name must not be silently shadowed.
    legacy = resolve_config_file(paths.normalization_config_dir, name)
    if legacy.exists():
        raise FileExistsError(f"Configuration already exists: {legacy}")
    if acquisition is None:
        text = (
            "# EDIT the interval, instruments and feeds before preparation.\n"
            + ACQUISITION_TEMPLATE
        )
    else:
        source = resolve_config_file(paths.acquisition_config_dir, acquisition)
        load_acquisition(source)
        text = source.read_text(encoding="utf-8")
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("x", encoding="utf-8") as stream:
        stream.write(text)
    return destination
