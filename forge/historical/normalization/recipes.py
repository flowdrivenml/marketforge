"""Acquisition-shaped recipe loading and editable copies; legacy requests stay readable."""

import tomllib
from pathlib import Path

from forge.historical.archives.config import load_acquisition
from forge.historical.archives.templates import ACQUISITION_TEMPLATE
from forge.historical.normalization.config import (
    ExactRecipe,
    NormalizationRecipe,
    PreparationRequest,
)
from forge.orchestration.registry import retain_recipe
from forge.shared.config.library import resolve_config_file
from forge.shared.config.loader import configuration_errors
from forge.shared.identity import RunOperation
from forge.shared.paths import ProjectPaths


def recipe_path(paths: ProjectPaths, name: str) -> Path:
    supplied = Path(name).expanduser()
    if supplied.is_absolute() or "/" in name:
        return supplied.resolve()
    return resolve_config_file(paths.normalization_recipe_dir, name)


Recipe = NormalizationRecipe | PreparationRequest | ExactRecipe


def load_configuration(source: Path, *, retain: bool = True) -> Recipe:
    with configuration_errors(source):
        data = source.read_bytes()
        document = tomllib.loads(data.decode())
        if "archive_pins" in document:
            recipe = ExactRecipe.model_validate(document)
        elif "inputs" in document or "job_id" in document:
            recipe = PreparationRequest.model_validate(document)
        else:
            recipe = NormalizationRecipe.model_validate(document)
    if retain:
        retain_recipe(RunOperation.NORMALIZE, source, data)
    return recipe


def create_recipe(paths: ProjectPaths, name: str, acquisition: str | None) -> Path:
    destination = resolve_config_file(paths.normalization_recipe_dir, name)
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
