"""Inspect live references in managed historical workflow recipes."""

import json
import tomllib
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from forge.historical.conversion.recipes import Recipe as ConversionRecipe
from forge.historical.conversion.recipes import Source, source_path
from forge.historical.merge.config import DerivedRecipe, normalization_path
from forge.shared.config.deletion import RecipeDependencies, RecipeFamily, recipe_directory
from forge.shared.config.library import list_config_names, resolve_config_file
from forge.shared.errors import MarketForgeError
from forge.shared.paths import ProjectPaths


def inspect_references(
    paths: ProjectPaths, family: RecipeFamily, target: Path
) -> RecipeDependencies:
    executions = execution_references(paths, family, target)
    referenced: list[Path] = []
    unreadable: list[Path] = []
    libraries = {
        "normalization": ("conversion", "merge"),
        "acquisition": ("conversion",),
        "merge": ("conversion",),
    }.get(family, ())
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
    return RecipeDependencies(tuple(referenced), tuple(unreadable), executions)


def read_evidence(path: Path) -> dict[str, Any]:
    with path.open("rb") as stream:
        return json.loads(stream.read(4 * 1024 * 1024 + 1))


def execution_references(
    paths: ProjectPaths, family: RecipeFamily, target: Path
) -> tuple[Path, ...]:
    """Check retained attempt intents across both registry environments in this storage."""
    operation = {
        "normalization": "normalize",
        "conversion": "convert",
        "registration": "acquisition",
        "import": "acquisition",
    }.get(family, family)
    dependencies: list[Path] = []
    for intent in paths.runs_dir.glob("*/attempts/*/intent.json"):
        try:
            record = read_evidence(intent)
            reference = record.get("recipe_source")
            if record.get("operation") != operation or not isinstance(reference, str):
                continue
            if operation == "acquisition":
                action_family = {"import": "import", "register": "registration"}.get(
                    str(record.get("action")), "acquisition"
                )
                if action_family != family:
                    continue
            source = Path(reference)
            if not source.is_absolute() and len(source.parts) == 1:
                source = resolve_config_file(recipe_directory(paths, family), reference)
            if source.resolve() != target:
                continue
            attempt = intent.parent
            run = attempt.parents[1]
            # Original bytes were already retained by these producers before the lifecycle change.
            if (
                (attempt / "recipe.toml").is_file()
                or ((run / "submitted.toml").is_file() and (run / "report.json").is_file())
                or (
                    (attempt / "acquisition/submitted.toml").is_file()
                    and (attempt / "acquisition/report.json").is_file()
                )
            ):
                continue
            if (attempt / "recipe-source.json").is_file():
                metadata = read_evidence(attempt / "recipe-source.json")
                snapshot = Path(metadata.get("snapshot", ""))
                if snapshot.is_relative_to(run) and snapshot.is_file():
                    continue
            if (attempt / "outcome.json").is_file():
                outcome = read_evidence(attempt / "outcome.json")
                report = Path(outcome.get("report_path") or "")
                if report.is_relative_to(paths.runs_dir) and (
                    report.with_name("registration.toml").is_file()
                    or report.with_name("recipe.toml").is_file()
                ):
                    continue
            dependencies.append(intent)
        except (OSError, ValueError, TypeError, MarketForgeError, AttributeError):
            continue
    return tuple(sorted(dependencies))
