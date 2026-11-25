"""Inspect live references in managed historical workflow recipes."""

import hashlib
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
        except (OSError, ValueError, TypeError, MarketForgeError, AttributeError):
            continue
        try:
            if retained_original(paths.runs_dir, run, attempt):
                continue
        except (OSError, ValueError, TypeError, AttributeError):
            pass
        dependencies.append(intent)
    return tuple(sorted(dependencies))


def retained_original(root: Path, run: Path, attempt: Path) -> bool:
    """Recognize current manifests and the original files recorded by legacy producers."""
    declaration = attempt / "recipe-source.json"
    if declaration.is_file():
        metadata = read_evidence(declaration)
        snapshot = Path(metadata.get("snapshot", attempt / "recipe.toml"))
        if snapshot.is_symlink() or not snapshot.resolve().is_relative_to(run.resolve()):
            return False
        with snapshot.open("rb") as stream:
            return hashlib.file_digest(stream, "sha256").hexdigest() == metadata.get("sha256")
    for directory in (attempt / "acquisition", run):
        snapshot = directory / "submitted.toml"
        report = directory / "report.json"
        if not snapshot.is_file() or snapshot.is_symlink() or not report.is_file():
            continue
        document = read_evidence(report)
        expected = document.get("recipe_sha256") or document.get("submitted_sha256")
        with snapshot.open("rb") as stream:
            if expected == hashlib.file_digest(stream, "sha256").hexdigest():
                return True
        if expected is None and document.get("action") == "import":
            return (directory / "resolved-plan.json").is_file()
    if (attempt / "outcome.json").is_file():
        outcome = read_evidence(attempt / "outcome.json")
        report = Path(outcome.get("report_path") or "")
        if not report.resolve().is_relative_to(root.resolve()):
            return False
        declaration = read_evidence(report)
        snapshot = report.with_name("registration.toml")
        if snapshot.is_file() and not snapshot.is_symlink():
            with snapshot.open("rb") as stream:
                return hashlib.file_digest(stream, "sha256").hexdigest() == declaration.get(
                    "recipe_sha256"
                )
    return False
