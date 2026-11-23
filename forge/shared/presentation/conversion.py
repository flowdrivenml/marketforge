"""Structured views of conversion recipes and operation evidence."""

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from forge.historical.conversion import recipes
from forge.historical.conversion.recipes import Recipe
from forge.shared.config.loader import ConfigurationLoadError
from forge.shared.errors import MarketForgeError
from forge.shared.paths import ProjectPaths


@dataclass(frozen=True)
class RecipeView:
    path: Path
    recipe: Recipe | None
    state: str
    diagnostic: str = ""

    @property
    def publication(self) -> recipes.PublicationSource | None:
        source = self.recipe.source if self.recipe else None
        return source if isinstance(source, recipes.PublicationSource) else None

    @property
    def source_label(self) -> str:
        if self.publication:
            source = self.publication
            return (
                f"Merged publication · Registry {source.registry_publication_id} · "
                f"Engine {source.engine_publication_id} · {source.environment}"
            )
        if self.recipe and isinstance(self.recipe.source, recipes.Source):
            return f"{self.recipe.source.kind} · {self.recipe.source.reference}"
        return "—"

    def document(self) -> dict[str, Any]:
        return {
            "name": self.path.stem,
            "path": str(self.path),
            "status": self.state,
            "diagnostic": self.diagnostic or None,
            "recipe": self.recipe.model_dump(mode="json", exclude_none=True)
            if self.recipe
            else None,
        }


@dataclass(frozen=True)
class ConversionView:
    action: str
    reference: str
    status: str
    environment: str
    elapsed: float
    result: dict[str, Any]
    jobs: tuple[dict[str, Any], ...]
    diagnostics: tuple[str, ...]

    @classmethod
    def from_result(
        cls, result: dict[str, Any], action: str, reference: str, elapsed: float
    ) -> "ConversionView":
        jobs = tuple(result.get("jobs", ()))
        status = result["status"]
        if status == "generated":
            if result.get("dry_run"):
                status = "planned"
            elif result.get("compatibility") == "validated":
                states = result.get("reuse_plan", {}).values()
                status = (
                    "validated · work pending"
                    if any(
                        item["status"] not in {"would_execute", "would_reuse"} for item in states
                    )
                    else "ready to run"
                )
            else:
                status = "blocked"
        return cls(
            action,
            reference,
            status,
            result.get("environment", ""),
            elapsed,
            result,
            jobs,
            tuple(
                dict.fromkeys(
                    [
                        *result.get("blockers", ()),
                        *result.get("compatibility_blockers", ()),
                        *([result["error"]] if result.get("error") else []),
                    ]
                )
            ),
        )


def inspect_recipe(paths: ProjectPaths, source: Path) -> RecipeView:
    try:
        recipe, _ = recipes.read_recipe(source)
    except (ConfigurationLoadError, MarketForgeError, OSError) as error:
        return RecipeView(source, None, "invalid", str(error))
    try:
        recipes.validate_binding(paths, source, recipe)
    except (ConfigurationLoadError, MarketForgeError, OSError) as error:
        return RecipeView(source, recipe, "stale", str(error))
    if isinstance(recipe.source, recipes.PublicationSource):
        return RecipeView(
            source,
            recipe,
            "valid",
            "Local declaration valid. Current registry eligibility is checked during preparation.",
        )
    return RecipeView(source, recipe, "valid")
