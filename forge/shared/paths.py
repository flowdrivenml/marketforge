"""Resolve MarketForge workspace and runtime paths."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Self

from forge.shared.errors import MarketForgeError

PROJECT_ROOT_ENV = "MARKETFORGE_PROJECT_ROOT"


class PathConfigurationError(MarketForgeError, ValueError):
    """Raised when the configured workspace cannot be resolved."""


def _is_project_root(path: Path) -> bool:
    return (path / "forge" / "pyproject.toml").is_file() and (path / "engine").is_dir()


def resolve_project_root(explicit_root: Path | None = None) -> Path:
    """Resolve an explicit root, environment setting, or enclosing workspace."""
    configured_root = explicit_root

    if configured_root is None:
        environment_root = os.environ.get(PROJECT_ROOT_ENV)
        if environment_root is not None:
            if not environment_root.strip():
                raise PathConfigurationError(f"{PROJECT_ROOT_ENV} must not be empty.")
            configured_root = Path(environment_root)
    if configured_root is not None:
        root = configured_root.expanduser().resolve()
        if not _is_project_root(root):
            raise PathConfigurationError(
                f"Invalid MarketForge project root: {root}. "
                "Expected forge/pyproject.toml and an engine directory."
            )
        return root

    current = Path.cwd().resolve()

    for candidate in (current, *current.parents):
        if _is_project_root(candidate):
            return candidate

    raise PathConfigurationError(
        "Cannot locate the MarketForge workspace. "
        f"Provide a project root or set {PROJECT_ROOT_ENV}."
    )


@dataclass(frozen=True, slots=True)
class ProjectPaths:
    """Resolved workspace paths; constructing these creates no directories."""

    root: Path
    data_root: Path

    @classmethod
    def resolve(
        cls,
        *,
        root: Path | None = None,
        data_root: Path | None = None,
    ) -> Self:
        resolved_root = resolve_project_root(root)
        selected_data_root = Path("data") if data_root is None else data_root.expanduser()

        if not selected_data_root.is_absolute():
            selected_data_root = resolved_root / selected_data_root

        return cls(
            root=resolved_root,
            data_root=selected_data_root.resolve(),
        )

    @property
    def bin_dir(self) -> Path:
        return self.root / ".bin"

    @property
    def config_dir(self) -> Path:
        return self.root / "data" / ".config"

    @property
    def settings_file(self) -> Path:
        return self.config_dir / "marketforge.toml"

    @property
    def pipeline_config_dir(self) -> Path:
        return self.config_dir / "pipeline"

    @property
    def raw_dir(self) -> Path:
        return self.data_root / "raw"

    @property
    def datasets_dir(self) -> Path:
        return self.data_root / "datasets"

    @property
    def runs_dir(self) -> Path:
        return self.data_root / "runs"

    @property
    def acquisition_config_dir(self) -> Path:
        return self.config_dir / "acquisition"
