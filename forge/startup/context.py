"""Initialize shared application configuration and presentation."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from rich.console import Console

from forge.shared.config.loader import configuration_errors, load_settings
from forge.shared.config.models import AppSettings
from forge.shared.paths import ProjectPaths, require_migrated
from forge.shared.terminal.console import create_console


@dataclass(frozen=True, slots=True)
class ApplicationContext:
    """Configuration and presentation objects shared by application entry points."""

    settings: AppSettings
    paths: ProjectPaths
    settings_file: Path | None
    console: Console
    error_console: Console
    testing: bool = False

    def configuration_report(self) -> dict[str, Any]:
        """Serialize public application configuration with per-setting provenance."""
        settings_file = str(self.settings_file) if self.settings_file is not None else None

        # Select public fields explicitly so future secrets are not exposed.
        payload = {
            "schema_version": 1,
            "project_root": str(self.paths.root),
            "settings_file": settings_file,
            "settings": {
                "version": self.settings.version,
                "storage": {
                    "data_root": self.settings.storage.data_root,
                    "resolved_data_root": str(self.paths.data_root),
                },
                "presentation": {
                    "mode": self.settings.presentation.mode,
                    "color": self.settings.presentation.color,
                    "editor": self.settings.presentation.editor,
                },
            },
            "http": self.settings.http.model_dump(mode="json"),
        }

        settings = self.settings
        sections = {
            "storage": settings.storage,
            "presentation": settings.presentation,
            "http": settings.http,
        }
        sources = {"version": settings_file if "version" in settings.model_fields_set else None}
        for section, model in sections.items():
            sources.update(
                {
                    f"{section}.{name}": settings_file if name in model.model_fields_set else None
                    for name in type(model).model_fields
                }
            )
        payload["setting_sources"] = sources
        return payload


def build_context(
    *,
    project_root: Path | None = None,
    config_file: Path | None = None,
    testing: bool = False,
) -> ApplicationContext:
    """Resolve the workspace, load settings, and prepare output consoles.

    An explicit configuration path is relative to the caller's working
    directory. Storage paths inside configuration are relative to the workspace.

    Missing default configuration uses application defaults.
    Missing explicit configuration and invalid files remain errors.
    """
    initial_paths = ProjectPaths.resolve(root=project_root)

    settings, selected_file = load_workspace_settings(initial_paths, config_file)
    paths = ProjectPaths.resolve(
        root=initial_paths.root,
        data_root=Path(settings.storage.data_root),
    )

    return ApplicationContext(
        settings=settings,
        paths=paths,
        settings_file=selected_file,
        console=create_console(settings.presentation),
        error_console=create_console(settings.presentation, stderr=True),
        testing=testing,
    )


def load_workspace_settings(
    paths: ProjectPaths, config_file: Path | None
) -> tuple[AppSettings, Path | None]:
    """Load the explicit file or the workspace default using the shared schema."""
    selected_file: Path | None

    if config_file is not None:
        selected_file = config_file.expanduser().resolve()
    else:
        default_file = require_migrated(paths.settings_file, paths.config_dir / "marketforge.toml")
        selected_file = default_file if default_file.exists() or default_file.is_symlink() else None

    if selected_file is None:
        settings = load_settings()
    else:
        with configuration_errors(selected_file):
            settings = load_settings(selected_file)

    return settings, selected_file
