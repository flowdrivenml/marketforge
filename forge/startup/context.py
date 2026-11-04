"""Initialize shared application configuration and presentation."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from rich.console import Console

from forge.shared.config.loader import load_settings
from forge.shared.config.models import AppSettings
from forge.shared.paths import ProjectPaths
from forge.shared.terminal.console import create_console


@dataclass(frozen=True, slots=True)
class ApplicationContext:
    """Configuration and presentation objects shared by application entry points."""

    settings: AppSettings
    paths: ProjectPaths
    settings_file: Path | None
    console: Console
    error_console: Console


def build_context(
    *,
    project_root: Path | None = None,
    config_file: Path | None = None,
) -> ApplicationContext:
    """Resolve the workspace, load settings, and prepare output consoles.

    An explicit configuration path is relative to the caller's working
    directory. Storage paths inside configuration are relative to the workspace.

    Missing default configuration uses application defaults.
    Missing explicit configuration and invalid files remain errors.
    """
    initial_paths = ProjectPaths.resolve(root=project_root)

    selected_file: Path | None

    if config_file is not None:
        selected_file = config_file.expanduser().resolve()
    else:
        default_file = initial_paths.settings_file
        selected_file = default_file if default_file.exists() or default_file.is_symlink() else None

    settings = load_settings(selected_file)

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
    )
