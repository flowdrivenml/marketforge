"""Resolve names inside managed configuration libraries."""

from __future__ import annotations

import re
from pathlib import Path

from forge.shared.errors import MarketForgeError


class ConfigurationNameError(MarketForgeError, ValueError):
    """Raised when a configuration name or location is invalid."""


def resolve_config_file(directory: Path, name: str) -> Path:
    """Resolve a configuration name without allowing library escape."""
    stem = name.removesuffix(".toml")

    if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]*", stem) is None:
        raise ConfigurationNameError(
            "Use a configuration name containing letters, digits, "
            "hyphens, or underscores, optionally followed by .toml. "
            "Directory paths are not accepted."
        )

    root = directory.resolve()
    destination = (root / f"{stem}.toml").resolve()

    if destination.parent != root:
        raise ConfigurationNameError(f"Configuration {name!r} points outside its managed library.")

    return destination


def list_config_names(directory: Path) -> tuple[str, ...]:
    """List supported file names without parsing configuration contents."""
    root = directory.resolve()

    if not root.exists():
        return ()

    if not root.is_dir():
        raise ConfigurationNameError(f"Configuration library is not a directory: {root}")

    names: list[str] = []

    for entry in root.glob("*.toml"):
        try:
            resolved = resolve_config_file(root, entry.name)
        except ConfigurationNameError:
            continue

        if resolved.is_file():
            names.append(entry.stem)

    return tuple(sorted(names))
