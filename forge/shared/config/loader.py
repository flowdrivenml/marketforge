"""Load application settings from TOML."""

from __future__ import annotations

import tomllib
from pathlib import Path

from forge.shared.config.models import AppSettings
from forge.shared.errors import MarketForgeError


class ConfigurationLoadError(MarketForgeError, ValueError):
    """Raised when a configuration file cannot be read or parsed."""


def load_settings(path: Path | None = None) -> AppSettings:
    """Load an explicit TOML file or return application defaults.

    Missing explicitly selected files are errors.
    Empty TOML documents use the application defaults.
    Field-validation failures retain their Pydantic ValidationError.
    """
    if path is None:
        return AppSettings()

    source = path.expanduser().resolve()

    try:
        with source.open("rb") as stream:
            payload: object = tomllib.load(stream)
    except tomllib.TOMLDecodeError as exc:
        raise ConfigurationLoadError(f"Invalid TOML in configuration file: {source}") from exc
    except (OSError, UnicodeError) as exc:
        raise ConfigurationLoadError(f"Cannot read configuration file: {source}") from exc

    return AppSettings.model_validate(payload)
