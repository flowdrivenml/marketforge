"""Load application settings from TOML."""

from __future__ import annotations

import tomllib
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path

from pydantic import ValidationError

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


@contextmanager
def configuration_errors(path: Path) -> Generator[None]:
    """Report parser and schema failures with the source, excluding submitted values."""
    try:
        yield
    except (tomllib.TOMLDecodeError, UnicodeError):
        raise ConfigurationLoadError(f"Invalid TOML: {path}") from None
    except ValidationError as error:
        details = (
            (".".join(str(part) for part in field["loc"]) or "configuration") + ": " + field["msg"]
            for field in error.errors(include_input=False, include_context=False, include_url=False)
        )
        raise ConfigurationLoadError(f"{path}: " + "; ".join(details)) from None
