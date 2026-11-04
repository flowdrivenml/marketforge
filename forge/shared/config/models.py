"""Validated application settings."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class SettingsModel(BaseModel, frozen=True):
    """Reject unknown settings and unintended type conversions."""

    model_config = ConfigDict(
        extra="forbid",
        strict=True,
        validate_default=True,
    )


class StorageSettings(SettingsModel, frozen=True):
    """Storage settings resolved relative to the workspace."""

    data_root: str = Field(default="data", min_length=1)

    @field_validator("data_root")
    @classmethod
    def validate_data_root(cls, value: str) -> str:
        """Reject blank paths without modifying valid path names."""
        if not value.strip():
            raise ValueError("data_root must not be blank")

        return value


class PresentationSettings(SettingsModel, frozen=True):
    """Default execution presentation."""

    mode: Literal["progress", "silent"] = "progress"
    color: Literal["auto", "always", "never"] = "auto"
    editor: str | None = Field(
        default=None,
        min_length=1,
        pattern=r"\S",
    )


class HttpSettings(SettingsModel, frozen=True):
    """Default HTTP behavior for each upstream service."""

    requests_per_second: float = Field(
        default=2.0,
        gt=0,
        allow_inf_nan=False,
    )
    max_concurrent: int = Field(default=4, ge=1, le=64)
    timeout_seconds: float = Field(
        default=30.0,
        gt=0,
        allow_inf_nan=False,
    )
    max_attempts: int = Field(default=3, ge=1, le=10)


class AppSettings(SettingsModel, frozen=True):
    """Versioned application settings."""

    version: int = Field(default=1, ge=1, le=1)
    storage: StorageSettings = Field(default_factory=StorageSettings)
    presentation: PresentationSettings = Field(
        default_factory=PresentationSettings,
    )
    http: HttpSettings = Field(default_factory=HttpSettings)
