from __future__ import annotations

from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class ProcessingProfile(BaseModel):
    """Validated reusable offline-processing configuration."""

    model_config = ConfigDict(frozen=True)

    id: int | None = None

    name: str = Field(
        min_length=1,
    )

    workers: int = Field(
        gt=0,
    )

    memory_budget_bytes: int = Field(
        gt=0,
    )

    scratch_path: Path

    scratch_budget_bytes: int = Field(
        gt=0,
    )

    parquet_row_group_target_bytes: int = Field(
        gt=0,
    )

    parquet_file_target_bytes: int = Field(
        gt=0,
    )

    integrity_profile: str

    @field_validator("name")
    @classmethod
    def validate_name(
        cls,
        value: str,
    ) -> str:
        value = value.strip()

        if not value:
            raise ValueError("Processing profile name cannot be empty")

        return value

    @field_validator("integrity_profile")
    @classmethod
    def validate_integrity_profile(
        cls,
        value: str,
    ) -> str:
        value = value.strip().lower()

        if value not in {
            "standard",
            "strict",
        }:
            raise ValueError("Integrity profile must be " "'standard' or 'strict'")

        return value

    @model_validator(mode="after")
    def validate_parquet_targets(
        self,
    ) -> ProcessingProfile:
        if self.parquet_row_group_target_bytes > self.parquet_file_target_bytes:
            raise ValueError(
                "Parquet row-group target cannot exceed " "Parquet file target"
            )

        return self
