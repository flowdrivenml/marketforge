from __future__ import annotations

from pathlib import Path

from marketforge.config.models import ProcessingProfile

DEFAULT_PROCESSING_PROFILES = (
    ProcessingProfile(
        name="low-memory",
        workers=2,
        memory_budget_bytes=2 * 1024**3,
        scratch_path=Path("data/.work"),
        scratch_budget_bytes=20 * 1024**3,
        parquet_row_group_target_bytes=64 * 1024**2,
        parquet_file_target_bytes=256 * 1024**2,
        integrity_profile="standard",
    ),
    ProcessingProfile(
        name="default",
        workers=4,
        memory_budget_bytes=4 * 1024**3,
        scratch_path=Path("data/.work"),
        scratch_budget_bytes=20 * 1024**3,
        parquet_row_group_target_bytes=128 * 1024**2,
        parquet_file_target_bytes=512 * 1024**2,
        integrity_profile="standard",
    ),
    ProcessingProfile(
        name="laptop",
        workers=4,
        memory_budget_bytes=8 * 1024**3,
        scratch_path=Path("data/.work"),
        scratch_budget_bytes=50 * 1024**3,
        parquet_row_group_target_bytes=128 * 1024**2,
        parquet_file_target_bytes=512 * 1024**2,
        integrity_profile="standard",
    ),
    ProcessingProfile(
        name="workstation",
        workers=12,
        memory_budget_bytes=32 * 1024**3,
        scratch_path=Path("data/.work"),
        scratch_budget_bytes=250 * 1024**3,
        parquet_row_group_target_bytes=256 * 1024**2,
        parquet_file_target_bytes=1024 * 1024**2,
        integrity_profile="standard",
    ),
    ProcessingProfile(
        name="high-end-workstation",
        workers=24,
        memory_budget_bytes=80 * 1024**3,
        scratch_path=Path("data/.work"),
        scratch_budget_bytes=750 * 1024**3,
        parquet_row_group_target_bytes=256 * 1024**2,
        parquet_file_target_bytes=2 * 1024**3,
        integrity_profile="strict",
    ),
)
