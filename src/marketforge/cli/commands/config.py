from __future__ import annotations

import argparse

from marketforge.config.repository import ConfigRepository
from marketforge.database import connect


def run(
    args: argparse.Namespace,
) -> int:
    """Inspect MarketForge processing configuration."""

    if args.config_command == "profiles":
        return _profiles()

    if args.config_command == "profile":
        return _profile(args.profile_name)

    raise ValueError(f"Unknown config command: {args.config_command}")


def _profiles() -> int:
    with connect() as conn:
        repository = ConfigRepository(conn)

        profiles = repository.list_processing_profiles()

    if not profiles:
        print("No processing profiles found.")
        return 0

    print(
        f"{'ID':<6} "
        f"{'NAME':<24} "
        f"{'WORKERS':<9} "
        f"{'MEMORY':<12} "
        f"{'SCRATCH':<12} "
        f"INTEGRITY"
    )

    print("-" * 80)

    for profile in profiles:
        print(
            f"{profile.id or '-':<6} "
            f"{profile.name:<24} "
            f"{profile.workers:<9} "
            f"{_size(profile.memory_budget_bytes):<12} "
            f"{_size(profile.scratch_budget_bytes):<12} "
            f"{profile.integrity_profile}"
        )

    return 0


def _profile(
    name: str,
) -> int:
    with connect() as conn:
        repository = ConfigRepository(conn)

        profile = repository.get_processing_profile(name)

    if profile is None:
        raise LookupError(f"Processing profile not found: {name}")

    print(f"Profile: {profile.name}")
    print()

    print(f"{'workers':<32}{profile.workers}")
    print(f"{'memory_budget':<32}" f"{_size(profile.memory_budget_bytes)}")
    print(f"{'scratch_path':<32}{profile.scratch_path}")
    print(f"{'scratch_budget':<32}" f"{_size(profile.scratch_budget_bytes)}")
    print(
        f"{'parquet_row_group_target':<32}"
        f"{_size(profile.parquet_row_group_target_bytes)}"
    )
    print(f"{'parquet_file_target':<32}" f"{_size(profile.parquet_file_target_bytes)}")
    print(f"{'integrity_profile':<32}" f"{profile.integrity_profile}")

    return 0


def _size(
    value: int,
) -> str:
    units = (
        ("TiB", 1024**4),
        ("GiB", 1024**3),
        ("MiB", 1024**2),
        ("KiB", 1024),
    )

    for suffix, divisor in units:
        if value >= divisor and value % divisor == 0:
            return f"{value // divisor} {suffix}"

    return f"{value} B"
