from __future__ import annotations

import re
from pathlib import Path

from marketforge.models import RemoteFile

DEFAULT_DATA_ROOT = Path("data")


def raw_file_path(
    remote_file: RemoteFile,
    *,
    data_root: Path = DEFAULT_DATA_ROOT,
) -> Path:
    """Return the local path for an immutable raw archive."""

    return (
        data_root
        / "raw"
        / remote_file.exchange.value
        / remote_file.instrument_type.value
        / remote_file.market_category.value
        / remote_file.data_type.value
        / safe_path_component(remote_file.symbol)
        / safe_filename(remote_file.filename)
    )


def safe_path_component(
    value: str,
) -> str:
    """Convert an identifier into one safe filesystem component."""

    value = value.strip()

    if not value:
        raise ValueError("Path component cannot be empty")

    value = re.sub(
        r"[/\\]+",
        "_",
        value,
    )

    if value in {
        ".",
        "..",
    }:
        raise ValueError(f"Unsafe path component: {value!r}")

    return value


def safe_filename(
    filename: str,
) -> str:
    """Validate and normalize a remote archive filename."""

    filename = filename.strip()

    if not filename:
        raise ValueError("Filename cannot be empty")

    # Reject path-navigation names before Path() normalizes them.
    if filename in {
        ".",
        "..",
    }:
        raise ValueError(f"Unsafe filename: {filename!r}")

    # Remote filenames must never control local directories.
    filename = Path(filename).name

    if not filename:
        raise ValueError("Filename cannot be empty")

    if filename in {
        ".",
        "..",
    }:
        raise ValueError(f"Unsafe filename: {filename!r}")

    return filename
