"""Read-only filesystem discovery for MarketForge Parquet datasets."""

from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path

from .dataset import Dataset, Stream
from .parquet import batches


def _identity(
    files: tuple[Path, ...],
) -> dict[str, object] | None:
    """Read a provisional stream identity from the first record."""

    import pyarrow.parquet as pq

    columns = (
        "exchange",
        "symbol",
        "stream_id",
        "instrument_id",
    )

    fields = pq.ParquetFile(files[0]).schema_arrow.names

    available = [name for name in columns if name in fields]

    if not available:
        return None

    iterator = batches(
        files[:1],
        columns=available,
        batch_size=1,
    )

    try:
        batch = next(iterator)
    except StopIteration:
        return None

    if batch.num_rows == 0:
        return None

    return {key: batch.column(index)[0].as_py() for index, key in enumerate(available)}


def open_dataset(
    path: str | Path,
    *,
    allow_staging: bool = False,
) -> Dataset:
    source = Path(path).expanduser().resolve()

    if not source.exists():
        raise FileNotFoundError(source)

    if source.is_file() and source.suffix != ".parquet":
        raise ValueError("Expected .parquet or a directory containing Parquet files.")

    is_staging = "staging" in source.parts

    if is_staging and not allow_staging:
        raise ValueError(
            "Staging output is uncommitted. " "Pass allow_staging=True explicitly."
        )

    paths = [source] if source.is_file() else sorted(source.rglob("*.parquet"))

    if not paths:
        raise ValueError(f"No Parquet files found in {source}")

    groups: dict[
        tuple[str, str],
        list[Path],
    ] = defaultdict(list)

    for file in paths:
        parts = file.parts

        stream_dirs = [index for index, part in enumerate(parts) if part == "streams"]

        if stream_dirs and stream_dirs[-1] + 2 < len(parts):
            index = stream_dirs[-1]

            fingerprint = parts[index + 1]
            kind = parts[index + 2]

            key = (fingerprint, kind)

        else:
            key = (str(file.parent), "unknown")

        groups[key].append(file)

    streams = tuple(
        Stream(
            files=tuple(group),
            fingerprint=fingerprint,
            kind=kind,
            dataset_path=source,
            identity=_identity(tuple(group)),
        )
        for (fingerprint, kind), group in sorted(groups.items())
    )

    reason = None

    failure = (
        source / "failure.json" if source.is_dir() else source.parent / "failure.json"
    )

    if failure.is_file():
        try:
            report = json.loads(failure.read_text())

            reason = str(
                report.get(
                    "error_message",
                    "Failure report present",
                )
            )

        except (OSError, ValueError):
            reason = "Failure report unreadable"

    status = "staging" if is_staging else "unverified"

    return Dataset(
        path=source,
        streams_by_key=streams,
        status=status,
        reason=reason,
    )
