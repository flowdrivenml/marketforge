from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path

from marketforge.models import (
    DataType,
    Exchange,
    InstrumentType,
    MarketCategory,
    RawFileRecord,
    RemoteFile,
)

MANIFEST_SUFFIX = ".manifest.json"


def manifest_path(
    raw_path: Path,
) -> Path:
    """Return the sidecar manifest path for a raw archive."""

    return raw_path.with_name(raw_path.name + MANIFEST_SUFFIX)


def write_raw_record(
    record: RawFileRecord,
) -> Path:
    """Atomically persist a completed raw-file record."""

    path = manifest_path(record.path)

    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temporary = path.with_name(path.name + ".tmp")

    payload = _record_to_dict(record)

    try:
        with temporary.open(
            "w",
            encoding="utf-8",
        ) as output:
            json.dump(
                payload,
                output,
                indent=2,
                sort_keys=True,
            )

            output.write("\n")

            output.flush()

            os.fsync(output.fileno())

        os.replace(
            temporary,
            path,
        )

    except Exception:
        temporary.unlink(missing_ok=True)
        raise

    return path


def read_raw_record(
    raw_path: Path,
) -> RawFileRecord | None:
    """Read the completed record for a raw archive."""

    path = manifest_path(raw_path)

    if not path.exists():
        return None

    with path.open(
        "r",
        encoding="utf-8",
    ) as source:
        payload = json.load(source)

    return _record_from_dict(payload)


def _record_to_dict(
    record: RawFileRecord,
) -> dict:
    remote = record.remote_file

    return {
        "version": 1,
        "remote_file": {
            "exchange": remote.exchange.value,
            "instrument_type": remote.instrument_type.value,
            "market_category": remote.market_category.value,
            "data_type": remote.data_type.value,
            "symbol": remote.symbol,
            "start": remote.start.isoformat(),
            "end": remote.end.isoformat(),
            "url": remote.url,
            "filename": remote.filename,
            "size_bytes": remote.size_bytes,
        },
        "local": {
            "path": str(record.path),
            "bytes_written": record.bytes_written,
            "sha256": record.sha256,
            "downloaded_at": record.downloaded_at.isoformat(),
        },
    }


def _record_from_dict(
    payload: dict,
) -> RawFileRecord:
    version = payload.get("version")

    if version != 1:
        raise ValueError("Unsupported raw manifest version: " f"{version!r}")

    remote = payload["remote_file"]

    local = payload["local"]

    remote_file = RemoteFile(
        exchange=Exchange(remote["exchange"]),
        instrument_type=InstrumentType(remote["instrument_type"]),
        market_category=MarketCategory(remote["market_category"]),
        data_type=DataType(remote["data_type"]),
        symbol=remote["symbol"],
        start=datetime.fromisoformat(remote["start"]),
        end=datetime.fromisoformat(remote["end"]),
        url=remote["url"],
        filename=remote["filename"],
        size_bytes=remote["size_bytes"],
    )

    return RawFileRecord(
        remote_file=remote_file,
        path=Path(local["path"]),
        bytes_written=int(local["bytes_written"]),
        sha256=local["sha256"],
        downloaded_at=datetime.fromisoformat(local["downloaded_at"]),
    )


def verify_raw_file(
    raw_path: Path,
    *,
    expected_remote_file: RemoteFile | None = None,
    chunk_size: int = 1024 * 1024,
) -> RawFileRecord | None:
    """Return the record when an existing raw archive verifies successfully.

    Returns None when the archive or its manifest is missing, or when
    the local bytes do not match the completed acquisition record.
    """

    if not raw_path.is_file():
        return None

    record = read_raw_record(raw_path)

    if expected_remote_file is not None and record.remote_file != expected_remote_file:
        return None

    if record is None:
        return None

    # The manifest must describe this exact local destination.
    if record.path != raw_path:
        return None

    stat = raw_path.stat()

    if stat.st_size != record.bytes_written:
        return None

    hasher = hashlib.sha256()

    with raw_path.open("rb") as source:
        while True:
            chunk = source.read(chunk_size)

            if not chunk:
                break

            hasher.update(chunk)

    if hasher.hexdigest() != record.sha256:
        return None

    return record
