import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from marketforge.models import (
    DataType,
    Exchange,
    InstrumentType,
    MarketCategory,
    RawFileRecord,
    RemoteFile,
)
from marketforge.storage.manifest import (
    manifest_path,
    read_raw_record,
    verify_raw_file,
    write_raw_record,
)


def make_record(
    tmp_path: Path,
) -> RawFileRecord:
    remote_file = RemoteFile(
        exchange=Exchange.BYBIT,
        instrument_type=InstrumentType.PERPETUAL,
        market_category=MarketCategory.LINEAR,
        data_type=DataType.TRADE_TICKS,
        symbol="BTCUSDT",
        start=datetime(
            2026,
            9,
            1,
            tzinfo=timezone.utc,
        ),
        end=datetime(
            2026,
            9,
            2,
            tzinfo=timezone.utc,
        ),
        url="https://example.com/BTCUSDT.zip",
        filename="BTCUSDT.zip",
        size_bytes=1234,
    )

    return RawFileRecord(
        remote_file=remote_file,
        path=(tmp_path / "BTCUSDT.zip"),
        bytes_written=1234,
        sha256="abc123",
        downloaded_at=datetime(
            2026,
            9,
            30,
            12,
            30,
            tzinfo=timezone.utc,
        ),
    )


def test_manifest_path(
    tmp_path,
):
    raw_path = tmp_path / "BTCUSDT.zip"

    assert manifest_path(raw_path) == (tmp_path / "BTCUSDT.zip.manifest.json")


def test_write_and_read_raw_record(
    tmp_path,
):
    record = make_record(tmp_path)

    path = write_raw_record(record)

    assert path.exists()

    loaded = read_raw_record(record.path)

    assert loaded == record


def test_manifest_is_readable_json(
    tmp_path,
):
    record = make_record(tmp_path)

    path = write_raw_record(record)

    payload = json.loads(path.read_text(encoding="utf-8"))

    assert payload["version"] == 1

    assert payload["remote_file"]["symbol"] == "BTCUSDT"

    assert payload["local"]["bytes_written"] == 1234

    assert payload["local"]["sha256"] == "abc123"


def test_missing_manifest_returns_none(
    tmp_path,
):
    raw_path = tmp_path / "missing.zip"

    assert read_raw_record(raw_path) is None


def test_unknown_manifest_version_raises(
    tmp_path,
):
    raw_path = tmp_path / "BTCUSDT.zip"

    path = manifest_path(raw_path)

    path.write_text(
        json.dumps(
            {
                "version": 999,
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match="Unsupported raw manifest version",
    ):
        read_raw_record(raw_path)


def test_verify_raw_file(
    tmp_path,
):
    content = b"marketforge-data"

    record = make_record(tmp_path)

    record.path.write_bytes(content)

    record = RawFileRecord(
        remote_file=record.remote_file,
        path=record.path,
        bytes_written=len(content),
        sha256=__import__("hashlib").sha256(content).hexdigest(),
        downloaded_at=record.downloaded_at,
    )

    write_raw_record(record)

    verified = verify_raw_file(record.path)

    assert verified == record


def test_verify_missing_raw_file_returns_none(
    tmp_path,
):
    raw_path = tmp_path / "missing.zip"

    assert verify_raw_file(raw_path) is None


def test_verify_without_manifest_returns_none(
    tmp_path,
):
    raw_path = tmp_path / "BTCUSDT.zip"

    raw_path.write_bytes(b"data")

    assert verify_raw_file(raw_path) is None


def test_verify_wrong_size_returns_none(
    tmp_path,
):
    record = make_record(tmp_path)

    record.path.write_bytes(b"wrong-size")

    write_raw_record(record)

    assert verify_raw_file(record.path) is None


def test_verify_corrupted_file_returns_none(
    tmp_path,
):
    content = b"original-data"

    record = make_record(tmp_path)

    record.path.write_bytes(content)

    valid_record = RawFileRecord(
        remote_file=record.remote_file,
        path=record.path,
        bytes_written=len(content),
        sha256=hashlib.sha256(content).hexdigest(),
        downloaded_at=record.downloaded_at,
    )

    write_raw_record(valid_record)

    # Same byte count, different contents.
    record.path.write_bytes(b"corrupted-dat")

    assert len(b"corrupted-dat") == len(content)

    assert verify_raw_file(record.path) is None
