"""Readable redirected/narrow/wide storage views with the shared terminal theme."""

from datetime import UTC, datetime
from io import StringIO
from pathlib import Path

import pytest

from forge.shared.terminal.console import create_console
from forge.shared.terminal.storage import render_storage
from forge.storage.browsing import archive_document, pagination
from forge.storage.coverage import CoverageGroup
from forge.storage.models import StorageFilters


@pytest.mark.parametrize("width", [65, 140])
@pytest.mark.parametrize("action", ["list", "show", "coverage"])
def test_storage_readable_layouts(tmp_path: Path, width: int, action: str) -> None:
    stream = StringIO()
    console = create_console(stream=stream)
    console.width = width
    row = archive_document(
        {
            "id": 536,
            "origin": "external",
            "external_archive_id": 1,
            "external_source_key": "recording-one",
            "exchange": "okx",
            "instrument_type": "future",
            "market_category": "linear",
            "scope_kind": "family",
            "scope_identifier": "ETH-USDT",
            "data_type": "order_book_l2",
            "feed": None,
            "base_asset": None,
            "quote_asset": None,
            "start_at": datetime(2026, 10, 27, tzinfo=UTC),
            "end_at": datetime(2026, 10, 30, tzinfo=UTC),
            "status": "complete",
            "relative_path": "fixture.bin",
            "filename": "opaque-archive.bin",
            "bytes_written": 1048576,
            "sha256": "a" * 64,
        },
        tmp_path,
    )
    value: dict[str, object] = {
        "archives": [row],
        "filters": {},
        "pagination": pagination(1, StorageFilters(), 1),
        "totals": {
            "total": 1,
            "recorded_bytes": 1048576,
            "unknown_size_count": 0,
            "unusable_intervals_before_date_filter": 0,
        },
        "semantics": "Nominal archive coverage; no new verification.",
    }
    if action == "coverage":
        group = CoverageGroup(
            {
                key: row.get(key)
                for key in (
                    "origin",
                    "external_source_key",
                    "exchange",
                    "instrument_type",
                    "market_category",
                    "scope_kind",
                    "scope_identifier",
                    "data_type",
                    "feed",
                )
            }
        )
        group.add(row)
        value["coverage"] = [group.document(StorageFilters(), include_gaps=True)]
    render_storage(console, action, value, wide=True)
    text = stream.getvalue()
    assert "\x1b" not in text and "family" in text
    assert "ETH-USDT" in text and "order_book_l2" in text and "unknown" in text
    if action == "list":
        assert "local:536" in text and "opaque-archive.bin" in text and "MiB" in text
    elif action == "show":
        assert "local:536" in text and "Stored timestamps" in text and "no new verification" in text
    else:
        assert "Gaps" in text and "complete local files" in text


@pytest.mark.parametrize("action", ["list", "coverage"])
def test_storage_empty_result(action: str) -> None:
    stream = StringIO()
    console = create_console(stream=stream)
    render_storage(
        console,
        action,
        {
            "archives": [],
            "coverage": [],
            "pagination": pagination(0, StorageFilters(), 0),
            "filters": {},
            "totals": {"total": 0, "recorded_bytes": 0, "unknown_size_count": 0},
            "semantics": "Nominal archive coverage.",
        },
    )
    assert "No matching" in stream.getvalue() and "0 of 0" in stream.getvalue()
