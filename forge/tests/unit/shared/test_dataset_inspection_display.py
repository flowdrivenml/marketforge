"""Terminal contract tests for file inspection without processing or a database."""

import json
from decimal import Decimal
from io import StringIO
from types import SimpleNamespace

import pytest

from forge.cli.commands import datasets
from forge.datasets.query import preview_columns
from forge.shared.terminal.console import create_console
from forge.shared.terminal.datasets import render_inspection

START = 1793059200123456789
LONG = "BTC-" + "X" * 110
MEMBER = {
    "symbol": LONG,
    "exchange": "bybit",
    "instrument_type": "future",
    "market_category": "linear",
    "instrument_id": 48,
    "stream_id": "bybit.family:BTC",
    "start_timestamp_ns": START,
}


@pytest.fixture
def file():
    return {
        "id": 715,
        "data_type": "trade",
        "operation": "normalize",
        "summary": {
            "memberships": [MEMBER, {**MEMBER, "symbol": "ETHUSDT", "exchange": "okx"}],
            "rows": 20,
            "size_bytes": 7320,
            "membership_scope": "file",
            "integrity": "clean",
        },
        "primary_file": "stream/trades/part-000000.parquet",
        "publication_id": 4,
        "storage_path": "/private/root/part-000000.parquet",
        "file_sha256": "a" * 64,
        "storage_format": "parquet",
        "status": "complete",
        "start_timestamp_ns": START,
    }


@pytest.mark.parametrize("width", [80, 100, 120, 160])
def test_search_and_details_preserve_membership_and_timestamps(width, file):
    output = StringIO()
    console = create_console(stream=output)
    console.width = width
    render_inspection(
        console,
        "show",
        {
            "file": file,
            "publication": {"publication_id": 4, "engine_dataset_id": 1234},
            "counts": {"raw": 3, "datasets": 0},
        },
    )
    render_inspection(
        console,
        "streams",
        {
            "id": 715,
            "file": file,
            "streams": file["summary"]["memberships"],
            "scope": "file",
        },
    )
    text = output.getvalue()
    assert all(len(line) <= width for line in text.splitlines())
    assert LONG in "".join(line.strip() for line in text.splitlines())
    assert "bybit.family:BTC" in text and "7.1 KiB" in text
    assert ".123456789 UTC" in text
    assert "/private/" not in text and file["file_sha256"] not in text and "…" not in text


def test_scoped_lineage(file):
    result = {
        "file": file,
        "counts": {"raw": 3, "datasets": 0},
        "limit": 1,
        "parents": [],
        "raw": [{"id": 1884, "exchange": "okx", "filename": "archive.zip", "sha256": "a" * 64}],
        "selected": {"raw": 500, "datasets": 2},
        "checksums": False,
    }
    output = StringIO()
    console = create_console(stream=output)
    render_inspection(console, "lineage", result)
    text = output.getvalue()
    assert "3 raw archives" in text and "500 raw archives" in text and "archive.zip" in text
    assert "SHA-256" not in text and "--publication" in text
    output.seek(0)
    output.truncate()
    render_inspection(
        console,
        "lineage",
        {
            **result,
            "checksums": True,
            "publication_raw": [{**result["raw"][0], "id": 1885}],
            "publication_datasets": [{"id": 714, "primary_file": "input.parquet"}],
        },
    )
    assert "SHA-256" in output.getvalue() and "#1885" in output.getvalue()
    assert "input.parquet" in output.getvalue()


@pytest.mark.parametrize(
    ("kind", "field", "value", "label"),
    [
        ("trade", "trade_id", "490", "TRADE ID"),
        ("l2", "order_count", 2, "ORDERS"),
    ],
)
def test_schema_and_preview(file, kind, field, value, label):
    file["data_type"] = kind
    cols = ["event_timestamp_ns", "side", "price", "quantity_base", "trade_id", "order_count"]
    projection = preview_columns(file, cols)
    assert projection[-1] == field
    price = Decimal("80982.9000000000000000000000000000")
    output = StringIO()
    console = create_console(stream=output)
    console.width = 120
    render_inspection(
        console,
        "schema",
        {
            "id": 715,
            "columns": [{"name": "price", "type": "decimal256(76, 28)", "nullable": False}],
        },
    )
    assert "decimal256(76, 28)" in output.getvalue()
    result = {
        "id": 715,
        "file": file,
        "rows": [
            {
                "price": price,
                "event_timestamp_ns": START,
                "side": "buy",
                field: value,
            }
        ],
        "projection": projection,
        "precise": True,
        "compact": True,
    }
    render_inspection(console, "head", result)
    assert "80982.9" in output.getvalue() and str(price) not in output.getvalue()
    assert ".123456789 UTC" in output.getvalue() and label in output.getvalue()
    output.seek(0)
    output.truncate()
    render_inspection(console, "head", {**result, "projection": ["price"], "compact": False})
    assert "price" in output.getvalue() and label not in output.getvalue()


def test_json_bypasses_the_renderer(monkeypatch, capsys):
    expected = {"id": 715, "rows": [{"event_timestamp_ns": START, "price": Decimal("12.300")}]}

    async def browse(*_, detailed):
        return expected

    monkeypatch.setattr(datasets, "browse", browse)
    assert (
        datasets.run(
            SimpleNamespace(as_json=True, dataset_action="head"), SimpleNamespace(paths=None)
        )
        == 0
    )
    raw = capsys.readouterr().out
    assert "\x1b" not in raw
    assert json.loads(raw) == {
        "id": 715,
        "rows": [{"event_timestamp_ns": START, "price": "12.300"}],
    }


def test_inspection_uses_the_interactive_theme(file, monkeypatch):
    class Terminal(StringIO):
        def isatty(self):
            return True

    monkeypatch.delenv("NO_COLOR", raising=False)
    monkeypatch.setenv("TERM", "xterm-256color")
    monkeypatch.setenv("COLORTERM", "truecolor")
    output = Terminal()
    render_inspection(
        create_console(stream=output),
        "show",
        {
            "file": file,
            "publication": {"publication_id": 4},
            "counts": {"raw": 3, "datasets": 0},
        },
    )
    text = output.getvalue()
    for rgb in ("187;154;247", "125;207;255", "158;206;106"):
        assert f"\x1b[38;2;{rgb}m" in text or f";38;2;{rgb}m" in text
