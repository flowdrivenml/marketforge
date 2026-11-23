"""Controlled-width tests for the dataset inspection presentation."""

import json
from io import StringIO
from types import SimpleNamespace

import pytest

from forge.cli.commands import datasets
from forge.datasets.query import DatasetRequest
from forge.shared.config.models import PresentationSettings
from forge.shared.terminal.console import create_console
from forge.shared.terminal.dataset_browse import render_browse

START = 1793059200123456789
END = START + 3_000_000_000


class Terminal(StringIO):
    def isatty(self) -> bool:
        return True


def member(symbol="BTCUSDT", exchange="bybit", start=START, end=END, stream_id="depth"):
    return {
        "exchange": exchange,
        "symbol": symbol,
        "instrument_id": 12,
        "stream_id": stream_id,
        "instrument_type": "perpetual",
        "market_category": "linear",
        "start_timestamp_ns": start,
        "end_timestamp_ns": end,
    }


def row(identity, *, publication="101", integrity="clean", members=None, **metadata):
    summary = {"rows": 9_419_550, "size_bytes": 108_404_168, **metadata}
    return {
        "id": identity,
        "engine_dataset_id": publication,
        "instrument_namespace": "ns",
        "primary_file": "streams/" + "f" * 64 + "/depth/part-000001.parquet",
        "operation": "normalize",
        "data_type": "l2",
        "status": "complete",
        "start_timestamp_ns": START,
        "end_timestamp_ns": END,
        "summary": {
            **summary,
            "memberships": [member()] if members is None else members,
            "integrity": integrity,
            "integrity_scope": "publication",
        },
    }


def page(*rows):
    return {
        "datasets": list(rows),
        "offset": 0,
        "limit": 50,
        "has_more": False,
        "view": "files",
        "environment": "testing",
        "total": len(rows),
        "stages": {"normalize": {"files": len(rows)}},
        "conversion_index": "available",
        "unavailable_conversion_outputs": [],
    }


@pytest.mark.parametrize("width", [80, 120, 160])
def test_precise_file_bounds_keep_integer_nanoseconds(width):
    output = StringIO()
    console = create_console(stream=output)
    console.width = width
    render_browse(console, page(row(98761)), DatasetRequest("list", view="files", precise=True))
    result = output.getvalue()
    assert "2026-10-27T00:00:00.123456789Z" in result
    assert "9,419,550 rows" in result
    assert "98761" in result
    assert all(len(line) <= width for line in result.splitlines())


@pytest.mark.parametrize(
    "color,as_json,tty,term",
    [
        ("always", True, True, "xterm-256color"),
        ("always", False, False, "xterm-256color"),
        ("never", False, True, "xterm-256color"),
        ("always", False, True, "dumb"),
    ],
)
def test_cli_plain_output_contract(monkeypatch, capsys, color, as_json, tty, term):
    monkeypatch.setenv("TERM", term)
    monkeypatch.setenv("COLORTERM", "truecolor")
    output = Terminal() if tty else StringIO()
    settings = PresentationSettings(color=color)
    context = SimpleNamespace(
        paths=None,
        console=create_console(settings, stream=output),
        settings=SimpleNamespace(presentation=settings),
    )
    args = SimpleNamespace(
        as_json=as_json, dataset_action="list", view="files", wide=False, precise=True
    )

    async def browse(*_, detailed):
        return page(row(98761))

    monkeypatch.setattr(datasets, "browse", browse)
    assert datasets.run(args, context) == 0
    if as_json:
        payload = capsys.readouterr().out
        assert (
            "\x1b" not in payload
            and json.loads(payload)["datasets"][0]["start_timestamp_ns"] == START
        )
        assert output.getvalue() == ""
    else:
        assert "\x1b" not in output.getvalue() and "98761" in output.getvalue()
