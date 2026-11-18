"""Controlled-width tests for the dataset inspection presentation."""

import json
import re
from io import StringIO
from types import SimpleNamespace

import pytest

from forge.cli.commands import datasets
from forge.shared.config.models import PresentationSettings
from forge.shared.terminal.console import create_console
from forge.shared.terminal.datasets import coverage_groups, render_coverage, render_datasets

START = 1793059200123456789
END = START + 3_000_000_000
LONG = "BTC-VERY-LONG-CANONICAL-SYMBOL-" + "X" * 128
ANSI = re.compile(r"\x1b\[[0-9;]*m")


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
    return {"datasets": list(rows), "offset": 50, "limit": 50, "has_more": True}


@pytest.mark.parametrize("width", [80, 100, 120, 160])
@pytest.mark.parametrize("precise", [False, True])
def test_responsive_listing_preserves_timestamps_and_identities(width, precise):
    output = StringIO()
    console = create_console(stream=output)
    console.width = width
    render_datasets(
        console,
        page(
            row(98761, members=[member(LONG), member("ETHUSDT", "okx")]),
            row(98762),
            row(98763, publication="102", integrity="degraded"),
            row(
                98764, publication="103", integrity="failed", members=[], rows=None, size_bytes=None
            ),
        ),
        wide=width == 160,
        precise=precise,
    )
    text = output.getvalue()
    assert all(len(line) <= width for line in text.splitlines())
    assert ("START (UTC)" in text) == (width >= 120)
    assert "2026 · UTC" in text and "10-27 00:00:00" in text
    assert (".123456789" in text) is precise
    assert LONG in "".join(line.strip() for line in text.splitlines())
    assert "ETHUSDT" in text and "Multi-stream / mixed membership" in text
    assert all(text.count(str(identity)) == 1 for identity in range(98761, 98765))
    assert text.count("Publication 101") == 1
    assert "9.42M" in text and "103.4 MiB" in text and "Unknown" in text
    assert "Next page --offset 100" in text and "…" not in text
    assert "clean" in text and "degraded" in text and "failed" in text


def test_cross_year_does_not_elide_year():
    one = row(98761)
    one["end_timestamp_ns"] = 1_800_000_000_000_000_000
    output = StringIO()
    console = create_console(stream=output)
    render_datasets(console, page(one))
    assert "2026-10-27" in output.getvalue() and "2027-" in output.getvalue()


def test_coverage_uses_membership_bounds_and_distinct_file_counts():
    relevant = member(start=START + 1, end=END - 1)
    records = [
        row(1, members=[relevant, {**relevant, "stream_id": "extra"}, member(exchange="okx")]),
        row(2, members=[member(start=END + 100, end=END + 200)]),
    ]
    aggregated = coverage_groups(records, {"exchange": "bybit"})
    assert len(aggregated) == 1
    assert aggregated[0]["files"] == {1, 2}
    assert aggregated[0]["start_timestamp_ns"] == START + 1
    assert aggregated[0]["end_timestamp_ns"] == END + 200
    filtered = coverage_groups(records, {"exchange": "bybit", "end": END})
    assert filtered[0]["files"] == {1}
    assert filtered[0]["end_timestamp_ns"] == END - 1
    output = StringIO()
    console = create_console(stream=output)
    console.width = 120
    render_coverage(console, page(*records), filters={})
    result = output.getvalue()
    assert "DATA COVERAGE" in result and "OBSERVED UTC ENVELOPE" in result
    assert "Continuity unknown" in result
    assert "Publication 101" not in result and "EVENTS" not in result
    assert "2026-10-27 00:00:00" in result and "…" not in result


@pytest.mark.parametrize("mode", ["progress", "silent"])
def test_terminal_uses_tokyo_night_styles(monkeypatch, mode):
    monkeypatch.delenv("NO_COLOR", raising=False)
    monkeypatch.setenv("TERM", "xterm-256color")
    monkeypatch.setenv("COLORTERM", "truecolor")
    output = Terminal()
    console = create_console(PresentationSettings(mode=mode), stream=output)
    render_datasets(
        console,
        page(
            row(98761),
            row(98762, publication="102", integrity="degraded"),
            row(98763, publication="103", integrity="failed"),
        ),
    )
    result = output.getvalue()
    for rgb in (
        "187;154;247",
        "125;207;255",
        "122;162;247",
        "158;206;106",
        "224;175;104",
        "247;118;142",
    ):
        assert f"38;2;{rgb}" in result
    assert "MARKETFORGE" in ANSI.sub("", result)


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
        console=create_console(settings, stream=output),
        settings=SimpleNamespace(presentation=settings),
    )
    args = SimpleNamespace(as_json=as_json, dataset_action="list", wide=False, precise=True)

    async def browse(*_):
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


@pytest.mark.parametrize("width", [80, 100, 120, 160])
def test_coverage_width_keeps_each_timestamp_complete(width):
    output = StringIO()
    console = create_console(stream=output)
    console.width = width
    render_coverage(console, page(row(246, members=[member(LONG)])), filters={})
    text = output.getvalue()
    assert all(len(line) <= width for line in text.splitlines())
    assert "2026-10-27 00:00:00" in text
    assert "2026-10-27 00:00:03" in text
    assert LONG in "".join(line.strip() for line in text.splitlines())
    assert "…" not in text
