"""Editable acquisition configuration templates."""

from __future__ import annotations

from pathlib import Path
from typing import Final

ACQUISITION_TEMPLATE: Final[str] = """\
# Edit the interval and requests before mapping or downloading.
version = 1

# Half-open UTC interval: start is included, end is excluded.
[interval]
start = 2026-09-01T00:00:00Z
end = 2026-09-03T00:00:00Z

# Repeat [[requests]] to select additional archive collections.
[[requests]]
exchange = "bybit"
instrument_type = "spot"
market_category = "spot"
data_type = "trades"

[requests.scope]
kind = "instrument"
identifier = "BTCUSDT"
"""


def write_acquisition_template(path: Path) -> Path:
    """Create an acquisition configuration without overwriting a file."""
    destination = path.expanduser().resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)

    with destination.open("x", encoding="utf-8") as stream:
        stream.write(ACQUISITION_TEMPLATE)

    return destination
