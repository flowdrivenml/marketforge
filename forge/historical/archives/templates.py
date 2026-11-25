"""Editable acquisition configuration templates."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Final

from forge.shared.config.toml import dumps

ACQUISITION_TEMPLATE: Final[str] = (
    "# Working example of one day of Bybit BTCUSDT perpetual trades.\n"
    "# Replace the dates and request with the data you want.\n"
    "# Half-open UTC interval includes the start and excludes the end.\n"
    "# Repeat [[requests]] to select additional archive collections.\n"
    + dumps(
        {
            "version": 1,
            "interval": {
                "start": datetime(2026, 9, 1, tzinfo=UTC),
                "end": datetime(2026, 9, 2, tzinfo=UTC),
            },
            "requests": [
                {
                    "exchange": "bybit",
                    "instrument_type": "perpetual",
                    "market_category": "linear",
                    "data_type": "trades",
                    "scope": {"kind": "instrument", "identifier": "BTCUSDT"},
                }
            ],
        }
    )
)


def write_acquisition_template(path: Path) -> Path:
    """Create an acquisition configuration without overwriting a file."""
    destination = path.expanduser().resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)

    with destination.open("x", encoding="utf-8") as stream:
        stream.write(ACQUISITION_TEMPLATE)

    return destination
