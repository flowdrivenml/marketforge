"""Bounded identity discovery checks stable bytes, excluding read access time."""

import json
from datetime import UTC, datetime
from pathlib import Path
from subprocess import CompletedProcess
from typing import Any

import pytest

from forge.historical.engine.managed import checksum
from forge.historical.metadata.discovery import discover
from forge.shared.errors import MarketForgeError
from forge.shared.paths import ProjectPaths


@pytest.mark.parametrize("changed", [False, True])
def test_discovery_stability_and_nonprocessing_contract(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, changed: bool
) -> None:
    paths = ProjectPaths(tmp_path, tmp_path / "data")
    paths.raw_dir.mkdir(parents=True)
    source = paths.raw_dir / "sample.csv"
    source.write_text("native identity evidence")
    operation = tmp_path / "operation"
    operation.mkdir()
    row: dict[str, Any] = {
        "id": 1,
        "relative_path": source.name,
        "bytes_written": source.stat().st_size,
        "sha256": checksum(source),
        "exchange": "okx",
        "instrument_type": "future",
        "market_category": "linear",
        "scope_identifier": "BTC-USD_UM",
        "start_at": datetime(2026, 10, 1, tzinfo=UTC),
        "end_at": datetime(2026, 11, 1, tzinfo=UTC),
        "data_type": "trades",
    }
    reference = {
        "base_asset": "BTC",
        "quote_asset": "USD",
        "tick_size": "0.1",
        "contract_value": "0.01",
        "contract_value_asset": "BTC",
    }

    def run(args: list[str], **kwargs: Any) -> CompletedProcess[str]:
        assert "--resolve-batch" in args and "--batch" not in args
        if changed:
            source.write_text("changed bytes")
        return CompletedProcess(
            args,
            0,
            json.dumps(
                {
                    "kind": "normalization_resolution",
                    "normalization_executed": False,
                    "instruments": [{"symbol": "BTC-USD_UM-261030"}],
                    "members": [],
                }
            ),
        )

    def executable(_: ProjectPaths) -> Path:
        return tmp_path / "engine"

    monkeypatch.setattr("forge.historical.metadata.discovery.resolve", executable)
    monkeypatch.setattr("forge.historical.metadata.discovery.supervised", run)
    if changed:
        with pytest.raises(MarketForgeError, match="changed"):
            discover(paths, row, reference, 1, operation)
    else:
        result = discover(paths, row, reference, 1, operation)
        assert result["symbols"] == ["BTC-USD_UM-261030"]
        assert not result["historical_family_complete"]
        assert result["normalization_executed"] is False
    assert list(operation.iterdir()) == [operation / "identity-1.json"]
