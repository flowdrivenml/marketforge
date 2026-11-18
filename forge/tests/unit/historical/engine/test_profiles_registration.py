"""Namespaced discovery, strict declarations and deterministic frozen inventory."""

from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from forge.historical.imports.models import HistoricalSpecs
from forge.historical.profiles.service import discover, identity, inspect
from forge.historical.registration.models import Recipe
from forge.historical.registration.service import inspect as inspect_registration
from forge.shared.errors import MarketForgeError
from forge.shared.paths import ProjectPaths


def test_namespaced_discovery_without_services(tmp_path: Path) -> None:
    paths = ProjectPaths(tmp_path, tmp_path / "data")
    directory = paths.config_dir / "historical/profiles/trades"
    directory.mkdir(parents=True)
    source = directory / "binance.spot.spot.trades.toml"
    source.write_text(
        'id="binance.spot.spot.trades"\nexchange="binance"\ninstrument_type="spot"\nmarket_category="spot"\n'
    )
    entries = discover(paths, exchange="binance", type="trades")
    assert len(entries) == 1 and entries[0]["validation"] == "TOML syntax and namespace only"
    assert discover(paths, exchange="bitget") == []
    source.rename(directory / "wrong.toml")
    with pytest.raises(MarketForgeError, match="filename"):
        inspect(directory / "wrong.toml")


@pytest.mark.parametrize(
    "value",
    [
        "external",
        "binance.spot.spot.trades.variant",
        "BINANCE.spot.spot.trades",
        "../binance.spot.spot.trades",
    ],
)
def test_profile_namespace_rejects_aliases(value: str) -> None:
    with pytest.raises(MarketForgeError):
        identity(value)


def test_exact_specifications_and_unknown_values() -> None:
    spec = HistoricalSpecs.model_validate(
        {
            "as_of": "2026-01-01T00:00:00+00:00",
            "base_asset": "BTC",
            "quote_asset": "USD",
            "quantity_type": "base",
            "tick_size": "0.00000000000000000001",
        }
    )
    assert str(spec.tick_size) == "1E-20"
    assert spec.max_qty is None
    with pytest.raises(ValidationError):
        HistoricalSpecs.model_validate({**spec.model_dump(), "tick_size": "0"})


def test_recipe_unknown_fields_and_references() -> None:
    with pytest.raises(ValidationError):
        Recipe.model_validate({"registration": {"version": 9, "name": "invalid"}, "unknown": True})


def test_duplicate_physical_inputs_rejected(tmp_path: Path) -> None:
    from datetime import UTC, datetime, timedelta

    data: dict[str, Any] = {
        "registration": {"version": 1, "name": "test"},
        "source": {
            "key": "test",
            "provider": "test",
            "product": "test",
            "root": str(tmp_path),
            "provenance": "synthetic",
        },
        "coverage": {
            "start": datetime(2026, 1, 1, tzinfo=UTC),
            "end": datetime(2026, 1, 1, tzinfo=UTC) + timedelta(days=1),
        },
        "inputs": [
            {
                "instrument": "binance.spot.spot.BTCUSDT",
                "profile": "binance.spot.spot.trades",
                "content": "trades",
                "files": ["one.csv"],
            }
        ]
        * 2,
    }
    (tmp_path / "one.csv").write_text("synthetic")
    with pytest.raises(MarketForgeError, match="duplicate physical"):
        inspect_registration(Recipe.model_validate(data), ProjectPaths(tmp_path, tmp_path / "data"))
