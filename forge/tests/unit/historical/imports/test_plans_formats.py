"""Frozen bounded selections and isolated physical/profile drafts."""

import tomllib
from datetime import UTC, datetime
from pathlib import Path

import pytest

from forge.historical.imports.formats import draft
from forge.historical.imports.models import (
    HistoricalSpecs,
    ImportConfig,
    ImportIdentity,
    freeze_inputs,
)
from forge.shared.config.toml import dumps


def config(root: Path) -> ImportConfig:
    return ImportConfig(
        source_key="recording",
        root=str(root),
        patterns=["*.csv"],
        data_type="trades",
        identity=ImportIdentity(
            exchange="binance", market_category="spot", instrument_type="spot", symbol="BTCUSDT"
        ),
        specifications=HistoricalSpecs(
            as_of=datetime(2026, 1, 1, tzinfo=UTC),
            base_asset="BTC",
            quote_asset="USDT",
            quantity_type="base",
        ),
    )


def test_inputs_freeze_and_pattern_bound(tmp_path: Path) -> None:
    (tmp_path / "first.csv").write_text("a,b\n1,2\n")
    frozen = freeze_inputs(config(tmp_path))
    (tmp_path / "later.csv").write_text("a,b\n3,4\n")
    assert len(frozen) == 1
    assert frozen[0].reference == "first.csv"
    with pytest.raises(Exception, match="max_files"):
        freeze_inputs(config(tmp_path).model_copy(update={"max_files": 1}))


def test_real_profile_draft_is_outside_active_catalogue(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "record.csv").write_text("timestamp,price,quantity,side\n1,2,3,buy\n")
    directory = tmp_path / "data/.config/historical/formats/trades"
    directory.mkdir(parents=True)
    template = {
        "schema_version": 1,
        "kind": "physical",
        "id": "existing",
        "container_format": "csv",
        "record_format": "rows",
        "supported_containers": ["plain"],
        "raw_schema": {"fields": [{"name": "value", "type": "string"}], "header": True},
    }
    (directory / "physical.toml").write_text(dumps(template))
    result = draft(tmp_path, config(source), "recorder-trades-v1", "record.csv", header=True)
    path = Path(str(result["profile"]))
    assert path.is_file() and not path.is_relative_to(directory)
    with path.open("rb") as stream:
        profile = tomllib.load(stream)
    assert profile["unsupported_reason"].startswith("DRAFT")
    assert profile["rules"][0]["rules"] == {}
    assert "timestamp_encoding" not in profile["rules"][0]
    assert result["import_configuration_updated"] is False
    with pytest.raises(Exception, match="already exists"):
        draft(tmp_path, config(source), "recorder-trades-v1", "record.csv", header=True)
