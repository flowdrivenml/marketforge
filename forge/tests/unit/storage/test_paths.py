from datetime import datetime, timezone
from pathlib import Path

import pytest

from marketforge.models import (
    DataType,
    Exchange,
    InstrumentType,
    MarketCategory,
    RemoteFile,
)
from marketforge.storage.paths import raw_file_path, safe_filename, safe_path_component


def remote_file(
    *,
    exchange=Exchange.BYBIT,
    instrument_type=InstrumentType.PERPETUAL,
    market_category=MarketCategory.LINEAR,
    data_type=DataType.TRADE_TICKS,
    symbol="BTCUSDT",
    filename="BTCUSDT_2026-09-01.csv.gz",
):
    return RemoteFile(
        exchange=exchange,
        instrument_type=instrument_type,
        market_category=market_category,
        data_type=data_type,
        symbol=symbol,
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
        url="https://example.com/archive",
        filename=filename,
        size_bytes=None,
    )


def test_raw_file_path():
    file = remote_file()

    path = raw_file_path(
        file,
        data_root=Path("/tmp/marketforge"),
    )

    assert path == Path(
        "/tmp/marketforge/"
        "raw/"
        "bybit/"
        "perpetual/"
        "linear/"
        "trade_ticks/"
        "BTCUSDT/"
        "BTCUSDT_2026-09-01.csv.gz"
    )


def test_spot_symbol_with_slash():
    file = remote_file(
        exchange=Exchange.BITGET,
        instrument_type=InstrumentType.SPOT,
        market_category=MarketCategory.SPOT,
        symbol="BTC/USDT",
        filename="BTCUSDT_20260901.zip",
    )

    path = raw_file_path(
        file,
        data_root=Path("data"),
    )

    assert path == Path(
        "data/"
        "raw/"
        "bitget/"
        "spot/"
        "spot/"
        "trade_ticks/"
        "BTC_USDT/"
        "BTCUSDT_20260901.zip"
    )


def test_gate_monthly_archive():
    file = remote_file(
        exchange=Exchange.GATEIO,
        instrument_type=InstrumentType.PERPETUAL,
        market_category=MarketCategory.LINEAR,
        symbol="BTC_USDT",
        filename="BTC_USDT-202608.csv.gz",
    )

    path = raw_file_path(file)

    assert path.name == "BTC_USDT-202608.csv.gz"

    assert path == Path(
        "data/"
        "raw/"
        "gateio/"
        "perpetual/"
        "linear/"
        "trade_ticks/"
        "BTC_USDT/"
        "BTC_USDT-202608.csv.gz"
    )


def test_filename_cannot_escape_destination():
    file = remote_file(
        filename="../../evil.zip",
    )

    path = raw_file_path(file)

    assert path.name == "evil.zip"
    assert ".." not in path.parts


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("BTCUSDT", "BTCUSDT"),
        ("BTC/USDT", "BTC_USDT"),
        ("BTC\\USDT", "BTC_USDT"),
        (" BTCUSDT ", "BTCUSDT"),
    ],
)
def test_safe_path_component(
    value,
    expected,
):
    assert safe_path_component(value) == expected


@pytest.mark.parametrize(
    "value",
    [
        "",
        " ",
        ".",
        "..",
    ],
)
def test_invalid_path_component(value):
    with pytest.raises(ValueError):
        safe_path_component(value)


def test_safe_filename_strips_directories():
    assert safe_filename("../../BTCUSDT.zip") == "BTCUSDT.zip"


@pytest.mark.parametrize(
    "value",
    [
        "",
        " ",
        ".",
        "..",
    ],
)
def test_invalid_filename(value):
    with pytest.raises(ValueError):
        safe_filename(value)
