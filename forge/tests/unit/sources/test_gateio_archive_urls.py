from datetime import datetime, timezone
from unittest.mock import Mock

import pytest
import requests

from marketforge.acquisition.sources.gateio import GATEIO_ARCHIVE_POLICY, GateIOSource
from marketforge.models import (
    DataType,
    Exchange,
    Instrument,
    InstrumentType,
    MarketCategory,
)


@pytest.mark.parametrize(
    (
        "instrument_type",
        "market_category",
        "data_type",
        "expected_path",
    ),
    [
        (
            InstrumentType.SPOT,
            MarketCategory.SPOT,
            DataType.TRADE_TICKS,
            "/spot/deals/",
        ),
        (
            InstrumentType.PERPETUAL,
            MarketCategory.LINEAR,
            DataType.TRADE_TICKS,
            "/futures_usdt/trades/",
        ),
        (
            InstrumentType.PERPETUAL,
            MarketCategory.INVERSE,
            DataType.TRADE_TICKS,
            "/futures_btc/trades/",
        ),
        (
            InstrumentType.PERPETUAL,
            MarketCategory.LINEAR,
            DataType.ORDER_BOOK_L2,
            "/futures_usdt/orderbooks/",
        ),
        (
            InstrumentType.PERPETUAL,
            MarketCategory.INVERSE,
            DataType.ORDER_BOOK_L2,
            "/futures_btc/orderbooks/",
        ),
    ],
)
def test_archive_url_routing(
    instrument_type,
    market_category,
    data_type,
    expected_path,
):
    source = GateIOSource()

    instrument = Instrument(
        exchange=Exchange.GATEIO,
        instrument_type=instrument_type,
        market_category=market_category,
        symbol="BTC_USDT",
    )

    url = source._archive_url(
        instrument=instrument,
        data_type=data_type,
        timestamp=datetime(
            2026,
            9,
            27,
            20,
            tzinfo=timezone.utc,
        ),
    )

    assert expected_path in url


def test_archive_missing_returns_false():
    source = GateIOSource()

    response = requests.Response()
    response.status_code = 404
    response.url = "https://download.gatedata.org/missing.csv.gz"

    source.http.head = Mock(return_value=response)

    exists = source._archive_exists("https://download.gatedata.org/missing.csv.gz")

    assert exists is False

    source.http.head.assert_called_once_with(
        "https://download.gatedata.org/missing.csv.gz",
        policy=GATEIO_ARCHIVE_POLICY,
        allowed_status_codes={404},
        allow_redirects=True,
    )
