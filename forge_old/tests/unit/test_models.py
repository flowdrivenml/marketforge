from datetime import datetime, timezone

import pytest

from marketforge.models import (
    AcquisitionRequest,
    DataType,
    Exchange,
    Instrument,
    InstrumentType,
    MarketCategory,
)


@pytest.fixture
def instrument():
    return Instrument(
        exchange=Exchange.BYBIT,
        instrument_type=InstrumentType.PERPETUAL,
        market_category=MarketCategory.LINEAR,
        symbol="BTCUSDT",
    )


def test_acquisition_request(
    instrument,
):
    start = datetime(
        2026,
        9,
        1,
        tzinfo=timezone.utc,
    )

    end = datetime(
        2026,
        9,
        8,
        tzinfo=timezone.utc,
    )

    request = AcquisitionRequest(
        target=instrument,
        data_type=DataType.TRADE_TICKS,
        start=start,
        end=end,
    )

    assert request.target == instrument
    assert request.data_type == DataType.TRADE_TICKS
    assert request.start == start
    assert request.end == end


def test_acquisition_request_rejects_equal_interval(
    instrument,
):
    timestamp = datetime(
        2026,
        9,
        1,
        tzinfo=timezone.utc,
    )

    with pytest.raises(
        ValueError,
        match="earlier than end",
    ):
        AcquisitionRequest(
            target=instrument,
            data_type=DataType.TRADE_TICKS,
            start=timestamp,
            end=timestamp,
        )


def test_acquisition_request_rejects_reversed_interval(
    instrument,
):
    with pytest.raises(
        ValueError,
        match="earlier than end",
    ):
        AcquisitionRequest(
            target=instrument,
            data_type=DataType.TRADE_TICKS,
            start=datetime(
                2026,
                9,
                8,
                tzinfo=timezone.utc,
            ),
            end=datetime(
                2026,
                9,
                1,
                tzinfo=timezone.utc,
            ),
        )


def test_acquisition_request_rejects_naive_start(
    instrument,
):
    with pytest.raises(
        ValueError,
        match="start must be timezone-aware",
    ):
        AcquisitionRequest(
            target=instrument,
            data_type=DataType.TRADE_TICKS,
            start=datetime(
                2026,
                9,
                1,
            ),
            end=datetime(
                2026,
                9,
                8,
                tzinfo=timezone.utc,
            ),
        )


def test_acquisition_request_rejects_naive_end(
    instrument,
):
    with pytest.raises(
        ValueError,
        match="end must be timezone-aware",
    ):
        AcquisitionRequest(
            target=instrument,
            data_type=DataType.TRADE_TICKS,
            start=datetime(
                2026,
                9,
                1,
                tzinfo=timezone.utc,
            ),
            end=datetime(
                2026,
                9,
                8,
            ),
        )
