from datetime import datetime, timedelta, timezone

import pytest

from marketforge.acquisition.sources.bitget import BitgetSource


@pytest.fixture
def source():
    return BitgetSource()


CASES = [
    # --------------------------------------------------------------
    # Whole day
    # [Sep 23 00:00, Sep 24 00:00)
    # --------------------------------------------------------------
    (
        datetime(
            2026,
            9,
            23,
            tzinfo=timezone.utc,
        ),
        datetime(
            2026,
            9,
            24,
            tzinfo=timezone.utc,
        ),
        "2026-09-23",
        "2026-09-23",
    ),
    # --------------------------------------------------------------
    # Intraday
    # Both timestamps fall on Sep 23.
    # --------------------------------------------------------------
    (
        datetime(
            2026,
            9,
            23,
            10,
            tzinfo=timezone.utc,
        ),
        datetime(
            2026,
            9,
            23,
            11,
            tzinfo=timezone.utc,
        ),
        "2026-09-23",
        "2026-09-23",
    ),
    # --------------------------------------------------------------
    # Seven calendar days
    # [Sep 23, Sep 30)
    # --------------------------------------------------------------
    (
        datetime(
            2026,
            9,
            23,
            tzinfo=timezone.utc,
        ),
        datetime(
            2026,
            9,
            30,
            tzinfo=timezone.utc,
        ),
        "2026-09-23",
        "2026-09-29",
    ),
    # --------------------------------------------------------------
    # Cross month
    # --------------------------------------------------------------
    (
        datetime(
            2026,
            8,
            30,
            tzinfo=timezone.utc,
        ),
        datetime(
            2026,
            9,
            3,
            tzinfo=timezone.utc,
        ),
        "2026-08-30",
        "2026-09-02",
    ),
    # --------------------------------------------------------------
    # Cross year
    # --------------------------------------------------------------
    (
        datetime(
            2026,
            12,
            30,
            tzinfo=timezone.utc,
        ),
        datetime(
            2027,
            1,
            2,
            tzinfo=timezone.utc,
        ),
        "2026-12-30",
        "2027-01-01",
    ),
    # --------------------------------------------------------------
    # End is intraday.
    #
    # Because Jan 2 contains requested data, Jan 2 must be included.
    # --------------------------------------------------------------
    (
        datetime(
            2026,
            12,
            31,
            20,
            tzinfo=timezone.utc,
        ),
        datetime(
            2027,
            1,
            2,
            6,
            tzinfo=timezone.utc,
        ),
        "2026-12-31",
        "2027-01-02",
    ),
]


@pytest.mark.parametrize(
    (
        "start",
        "end",
        "expected_begin",
        "expected_end",
    ),
    CASES,
)
def test_historical_date_bounds(
    source,
    start,
    end,
    expected_begin,
    expected_end,
):
    begin_date, end_date = source._historical_date_bounds(
        start,
        end,
    )

    assert begin_date == expected_begin
    assert end_date == expected_end


def test_history_windows_single_intraday_window(source):
    start = datetime(
        2026,
        9,
        23,
        10,
        tzinfo=timezone.utc,
    )

    end = datetime(
        2026,
        9,
        23,
        11,
        tzinfo=timezone.utc,
    )

    windows = list(
        source._history_windows(
            start,
            end,
        )
    )

    assert windows == [
        (
            start,
            end,
        )
    ]


def test_history_windows_exact_seven_days(source):
    start = datetime(
        2026,
        9,
        23,
        tzinfo=timezone.utc,
    )

    end = datetime(
        2026,
        9,
        30,
        tzinfo=timezone.utc,
    )

    windows = list(
        source._history_windows(
            start,
            end,
        )
    )

    assert windows == [
        (
            datetime(
                2026,
                9,
                23,
                tzinfo=timezone.utc,
            ),
            datetime(
                2026,
                9,
                30,
                tzinfo=timezone.utc,
            ),
        )
    ]


def test_history_windows_split_long_interval(source):
    start = datetime(
        2026,
        9,
        1,
        tzinfo=timezone.utc,
    )

    end = datetime(
        2026,
        9,
        20,
        tzinfo=timezone.utc,
    )

    windows = list(
        source._history_windows(
            start,
            end,
        )
    )

    assert windows == [
        (
            datetime(
                2026,
                9,
                1,
                tzinfo=timezone.utc,
            ),
            datetime(
                2026,
                9,
                8,
                tzinfo=timezone.utc,
            ),
        ),
        (
            datetime(
                2026,
                9,
                8,
                tzinfo=timezone.utc,
            ),
            datetime(
                2026,
                9,
                15,
                tzinfo=timezone.utc,
            ),
        ),
        (
            datetime(
                2026,
                9,
                15,
                tzinfo=timezone.utc,
            ),
            datetime(
                2026,
                9,
                20,
                tzinfo=timezone.utc,
            ),
        ),
    ]


def test_history_windows_are_contiguous(source):
    start = datetime(
        2026,
        8,
        25,
        12,
        tzinfo=timezone.utc,
    )

    end = datetime(
        2026,
        9,
        18,
        6,
        tzinfo=timezone.utc,
    )

    windows = list(
        source._history_windows(
            start,
            end,
        )
    )

    assert windows[0][0] == start
    assert windows[-1][1] == end

    for current, following in zip(
        windows,
        windows[1:],
    ):
        assert current[1] == following[0]

    for window_start, window_end in windows:
        assert window_start < window_end
        assert window_end - window_start <= timedelta(days=7)


def test_history_payload_converts_exclusive_end_to_inclusive_date(
    source,
):
    from marketforge.models import (
        DataType,
        Exchange,
        Instrument,
        InstrumentType,
        MarketCategory,
    )

    instrument = Instrument(
        exchange=Exchange.BITGET,
        instrument_type=InstrumentType.PERPETUAL,
        market_category=MarketCategory.LINEAR,
        symbol="BTCUSDT",
    )

    payload = source._history_payload(
        instrument=instrument,
        data_type=DataType.TRADE_TICKS,
        start=datetime(
            2026,
            9,
            23,
            tzinfo=timezone.utc,
        ),
        end=datetime(
            2026,
            9,
            30,
            tzinfo=timezone.utc,
        ),
    )

    assert payload["beginTimeStr"] == "2026-09-23"
    assert payload["endTimeStr"] == "2026-09-29"

    assert payload["displaySymbol"] == ["BTCUSDT"]

    assert payload["businessLine"] == 2
    assert payload["businessType"] == 2
    assert payload["dateType"] == 1
