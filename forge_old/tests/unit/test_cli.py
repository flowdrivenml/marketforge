from datetime import timezone

from marketforge.cli.arguments import request_from_args
from marketforge.cli.main import build_parser
from marketforge.models import DataType, Exchange, InstrumentType, MarketCategory


def test_plan_arguments_build_request():
    parser = build_parser()

    args = parser.parse_args(
        [
            "plan",
            "--exchange",
            "bybit",
            "--type",
            "perpetual",
            "--category",
            "linear",
            "--symbol",
            "BTCUSDT",
            "--data",
            "trade_ticks",
            "--start",
            "2026-09-01",
            "--end",
            "2026-09-02",
        ]
    )

    request = request_from_args(args)

    assert request.target.exchange == Exchange.BYBIT

    assert request.target.instrument_type == InstrumentType.PERPETUAL

    assert request.target.market_category == MarketCategory.LINEAR

    assert request.target.symbol == "BTCUSDT"

    assert request.data_type == DataType.TRADE_TICKS

    assert request.start.tzinfo == timezone.utc

    assert request.end.tzinfo == timezone.utc


def test_cli_datetime_with_offset_is_normalized_to_utc():
    parser = build_parser()

    args = parser.parse_args(
        [
            "plan",
            "--exchange",
            "bybit",
            "--type",
            "perpetual",
            "--category",
            "linear",
            "--symbol",
            "BTCUSDT",
            "--data",
            "trade_ticks",
            "--start",
            "2026-09-01T02:00:00+02:00",
            "--end",
            "2026-09-02T02:00:00+02:00",
        ]
    )

    request = request_from_args(args)

    assert request.start.hour == 0
    assert request.end.hour == 0

    assert request.start.tzinfo == timezone.utc

    assert request.end.tzinfo == timezone.utc


def test_download_uses_same_request_arguments():
    parser = build_parser()

    args = parser.parse_args(
        [
            "download",
            "--exchange",
            "bybit",
            "--type",
            "perpetual",
            "--category",
            "linear",
            "--symbol",
            "BTCUSDT",
            "--data",
            "trade_ticks",
            "--start",
            "2026-09-01",
            "--end",
            "2026-09-02",
        ]
    )

    request = request_from_args(args)

    assert args.command == "download"

    assert request.target.exchange == Exchange.BYBIT

    assert request.target.instrument_type == InstrumentType.PERPETUAL

    assert request.target.market_category == MarketCategory.LINEAR

    assert request.target.symbol == "BTCUSDT"

    assert request.data_type == DataType.TRADE_TICKS
