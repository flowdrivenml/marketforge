from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
from pathlib import Path

from marketforge.models import (
    AcquisitionRequest,
    BaseCoin,
    DataType,
    Exchange,
    Instrument,
    InstrumentType,
    MarketCategory,
)


def parse_date(
    value: str,
) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Invalid date: {value}") from exc


def add_archives_arguments(
    parser: argparse.ArgumentParser,
) -> None:
    parser.add_argument(
        "archives_command",
        nargs="?",
        choices=[
            "delete",
        ],
        help="Optional archive operation.",
    )

    parser.add_argument("--exchange")

    parser.add_argument(
        "--type",
        dest="instrument_type",
    )

    parser.add_argument(
        "--category",
    )

    parser.add_argument(
        "--data",
        dest="data_type",
    )

    parser.add_argument("--symbol")

    parser.add_argument(
        "--start",
        type=parse_date,
    )

    parser.add_argument(
        "--end",
        type=parse_date,
    )

    parser.add_argument(
        "--data-root",
        type=Path,
        default=Path("data"),
    )

    parser.add_argument(
        "--yes",
        action="store_true",
        help="Delete selected archives without confirmation.",
    )


def add_archive_delete_arguments(
    parser: argparse.ArgumentParser,
) -> None:
    """
    Add selectors for deleting downloaded raw archives.
    """

    add_archives_arguments(parser)

    parser.add_argument(
        "--yes",
        action="store_true",
        help="Delete selected archives without confirmation.",
    )


def add_datasets_arguments(
    parser: argparse.ArgumentParser,
) -> None:
    parser.add_argument("--exchange")
    parser.add_argument("--symbol")

    parser.add_argument(
        "--data",
        dest="data_type",
        choices=[
            "trade",
            "l2",
            "trade_l2",
        ],
    )

    parser.add_argument(
        "--status",
        choices=[
            "pending",
            "processing",
            "complete",
            "failed",
        ],
    )


def add_process_arguments(
    parser: argparse.ArgumentParser,
) -> None:
    parser.add_argument(
        "--exchange",
        required=True,
    )

    parser.add_argument(
        "--type",
        dest="instrument_type",
        required=True,
    )

    parser.add_argument(
        "--category",
        required=True,
    )

    parser.add_argument(
        "--symbol",
        required=True,
    )

    parser.add_argument(
        "--data",
        dest="data_type",
        required=True,
        choices=[
            "trade_ticks",
            "order_book_l2",
        ],
    )

    parser.add_argument(
        "--dataset",
        required=True,
        choices=[
            "trade",
            "l2",
        ],
    )

    parser.add_argument(
        "--start",
        type=parse_date,
    )

    parser.add_argument(
        "--end",
        type=parse_date,
    )

    parser.add_argument(
        "--profile",
        default="default",
    )

    parser.add_argument(
        "--data-root",
        type=Path,
        default=Path("data"),
    )


def date_to_ns(
    value: date | None,
) -> int | None:
    if value is None:
        return None

    timestamp = datetime(
        value.year,
        value.month,
        value.day,
        tzinfo=timezone.utc,
    )

    return int(timestamp.timestamp() * 1_000_000_000)


def add_merge_arguments(
    parser: argparse.ArgumentParser,
) -> None:
    parser.add_argument(
        "dataset_ids",
        nargs="+",
        type=int,
    )

    parser.add_argument(
        "--start",
        type=parse_date,
    )

    parser.add_argument(
        "--end",
        type=parse_date,
    )

    parser.add_argument(
        "--profile",
        default="default",
    )

    parser.add_argument(
        "--data-root",
        type=Path,
        default=Path("data"),
    )


def add_request_arguments(
    parser: argparse.ArgumentParser,
) -> None:
    """Add arguments shared by acquisition commands."""

    parser.add_argument(
        "--exchange",
        required=True,
        choices=[exchange.value for exchange in Exchange],
    )

    parser.add_argument(
        "--type",
        dest="instrument_type",
        required=True,
        choices=[instrument_type.value for instrument_type in InstrumentType],
    )

    parser.add_argument(
        "--category",
        required=True,
        choices=[category.value for category in MarketCategory],
    )

    target = parser.add_mutually_exclusive_group(
        required=True,
    )

    target.add_argument(
        "--symbol",
        help="Specific exchange instrument symbol.",
    )

    target.add_argument(
        "--base-coin",
        dest="base_coin",
        help=("Base-coin target for grouped datasets, " "for example BTC options."),
    )

    parser.add_argument(
        "--family",
        default=None,
        help=("Optional exchange instrument family, " "for example BTC-USDT on OKX."),
    )

    parser.add_argument(
        "--data",
        dest="data_type",
        required=True,
        choices=[data_type.value for data_type in DataType],
    )

    parser.add_argument(
        "--start",
        required=True,
        help="Inclusive UTC start date/time.",
    )

    parser.add_argument(
        "--end",
        required=True,
        help="Exclusive UTC end date/time.",
    )

    parser.add_argument(
        "--data-root",
        type=Path,
        default=Path("data"),
        help="MarketForge data root.",
    )


def request_from_args(
    args: argparse.Namespace,
) -> AcquisitionRequest:
    """Build an AcquisitionRequest from parsed CLI arguments."""

    exchange = Exchange(args.exchange)

    market_category = MarketCategory(args.category)

    if args.base_coin is not None:
        target = BaseCoin(
            exchange=exchange,
            market_category=market_category,
            symbol=args.base_coin,
        )

    else:
        target = Instrument(
            exchange=exchange,
            instrument_type=InstrumentType(args.instrument_type),
            market_category=market_category,
            symbol=args.symbol,
            family=args.family,
        )

    return AcquisitionRequest(
        target=target,
        data_type=DataType(args.data_type),
        start=parse_datetime(args.start),
        end=parse_datetime(args.end),
    )


def parse_datetime(
    value: str,
) -> datetime:
    """Parse a CLI date/time and normalize it to UTC."""

    try:
        parsed = datetime.fromisoformat(
            value.replace(
                "Z",
                "+00:00",
            )
        )
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Invalid date/time: {value}") from exc

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)

    return parsed.astimezone(timezone.utc)


def add_instrument_arguments(
    parser: argparse.ArgumentParser,
) -> None:
    """Add arguments for catalog market discovery."""

    parser.add_argument(
        "--exchange",
        choices=[
            "bybit",
            "binance",
            "okx",
            "bitget",
            "gateio",
        ],
        help="Filter by exchange.",
    )

    parser.add_argument(
        "--type",
        dest="instrument_type",
        choices=[
            "spot",
            "perpetual",
            "future",
        ],
        help="Filter by instrument type.",
    )

    parser.add_argument(
        "--market",
        dest="market_category",
        choices=[
            "spot",
            "linear",
            "inverse",
        ],
        help="Filter by market category.",
    )

    parser.add_argument(
        "--symbol",
        help="Filter instruments by symbol.",
    )

    parser.add_argument(
        "--sort",
        choices=[
            "turnover",
            "open_interest",
            "symbol",
        ],
        default="turnover",
        help="Sort returned instruments.",
    )

    parser.add_argument(
        "--details",
        action="store_true",
        help="Display detailed market metrics.",
    )

    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Refresh current market metrics before displaying instruments.",
    )


def add_metadata_arguments(
    parser: argparse.ArgumentParser,
) -> None:
    """Add instrument-metadata synchronization arguments."""

    parser.add_argument(
        "--exchange",
        choices=[
            "bybit",
            "binance",
            "okx",
            "bitget",
            "gateio",
        ],
        help=(
            "Synchronize only one exchange. " "If omitted, synchronize all exchanges."
        ),
    )


def add_metadata_sync_arguments(
    parser: argparse.ArgumentParser,
) -> None:
    """Add metadata synchronization arguments."""

    parser.add_argument(
        "--exchange",
        choices=[
            "bybit",
            "binance",
            "okx",
            "bitget",
            "gateio",
        ],
        help=(
            "Synchronize only one exchange. " "If omitted, synchronize all exchanges."
        ),
    )


def add_metadata_list_arguments(
    parser: argparse.ArgumentParser,
) -> None:
    """Add metadata listing arguments."""

    parser.add_argument(
        "--exchange",
        choices=[
            "bybit",
            "binance",
            "okx",
            "bitget",
            "gateio",
        ],
        help="Filter by exchange.",
    )

    parser.add_argument(
        "--type",
        dest="instrument_type",
        choices=[
            "spot",
            "perpetual",
            "future",
            "option",
        ],
        help="Filter by instrument type.",
    )

    parser.add_argument(
        "--category",
        choices=[
            "spot",
            "linear",
            "inverse",
            "option",
        ],
        help="Filter by market category.",
    )

    parser.add_argument(
        "--symbol",
        help="Filter by exact exchange symbol.",
    )

    parser.add_argument(
        "--base",
        dest="base_asset",
        help="Filter by base asset.",
    )

    parser.add_argument(
        "--quote",
        dest="quote_asset",
        help="Filter by quote asset.",
    )

    parser.add_argument(
        "--settlement",
        dest="settlement_asset",
        help="Filter by settlement asset.",
    )

    parser.add_argument(
        "--status",
        help="Filter by exchange instrument status.",
    )

    parser.add_argument(
        "--quantity-type",
        choices=[
            "base",
            "quote",
            "contracts",
        ],
        help="Filter by canonical quantity type.",
    )

    parser.add_argument(
        "--contract-value-asset",
        help="Filter by contract-value asset.",
    )

    parser.add_argument(
        "--limit",
        type=int,
        help="Maximum number of instruments to display.",
    )


def add_metadata_formats_arguments(
    parser: argparse.ArgumentParser,
) -> None:
    """Add raw-format listing arguments."""

    parser.add_argument(
        "--exchange",
        choices=[
            "bybit",
            "binance",
            "okx",
            "bitget",
            "gateio",
        ],
        help="Filter by exchange.",
    )

    parser.add_argument(
        "--format",
        dest="format_code",
        help="Filter by raw-format code.",
    )

    parser.add_argument(
        "--dataset",
        choices=[
            "trade",
            "l2",
        ],
        help="Filter by dataset.",
    )

    parser.add_argument(
        "--type",
        dest="instrument_type",
        choices=[
            "spot",
            "perpetual",
            "future",
            "option",
        ],
        help="Filter by instrument type.",
    )

    parser.add_argument(
        "--category",
        choices=[
            "spot",
            "linear",
            "inverse",
            "option",
        ],
        help="Filter by market category.",
    )


def add_metadata_rules_arguments(
    parser: argparse.ArgumentParser,
) -> None:
    """Add normalization-rule listing arguments."""

    parser.add_argument(
        "--exchange",
        choices=[
            "bybit",
            "binance",
            "okx",
            "bitget",
            "gateio",
        ],
        help="Filter by exchange.",
    )

    parser.add_argument(
        "--format",
        dest="format_code",
        help="Filter by raw-format code.",
    )

    parser.add_argument(
        "--dataset",
        choices=[
            "trade",
            "l2",
        ],
        help="Filter by dataset.",
    )

    parser.add_argument(
        "--type",
        dest="instrument_type",
        choices=[
            "spot",
            "perpetual",
            "future",
            "option",
        ],
        help="Filter by instrument type.",
    )

    parser.add_argument(
        "--category",
        choices=[
            "spot",
            "linear",
            "inverse",
            "option",
        ],
        help="Filter by market category.",
    )

    parser.add_argument(
        "--target",
        dest="target_schema",
        choices=[
            "trade",
            "l2_snapshot",
            "l2_update",
        ],
        help="Filter by canonical target schema.",
    )

    parser.add_argument(
        "--show-json",
        action="store_true",
        help="Display normalization rules JSON.",
    )
