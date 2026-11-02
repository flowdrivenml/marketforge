from __future__ import annotations

import argparse

from marketforge.cli.ui.instruments import print_instruments
from marketforge.database import connect
from marketforge.market_metrics import MarketMetricsService


def run(
    args: argparse.Namespace,
) -> int:
    """Inspect catalog instruments and current market activity."""

    with connect() as conn:
        service = MarketMetricsService(conn)

        if args.refresh:
            result = service.refresh(
                exchange=args.exchange,
            )

            conn.commit()

            for exchange, count in result.items():
                print(f"{exchange}: " f"{count:,} metric row(s)")

        instruments = service.list_instruments(
            exchange=args.exchange,
            instrument_type=args.instrument_type,
            market_category=args.market_category,
            symbol=args.symbol,
            sort=args.sort,
        )

    print_instruments(
        instruments,
        details=args.details,
    )

    return 0
