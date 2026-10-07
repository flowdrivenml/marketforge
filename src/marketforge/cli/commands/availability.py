from __future__ import annotations

import argparse

from marketforge.acquisition.client import MarketForgeClient
from marketforge.cli.arguments import request_from_args


def run(
    args: argparse.Namespace,
) -> int:
    """Discover historical availability for an instrument."""

    request = request_from_args(args)

    with MarketForgeClient(data_root=args.data_root) as client:
        availability = client.availability(
            target=request.target,
            data_type=request.data_type,
            start=request.start,
            end=request.end,
        )

    print()
    print("MarketForge Historical Availability")
    print("=" * 72)

    print(f"Exchange : {request.target.exchange.value}")
    print(f"Symbol   : {request.target.symbol}")
    print(f"Type     : {request.target.instrument_type.value}")
    print(f"Category : {request.target.market_category.value}")
    print(f"Data     : {request.data_type.value}")

    if availability.file_count == 0:
        print("Available : no")
        print("Files     : 0")
        return 0

    print("Available : yes")
    print(f"Files     : {availability.file_count}")

    if availability.start is not None:
        print(f"First     : " f"{availability.start.isoformat()}")

    if availability.end is not None:
        print(f"Last      : " f"{availability.end.isoformat()}")

    return 0
