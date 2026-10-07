from __future__ import annotations

import argparse
from datetime import datetime, timezone

from marketforge.database import connect
from marketforge.processing.datasets import DatasetRepository


def run(
    args: argparse.Namespace,
) -> int:
    """List canonical MarketForge datasets."""

    with connect() as conn:
        repository = DatasetRepository(conn)

        datasets = repository.list_resolved(
            exchange=args.exchange,
            symbol=args.symbol,
            data_type=args.data_type,
            status=args.status,
        )

    if not datasets:
        print("No datasets found.")
        return 0

    print(
        f"{'ID':<7} "
        f"{'EXCHANGE':<10} "
        f"{'SYMBOL':<24} "
        f"{'DATA':<10} "
        f"{'START':<12} "
        f"{'END':<12} "
        f"{'STATUS':<10}"
    )

    print("-" * 92)

    for dataset in datasets:
        print(
            f"{dataset.id:<7} "
            f"{dataset.exchange or '-':<10} "
            f"{dataset.symbol or 'MERGED':<24} "
            f"{dataset.data_type:<10} "
            f"{_date_ns(dataset.start_timestamp_ns):<12} "
            f"{_date_ns(dataset.end_timestamp_ns):<12} "
            f"{dataset.status:<10}"
        )

    print()
    print(f"{len(datasets)} dataset(s)")

    return 0


def _date_ns(
    value: int,
) -> str:
    return datetime.fromtimestamp(
        value / 1_000_000_000,
        tz=timezone.utc,
    ).strftime("%Y-%m-%d")
