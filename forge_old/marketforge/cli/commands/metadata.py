from __future__ import annotations

import argparse
import json

from marketforge.catalog.repository import CatalogRepository
from marketforge.database import connect
from marketforge.metadata.sync import EXCHANGE_SYNCS, sync_exchange


def run(args: argparse.Namespace) -> int:
    """Run a metadata command."""

    if args.metadata_command == "sync":
        return _run_sync(args)

    if args.metadata_command == "list":
        return _run_list(args)

    if args.metadata_command == "formats":
        return _run_formats(args)

    if args.metadata_command == "rules":
        return _run_rules(args)

    raise ValueError(f"Unknown metadata command: {args.metadata_command}")


def _run_sync(
    args: argparse.Namespace,
) -> int:
    """Synchronize exchange instrument metadata."""

    exchanges = [args.exchange] if args.exchange is not None else list(EXCHANGE_SYNCS)

    with connect() as conn:
        repo = CatalogRepository(conn)

        for exchange in exchanges:
            print(f"{exchange}:")

            try:
                result = sync_exchange(
                    repo,
                    exchange,
                )

                conn.commit()

            except Exception:
                conn.rollback()
                raise

            for market, count in result.items():
                print(f"  {market:<12} {count}")

    return 0


def _run_list(
    args: argparse.Namespace,
) -> int:
    """List instrument metadata."""

    with connect() as conn:
        repo = CatalogRepository(conn)

        rows = repo.list_instruments(
            exchange=args.exchange,
            instrument_type=args.instrument_type,
            market_category=args.category,
            symbol=args.symbol,
            base_asset=args.base_asset,
            quote_asset=args.quote_asset,
            settlement_asset=args.settlement_asset,
            status=args.status,
            quantity_type=args.quantity_type,
            contract_value_asset=args.contract_value_asset,
            limit=args.limit,
        )

    if not rows:
        print("No instruments found.")
        return 0

    _print_instruments(rows)

    print()
    print(f"{len(rows)} instrument(s)")

    return 0


def _print_instruments(
    rows: list[dict],
) -> None:
    """Print instrument metadata as a table."""

    columns = [
        ("exchange", "EXCHANGE"),
        ("symbol", "SYMBOL"),
        ("instrument_type", "TYPE"),
        ("market_category", "CATEGORY"),
        ("base_asset", "BASE"),
        ("quote_asset", "QUOTE"),
        ("settlement_asset", "SETTLE"),
        ("quantity_type", "QUANTITY"),
        ("contract_value", "CONTRACT"),
        ("contract_value_asset", "VALUE ASSET"),
    ]

    values = [[_display(row[key]) for key, _ in columns] for row in rows]

    widths = [
        max(
            len(header),
            max(len(row[index]) for row in values),
        )
        for index, (_, header) in enumerate(columns)
    ]

    print(
        "  ".join(
            header.ljust(widths[index]) for index, (_, header) in enumerate(columns)
        )
    )

    print("  ".join("-" * width for width in widths))

    for row in values:
        print("  ".join(value.ljust(widths[index]) for index, value in enumerate(row)))


def _display(value: object) -> str:
    """Convert database values to CLI display strings."""

    if value is None:
        return "-"

    return str(value)


def _run_formats(
    args: argparse.Namespace,
) -> int:
    """List raw-format specifications."""

    with connect() as conn:
        repo = CatalogRepository(conn)

        rows = repo.list_raw_formats(
            exchange=args.exchange,
            format_code=args.format_code,
            dataset=args.dataset,
            instrument_type=args.instrument_type,
            market_category=args.category,
        )

    if not rows:
        print("No raw formats found.")
        return 0

    _print_raw_formats(rows)

    print()
    print(f"{len(rows)} raw format(s)")

    return 0


def _print_raw_formats(
    rows: list[dict],
) -> None:
    """Print raw-format specifications as a table."""

    columns = [
        ("exchange", "EXCHANGE"),
        ("format_code", "FORMAT"),
        ("dataset", "DATASET"),
        ("instrument_type", "TYPE"),
        ("market_category", "CATEGORY"),
        ("container_format", "CONTAINER"),
        ("compression", "COMPRESSION"),
        ("record_format", "RECORDS"),
    ]

    values = [[_display(row[key]) for key, _ in columns] for row in rows]

    widths = [
        max(
            len(header),
            max(len(row[index]) for row in values),
        )
        for index, (_, header) in enumerate(columns)
    ]

    print(
        "  ".join(
            header.ljust(widths[index]) for index, (_, header) in enumerate(columns)
        )
    )

    print("  ".join("-" * width for width in widths))

    for row in values:
        print("  ".join(value.ljust(widths[index]) for index, value in enumerate(row)))


def _run_rules(
    args: argparse.Namespace,
) -> int:
    """List normalization rules."""

    with connect() as conn:
        repo = CatalogRepository(conn)

        rows = repo.list_normalization_rules(
            exchange=args.exchange,
            format_code=args.format_code,
            dataset=args.dataset,
            instrument_type=args.instrument_type,
            market_category=args.category,
            target_schema=args.target_schema,
        )

    if not rows:
        print("No normalization rules found.")
        return 0

    _print_normalization_rules(
        rows,
        show_json=args.show_json,
    )

    print()
    print(f"{len(rows)} normalization rule(s)")

    return 0


def _print_normalization_rules(
    rows: list[dict],
    *,
    show_json: bool,
) -> None:
    """Print normalization rules."""

    columns = [
        ("exchange", "EXCHANGE"),
        ("format_code", "FORMAT"),
        ("dataset", "DATASET"),
        ("instrument_type", "TYPE"),
        ("market_category", "CATEGORY"),
        ("target_schema", "TARGET"),
    ]

    values = [[_display(row[key]) for key, _ in columns] for row in rows]

    widths = [
        max(
            len(header),
            max(len(row[index]) for row in values),
        )
        for index, (_, header) in enumerate(columns)
    ]

    print(
        "  ".join(
            header.ljust(widths[index]) for index, (_, header) in enumerate(columns)
        )
    )

    print("  ".join("-" * width for width in widths))

    for row in values:
        print("  ".join(value.ljust(widths[index]) for index, value in enumerate(row)))

    if not show_json:
        return

    for row in rows:
        print()
        print(
            f"{row['exchange']} / " f"{row['format_code']} / " f"{row['target_schema']}"
        )

        print(
            json.dumps(
                row["rules_json"],
                indent=2,
                sort_keys=True,
                default=str,
            )
        )
