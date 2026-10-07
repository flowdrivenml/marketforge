from __future__ import annotations

import argparse

from marketforge.processing.archives import (
    delete_raw_archives,
    scan_raw_archives,
    select_raw_archives,
)


def run(
    args: argparse.Namespace,
) -> int:
    """Inspect or delete locally downloaded raw archives."""

    if getattr(args, "archives_command", None) == "delete":
        return _delete(args)

    return _list(args)


def _list(
    args: argparse.Namespace,
) -> int:
    archives = scan_raw_archives(
        data_root=args.data_root,
    )

    selected = select_raw_archives(
        archives,
        exchange=args.exchange,
        instrument_type=args.instrument_type,
        market_category=args.category,
        data_type=args.data_type,
        symbol=args.symbol,
        start=args.start,
        end=args.end,
    )

    if not selected:
        print("No raw archives found.")
        return 0

    _print_archives(selected)

    print()
    print(f"{len(selected)} archive(s)")

    return 0


def _delete(
    args: argparse.Namespace,
) -> int:
    _require_delete_filter(args)

    archives = scan_raw_archives(
        data_root=args.data_root,
    )

    selected = select_raw_archives(
        archives,
        exchange=args.exchange,
        instrument_type=args.instrument_type,
        market_category=args.category,
        data_type=args.data_type,
        symbol=args.symbol,
        start=args.start,
        end=args.end,
    )

    if not selected:
        print("No raw archives matched the selection.")
        return 0

    print("Archives selected for deletion")
    print("=" * 72)
    print()

    _print_archives(selected)

    print()
    print(f"{len(selected)} archive(s)")

    if not args.yes:
        print()

        answer = input("Delete these archives? [y/N]: ")

        if answer.strip().casefold() not in {
            "y",
            "yes",
        }:
            print("Deletion cancelled.")
            return 0

    deleted = delete_raw_archives(selected)

    print()
    print(f"Deleted {len(deleted)} archive(s).")

    return 0


def _require_delete_filter(
    args: argparse.Namespace,
) -> None:
    selectors = (
        args.exchange,
        args.instrument_type,
        args.category,
        args.data_type,
        args.symbol,
        args.start,
        args.end,
    )

    if not any(value is not None for value in selectors):
        raise ValueError("Archive deletion requires at least one filter")


def _print_archives(
    archives,
) -> None:
    print(
        f"{'EXCHANGE':<10} "
        f"{'TYPE':<12} "
        f"{'MARKET':<10} "
        f"{'DATA':<16} "
        f"{'SYMBOL':<24} "
        f"FILE"
    )

    print("-" * 110)

    for archive in archives:
        print(
            f"{archive.exchange:<10} "
            f"{archive.instrument_type:<12} "
            f"{archive.market_category:<10} "
            f"{archive.data_type:<16} "
            f"{archive.symbol:<24} "
            f"{archive.filename}"
        )
