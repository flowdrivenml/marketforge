from __future__ import annotations

import argparse
from typing import Sequence

from marketforge.cli.arguments import (
    add_archives_arguments,
    add_datasets_arguments,
    add_instrument_arguments,
    add_merge_arguments,
    add_metadata_formats_arguments,
    add_metadata_list_arguments,
    add_metadata_rules_arguments,
    add_metadata_sync_arguments,
    add_process_arguments,
    add_request_arguments,
)
from marketforge.cli.commands import (
    archives,
    availability,
    config,
    datasets,
    download,
    instruments,
    merge,
    metadata,
    plan,
    process,
)
from marketforge.cli.database import add_database_parser


def main(
    argv: Sequence[str] | None = None,
) -> int:
    """Run the MarketForge command-line interface."""

    parser = build_parser()

    args = parser.parse_args(argv)

    if hasattr(args, "handler"):
        result = args.handler(args)

        return 0 if result is None else result

    if args.command == "plan":
        return plan.run(args)

    if args.command == "download":
        return download.run(args)

    if args.command == "instruments":
        return instruments.run(args)

    if args.command == "availability":
        return availability.run(args)

    if args.command == "metadata":
        return metadata.run(args)

    if args.command == "archives":
        return archives.run(args)

    if args.command == "process":
        return process.run(args)

    if args.command == "datasets":
        return datasets.run(args)

    if args.command == "merge":
        return merge.run(args)

    if args.command == "config":
        return config.run(args)

    parser.error(f"Unknown command: {args.command}")

    return 2


def build_parser() -> argparse.ArgumentParser:
    """Build the MarketForge CLI parser."""

    parser = argparse.ArgumentParser(
        prog="marketforge",
        description=("Historical market-microstructure " "data acquisition."),
    )

    subparsers = parser.add_subparsers(
        dest="command",
        required=True,
    )

    # Database

    add_database_parser(subparsers)

    # Plan

    plan_parser = subparsers.add_parser(
        "plan",
        help="Plan a historical-data acquisition.",
    )

    add_request_arguments(plan_parser)

    # Download

    download_parser = subparsers.add_parser(
        "download",
        help="Download historical-data archives.",
    )

    add_request_arguments(download_parser)

    # Instruments

    instruments_parser = subparsers.add_parser(
        "instruments",
        help="Discover exchange instruments.",
    )

    add_instrument_arguments(instruments_parser)

    # Availability

    availability_parser = subparsers.add_parser(
        "availability",
        help="Check historical data availability.",
    )

    add_request_arguments(availability_parser)

    # Metadata

    metadata_parser = subparsers.add_parser(
        "metadata",
        help="Manage exchange metadata.",
    )

    metadata_subparsers = metadata_parser.add_subparsers(
        dest="metadata_command",
        required=True,
    )

    # Metadata: sync

    metadata_sync_parser = metadata_subparsers.add_parser(
        "sync",
        help="Synchronize exchange instrument metadata.",
    )

    add_metadata_sync_arguments(metadata_sync_parser)

    # Metadata: list

    metadata_list_parser = metadata_subparsers.add_parser(
        "list",
        help="List instrument metadata.",
    )

    add_metadata_list_arguments(metadata_list_parser)

    # Metadata: formats

    metadata_formats_parser = metadata_subparsers.add_parser(
        "formats",
        help="List known raw data formats.",
    )

    add_metadata_formats_arguments(metadata_formats_parser)

    # Metadata: rules

    metadata_rules_parser = metadata_subparsers.add_parser(
        "rules",
        help="List normalization rules.",
    )

    add_metadata_rules_arguments(metadata_rules_parser)

    # Archives

    archives_parser = subparsers.add_parser(
        "archives",
        help="Inspect downloaded raw archives.",
    )

    add_archives_arguments(
        archives_parser,
    )

    # Process

    process_parser = subparsers.add_parser(
        "process",
        help="Generate an offline processing job.",
    )

    add_process_arguments(
        process_parser,
    )

    # Datasets

    datasets_parser = subparsers.add_parser(
        "datasets",
        help="Inspect canonical datasets.",
    )

    add_datasets_arguments(
        datasets_parser,
    )

    # Merge

    merge_parser = subparsers.add_parser(
        "merge",
        help="Generate a canonical dataset merge job.",
    )

    add_merge_arguments(
        merge_parser,
    )

    # Config

    config_parser = subparsers.add_parser(
        "config",
        help="Manage processing configuration.",
    )

    config_subparsers = config_parser.add_subparsers(
        dest="config_command",
        required=True,
    )

    config_subparsers.add_parser(
        "profiles",
        help="List processing profiles.",
    )

    profile_parser = config_subparsers.add_parser(
        "profile",
        help="Show one processing profile.",
    )

    profile_parser.add_argument(
        "profile_name",
    )

    return parser
