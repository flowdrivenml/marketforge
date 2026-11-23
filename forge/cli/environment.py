"""Shared CLI options for database selection."""

import argparse


def add_database_options(parser: argparse.ArgumentParser, *, required: bool = False) -> None:
    group = parser.add_mutually_exclusive_group(required=required)
    group.add_argument(
        "--testing",
        action="store_true",
        default=argparse.SUPPRESS,
        help="Select the testing database.",
    )
    group.add_argument(
        "--production",
        action="store_true",
        default=argparse.SUPPRESS,
        help="Select the production database (default).",
    )
