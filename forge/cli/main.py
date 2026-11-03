"""MarketForge command-line entry point."""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from importlib.metadata import PackageNotFoundError, version


def _application_version() -> str:
    """Return the installed application version."""
    try:
        return version("marketforge")
    except PackageNotFoundError:
        return "uninstalled"


def build_parser() -> argparse.ArgumentParser:
    """Build the root command parser."""
    parser = argparse.ArgumentParser(
        prog="marketforge",
        description=(
            "Historical market-data acquisition and Rust engine orchestration."
        ),
        allow_abbrev=False,
    )

    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {_application_version()}",
    )

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the command-line interface."""
    parser = build_parser()
    parser.parse_args(argv)
    parser.print_help()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
