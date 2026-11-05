"""Shared metadata/symbol filter arguments for stored market inspection."""

import argparse


def configure_identity_filters(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--base-asset", help="Exact base asset, case-insensitive, e.g. ETH.")
    parser.add_argument(
        "--quote-asset",
        help="Exact instrument quote asset, case-insensitive; independent of ranking comparison currency.",
    )
    parser.add_argument(
        "--match",
        help="Case-insensitive glob over the full native symbol: * matches zero or more characters, ? one. Quote it in the shell: --match 'ETH*' or --match '*BTC'. Percent and underscore are literal.",
    )
