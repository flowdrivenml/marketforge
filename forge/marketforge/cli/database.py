from __future__ import annotations

import argparse

from marketforge.database import connect, initialize_schema, seed_database


def add_database_parser(
    subparsers: argparse._SubParsersAction,
) -> None:
    database_parser = subparsers.add_parser(
        "database",
        help="Manage the MarketForge database.",
    )

    database_subparsers = database_parser.add_subparsers(
        dest="database_command",
        required=True,
    )

    init_parser = database_subparsers.add_parser(
        "init",
        help="Initialize the PostgreSQL schema and seed static data.",
    )

    init_parser.set_defaults(
        handler=run_database_init,
    )


def run_database_init(
    args: argparse.Namespace,
) -> None:
    with connect() as conn:
        initialize_schema(conn)
        seed_database(conn)
        conn.commit()

    print("MarketForge database initialized.")
