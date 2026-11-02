from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Iterator

import psycopg
from psycopg import Connection
from psycopg.rows import dict_row

DATABASE_URL_ENV = "MARKETFORGE_DATABASE_URL"


def get_database_url() -> str:
    """
    Return the MarketForge PostgreSQL connection URL.

    The URL must be provided through MARKETFORGE_DATABASE_URL.
    """
    database_url = os.getenv(DATABASE_URL_ENV)

    if not database_url:
        raise RuntimeError(
            f"{DATABASE_URL_ENV} is not set. "
            "Configure the PostgreSQL connection before using the database."
        )

    return database_url


def connect(
    database_url: str | None = None,
) -> Connection:
    """
    Open a PostgreSQL connection.

    dict_row makes query results accessible by column name:

        row["symbol"]

    rather than only by positional index.
    """
    return psycopg.connect(
        database_url or get_database_url(),
        row_factory=dict_row,
    )


@contextmanager
def transaction(
    conn: Connection,
) -> Iterator[Connection]:
    """
    Execute operations inside an explicit transaction.

    Commit on success and roll back on failure.
    """
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
