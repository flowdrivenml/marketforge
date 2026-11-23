"""Managed acquisition CLI fixtures and isolated testing-database cleanup."""

from collections.abc import Iterator
from pathlib import Path

import pytest

_VALID_DOCUMENT = """
version = 1

[interval]
start = 2026-09-01T00:00:00Z
end = 2026-09-03T00:00:00Z

[[requests]]
exchange = "bybit"
instrument_type = "spot"
market_category = "spot"
data_type = "trades"

[requests.scope]
kind = "instrument"
identifier = "BTCUSDT"
"""


@pytest.fixture
def workspace(tmp_path: Path) -> Path:
    root = tmp_path / "workspace"
    (root / "forge").mkdir(parents=True)
    (root / "engine").mkdir()
    (root / "forge" / "pyproject.toml").write_text(
        '[project]\nname = "marketforge"\n',
        encoding="utf-8",
    )
    return root


@pytest.fixture
def request_file(workspace: Path) -> Path:
    source = workspace / "data/.config/workflows/data_engineering/acquisition/request.toml"
    source.parent.mkdir(parents=True)
    source.write_text(_VALID_DOCUMENT, encoding="utf-8")
    return source


# These fixtures are opt-in: configuration-only tests need no database.
@pytest.fixture
def isolated_acquisition(request_file: Path) -> Iterator[str]:
    import asyncio
    import os
    from uuid import uuid4

    from forge.shared.database.testing import testing_connection

    dsn = os.environ.get("TEST_DATABASE_KEY")
    if not dsn:
        pytest.skip("TEST_DATABASE_KEY is not set.")
    symbol = "MF_TEST_" + uuid4().hex
    request_file.write_text(request_file.read_text().replace("BTCUSDT", symbol))

    async def cleanup() -> None:
        async with testing_connection(dsn) as connection, connection.transaction():
            prefix = str(request_file.parents[5] / "data/runs") + "/"
            # Only execution fixtures created under this test workspace.
            for table in ("execution_resources", "execution_attempts", "execution_runs"):
                from psycopg import sql

                runs = sql.SQL(
                    "(SELECT run_id FROM historical.execution_runs WHERE left(artifact_root,length(%s))=%s)"
                )
                clause = (
                    sql.SQL(
                        "attempt_id IN (SELECT attempt_id FROM historical.execution_attempts WHERE run_id IN {})"
                    ).format(runs)
                    if table == "execution_resources"
                    else sql.SQL("run_id IN {}").format(runs)
                )
                await connection.execute(
                    sql.SQL("DELETE FROM historical.{} WHERE {}").format(
                        sql.Identifier(table), clause
                    ),
                    (prefix, prefix),
                )
            await connection.execute(
                """DELETE FROM historical.discovery_scans
                WHERE scope_json->>'scope_identifier' = %s""",
                (symbol,),
            )
            await connection.execute(
                """DELETE FROM historical.local_archives
                WHERE remote_archive_id IN (SELECT id FROM historical.remote_archives
                    WHERE scope_json->>'scope_identifier' = %s)""",
                (symbol,),
            )
            await connection.execute(
                """DELETE FROM historical.remote_archives
                WHERE scope_json->>'scope_identifier' = %s""",
                (symbol,),
            )

    try:
        yield symbol
    finally:
        asyncio.run(cleanup())
