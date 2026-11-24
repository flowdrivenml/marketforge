"""Stage inventory regressions using retained PostgreSQL membership records."""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path
from uuid import uuid4

import pytest
from psycopg import AsyncConnection
from psycopg.rows import TupleRow
from psycopg.types.json import Jsonb

from forge.datasets import workspace_inventory as inventory
from forge.shared.paths import ProjectPaths

pytestmark = [pytest.mark.integration, pytest.mark.database]


@pytest.mark.asyncio
async def test_normalized_membership_rows_preserve_publications_and_distinct_files(
    database: AsyncConnection[TupleRow], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    namespace = uuid4().hex
    start = 1_793_059_200_000_000_000
    trades = {
        "exchange": "bybit",
        "instrument_type": "spot",
        "market_category": "spot",
        "instrument_id": 1,
        "symbol": "BTCUSDT",
        "kind": "trade",
        "stream_id": "BTCUSDT-trades",
        "observed_start_ns": start + 1,
        "observed_end_ns": start + 2,
    }
    usdc = {**trades, "symbol": "BTCUSDC", "instrument_id": 65, "stream_id": "BTCUSDC-trades"}
    depth = {**trades, "kind": "depth", "stream_id": "BTCUSDT-depth"}

    async def publish(
        engine: str, files: list[list[dict[str, str | int]]], generation: int
    ) -> tuple[int, list[int]]:
        paths = [f"file-{part}.parquet" for part in range(len(files))]
        publication = await (
            await database.execute(
                """INSERT INTO historical.dataset_publications
                (instrument_namespace,engine_dataset_id,operation,storage_root,manifest_path,evidence)
                VALUES (%s,%s,'normalize',%s,'manifest.json',%s) RETURNING id""",
                (
                    namespace,
                    engine,
                    str(tmp_path),
                    Jsonb(
                        {
                            "generation": generation,
                            "execution": {"execution_id": "normalize-fixture"},
                            "manifest": {
                                "integrity_status": "clean",
                                "files": [
                                    {"path": path, "partition_id": i}
                                    for i, path in enumerate(paths)
                                ],
                            },
                        }
                    ),
                ),
            )
        ).fetchone()
        assert publication is not None
        primary_files: list[int] = []
        for name, members in zip(paths, files, strict=True):
            result = await (
                await database.execute(
                    """INSERT INTO historical.datasets
                    (data_type,start_timestamp_ns,end_timestamp_ns,storage_format,storage_path,
                    status,completed_at,operation,storage_root_key,manifest_path,operation_details,
                    publication_id,primary_file,file_sha256)
                    VALUES (%s,%s,%s,'parquet',%s,'complete',now(),'normalize',%s,'manifest.json',
                    %s,%s,%s,%s) RETURNING id""",
                    (
                        "l2" if members and members[0]["kind"] == "depth" else "trade",
                        start,
                        start + 10,
                        str(tmp_path / name),
                        str(tmp_path),
                        Jsonb({"size_bytes": 100, "memberships": members}),
                        publication[0],
                        name,
                        "a" * 64,
                    ),
                )
            ).fetchone()
            assert result is not None
            primary_files.append(result[0])
        return publication[0], primary_files

    trade_id, trade_files = await publish("967", [[trades, trades], [usdc]], 1)
    depth_id, depth_files = await publish("968", [[depth], [depth]], 1)
    generation_id, _ = await publish("967-later", [[trades]], 2)
    unknown_id, _ = await publish("unresolved", [[]], 1)

    @asynccontextmanager
    async def fixture_connection(_: ProjectPaths) -> AsyncGenerator[AsyncConnection[TupleRow]]:
        yield database

    monkeypatch.setattr(inventory, "readonly", fixture_connection)
    paths = ProjectPaths(tmp_path, tmp_path)
    rows, more = await inventory.stage_page(paths, "Normalized", 0, namespace)
    assert not more and len(rows) == 5
    trades_rows = {row["symbol"]: row for row in rows if row["publication"] == trade_id}
    assert set(trades_rows) == {"BTCUSDC", "BTCUSDT"}
    assert {row["primary_files"][0] for row in trades_rows.values()} == set(trade_files)
    for row in trades_rows.values():
        assert (row["exchange"], row["instrument_type"], row["market_category"]) == (
            "bybit",
            "spot",
            "spot",
        )
        assert (row["data_type"], row["engine_publication"], row["state"], row["verification"]) == (
            "trade",
            "967",
            "Registered",
            "clean",
        )
        assert (row["file_count"], row["partitions"], row["recorded_bytes"]) == (1, 1, 100)
        assert (row["start"], row["finish"]) == (start + 1, start + 2)
    depth_row = next(row for row in rows if row["publication"] == depth_id)
    assert (depth_row["symbol"], depth_row["data_type"], depth_row["primary_files"]) == (
        "BTCUSDT",
        "depth",
        depth_files,
    )
    assert (depth_row["file_count"], depth_row["partitions"], depth_row["recorded_bytes"]) == (
        2,
        2,
        200,
    )
    assert next(row for row in rows if row["publication"] == generation_id)["generation"] == "2"
    assert next(row for row in rows if row["publication"] == unknown_id)["instrument"] is None
