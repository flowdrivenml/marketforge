"""Inventory counts against isolated PostgreSQL publication metadata."""

from pathlib import Path
from uuid import uuid4

import pytest
from psycopg import AsyncConnection
from psycopg.rows import TupleRow
from psycopg.types.json import Jsonb

from forge.datasets import inventory as subject

pytestmark = [pytest.mark.integration, pytest.mark.database]


@pytest.mark.asyncio
async def test_inventory_deduplicates_registered_files_and_canonical_streams(
    database: AsyncConnection[TupleRow], tmp_path: Path
) -> None:
    before = await subject.summarize(database)
    namespace = uuid4().hex
    exchange = "zz-inventory-" + namespace
    path = str(tmp_path)
    ids: dict[str, int] = {}
    for operation in ("normalize", "merge"):
        record = await (
            await database.execute(
                """INSERT INTO historical.dataset_publications
                (instrument_namespace,engine_dataset_id,operation,storage_root,manifest_path,evidence)
                VALUES (%s,%s,%s,%s,%s,%s) RETURNING id""",
                (
                    namespace if operation == "normalize" else namespace + "-publication",
                    operation,
                    operation,
                    path,
                    path + "/manifest",
                    Jsonb({"merge_inputs": [{"namespace": namespace}, {"namespace": namespace}]}),
                ),
            )
        ).fetchone()
        assert record
        ids[operation] = int(record[0])

    member = {
        "exchange": exchange,
        "instrument_id": 100,
        "symbol": "SHARED",
        "market_category": "spot",
        "stream_id": "stable-trades",
        "kind": "trade",
        "end_timestamp_ns": 1700000000000000000,
    }
    for operation, name, size, members, state in (
        ("normalize", "one", 1024, [member, member], "complete"),
        (
            "normalize",
            "two",
            None,
            [{**member, "kind": "depth", "stream_id": "stable-depth"}],
            "complete",
        ),
        (
            "merge",
            "three",
            2048,
            [member, {**member, "end_timestamp_ns": 1700000000000000001}],
            "complete",
        ),
        ("merge", "excluded", 99999, [{**member, "instrument_id": 432}], "failed"),
    ):
        details = {
            "rows": 3,
            "memberships": members,
            **({"size_bytes": size} if size is not None else {}),
        }
        await database.execute(
            """INSERT INTO historical.datasets (data_type,start_timestamp_ns,end_timestamp_ns,
            storage_format,storage_path,status,completed_at,operation,storage_root_key,manifest_path,
            operation_details,publication_id,primary_file,file_sha256)
            VALUES ('trade',1,2,'parquet',%s,%s,CASE WHEN %s='complete' THEN now() END,%s,%s,%s,%s,%s,%s,%s)""",
            (
                path + "/" + name,
                state,
                state,
                operation,
                path,
                path + "/manifest",
                Jsonb(details),
                ids[operation],
                name,
                "a" * 64,
            ),
        )
    after = await subject.summarize(database)
    assert after.instruments - before.instruments == 1
    assert after.streams - before.streams == 2
    assert after.trades - before.trades == after.depth - before.depth == 1
    assert (after.size or 0) - (before.size or 0) == 3072
    assert after.missing_sizes - before.missing_sizes == 1
    assert sum(s.count for s in after.stages) - sum(s.count for s in before.stages) == 2
    assert sum(s.files for s in after.stages) - sum(s.files for s in before.stages) == 3

    selected = subject.Market(namespace, exchange, "100", "SHARED", "", "spot", True, True, None)
    count = await (
        await database.execute("SELECT count(*) FROM historical.dataset_publications")
    ).fetchone()
    assert count
    # Page through retained rows with the actual bounded market SQL.
    markets: list[subject.Market] = []
    for offset in range(0, 25000, 25):
        page = await (await database.execute(subject.MARKETS, (25, offset))).fetchall()
        markets.extend(subject.market_from_row(row) for row in page)
        if len(page) < 25:
            break
    matching = [item for item in markets if item.namespace == namespace]
    assert len(matching) == 1 and matching[0].latest == 1700000000000000001

    assert selected.identity == matching[0].identity
