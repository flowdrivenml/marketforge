"""Exercise discovery and inspection using isolated registry records."""

import argparse
from collections.abc import AsyncGenerator, Iterator, Sequence
from contextlib import asynccontextmanager
from decimal import Decimal
from pathlib import Path
from typing import Any, LiteralString, Never
from uuid import uuid4

import pytest
from psycopg import AsyncConnection
from psycopg.rows import TupleRow
from psycopg.types.json import Jsonb

from forge.cli.commands import datasets
from forge.datasets import query
from forge.shared.paths import ProjectPaths

pytestmark = [pytest.mark.integration, pytest.mark.database]
START = 1793059200123456789
END = START + 2_000_000_000


@pytest.mark.asyncio
async def test_file_search_and_inspection(
    database: AsyncConnection[TupleRow],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    parser = argparse.ArgumentParser()
    datasets.configure(parser)

    @asynccontextmanager
    async def connection(_: str | None) -> AsyncGenerator[AsyncConnection[TupleRow]]:
        yield database

    def unexpected(*_args: object, **_kwargs: object) -> Never:
        pytest.fail("Discovery and inspection must not hash artifacts")

    monkeypatch.setattr(query, "testing_connection", connection)

    def dsn(_: Path | None) -> str:
        return "fixture"

    monkeypatch.setattr(query, "testing_dsn", dsn)
    monkeypatch.setattr(query, "checksum", unexpected)
    paths = ProjectPaths(tmp_path, tmp_path / "data")

    async def invoke(*args: str, as_json: bool = True) -> dict[str, Any]:
        options = parser.parse_args([*args, "--testing"])
        return await query.browse(paths, datasets.request_from_args(options), detailed=not as_json)

    async def insert(sql: LiteralString, values: tuple[object, ...]) -> int:
        row = await (await database.execute(sql, values)).fetchone()
        assert row
        return int(row[0])

    token = uuid4().hex
    engine = str(int(token[:15], 16))
    root = str(tmp_path)
    manifest = str(tmp_path / "manifest.json")
    sha = "a" * 64
    publication = await insert(
        """INSERT INTO historical.dataset_publications
        (instrument_namespace,engine_dataset_id,operation,storage_root,manifest_path,evidence)
        VALUES(%s,%s,'normalize',%s,%s,%s) RETURNING id""",
        (
            token,
            engine,
            root,
            manifest,
            Jsonb(
                {
                    "selected_raw_inputs": [[i, sha] for i in range(1, 301)],
                    "selected_dataset_inputs": [],
                    "manifest": {
                        "streams": [
                            {"symbol": symbol, "kind": kind, "stream_id": f"{symbol}.{kind}"}
                            for symbol in ("BTCUSDT", "BTCUSDC", "ETHUSDT", "ETHBTC")
                            for kind in ("trade", "depth")
                        ]
                    },
                }
            ),
        ),
    )
    bybit: dict[str, str | int] = {
        "exchange": "bybit",
        "symbol": "BTCUSDT",
        "instrument_id": 141,
        "instrument_type": "perpetual",
        "market_category": "linear",
        "start_timestamp_ns": START,
        "end_timestamp_ns": END,
    }
    okx: dict[str, str | int] = {
        "exchange": "okx",
        "symbol": "BTC-USD_UM-261225",
        "instrument_id": 142,
        "instrument_type": "future",
        "market_category": "linear",
        "start_timestamp_ns": END,
        "end_timestamp_ns": END,
        "stream_id": "okx.family:BTC-USD_UM.trades.instrument:BTC-USD_UM-261225",
    }

    async def add(primary: str, kind: str, members: list[dict[str, str | int]]) -> int:
        return await insert(
            """INSERT INTO historical.datasets(data_type,start_timestamp_ns,end_timestamp_ns,
            storage_format,storage_path,status,completed_at,operation,storage_root_key,manifest_path,
            operation_details,publication_id,primary_file,file_sha256)
            VALUES(%s,%s,%s,'parquet',%s,'complete',now(),'normalize',%s,%s,%s,%s,%s,%s)
            RETURNING id""",
            (
                kind,
                START,
                END,
                str(tmp_path / primary),
                root,
                manifest,
                Jsonb(
                    {
                        "rows": 3,
                        "memberships": members,
                        "membership_scope": "file",
                    }
                ),
                publication,
                primary,
                sha,
            ),
        )

    trade = await add("trade.parquet", "trade", [bybit, okx])
    depth = await add("depth.parquet", "l2", [{**bybit, "market_category": "inverse"}])

    page = await query.publications(paths, identity=publication)
    assert len(page["publications"]) == 1
    retained = page["publications"][0]
    assert retained["registry_publication_id"] == publication
    assert retained["engine_dataset_id"] == engine
    assert {item["registry_primary_file_id"] for item in retained["primary_files"]} == {
        trade,
        depth,
    }
    assert len({item["symbol"] for item in retained["streams"]}) == 4
    assert len(retained["streams"]) == 8

    async def search(*args: str, action: str = "find") -> tuple[list[int], bool]:
        result = await invoke(action, "--publication", engine, *args, as_json=False)
        return [row["id"] for row in result["datasets"]], result["has_more"]

    # The two matching values must come from the same file membership.
    assert await search("--exchange", "bybit", "--instrument", str(okx["symbol"])) == ([], False)
    assert await search("--exchange", "okx", "--end", str(END)) == ([], False)
    assert await search("--instrument", "BTC-USD_UM") == ([], False)
    options = ("--exchange", "okx", "--instrument-type", "future", "--market-category", "linear")
    for action in ("find", "list", "coverage"):
        assert await search(*options, "--data-type", "trades", action=action) == ([trade], False)
    assert await search("--instrument", "142") == ([trade], False)
    assert await search("--instrument", "btcusdt", "--data-type", "order_book_l2") == (
        [depth],
        False,
    )
    assert await search("--exchange", "bybit", "--limit", "1") == ([trade], True)
    assert await search("--exchange", "bybit", "--limit", "1", "--offset", "1") == ([depth], False)

    venue = await insert(
        "INSERT INTO catalog.exchanges(code,name) VALUES(%s,'Fixture') RETURNING id",
        ("test_" + token[:12],),
    )
    raw = await insert(
        """INSERT INTO historical.raw_objects(exchange_id,scope_key,scope_json,data_type,
        source_object_key,filename,start_at,end_at)
        VALUES(%s,%s,%s,'trades',%s,'native-name.zip','2024-01-01','2024-01-02') RETURNING id""",
        (venue, token, Jsonb({"scope_kind": "family", "scope_identifier": "BTC-USD_UM"}), token),
    )
    archive = await insert(
        """INSERT INTO historical.local_archives(remote_archive_id,storage_root_key,relative_path,
        status,bytes_written,sha256,downloaded_at)
        VALUES(%s,%s,'other.zip','complete',12,%s,now()) RETURNING id""",
        (raw, root, sha),
    )
    await database.execute(
        """INSERT INTO historical.dataset_raw_inputs(dataset_id,local_archive_id,input_sha256)
        VALUES(%s,%s,%s)""",
        (trade, archive, sha),
    )
    viewed = await invoke("lineage", str(trade), as_json=False)
    assert viewed["raw"][0]["filename"] == "native-name.zip"
    assert viewed["raw"][0]["scope"] == "BTC-USD_UM"
    assert viewed["raw"][0]["start_at"].year == 2024
    assert viewed["selected"]["raw"] == 300
    assert "publication_raw" not in viewed
    expanded = await invoke("lineage", str(trade), "--publication", "--limit", "2", as_json=False)
    assert len(expanded["publication_raw"]) == 2
    evidence = await invoke("lineage", str(trade), "--limit", "2")
    assert len(evidence["publication_selection"]["raw_inputs"]) == 300
    assert evidence["file_contributions"]["raw_inputs"][0]["input_sha256"] == sha

    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    price_type = pa.decimal256(76, 28)
    price = Decimal("80982.9000000000000000000000000000")
    pq.write_table(
        pa.table(
            {
                "event_timestamp_ns": [START, END, START + 1],
                "price": pa.array([price, price, price], type=price_type),
                "trade_id": ["2", "3", "1"],
            }
        ),
        tmp_path / "trade.parquet",
    )
    schema = await invoke("schema", str(trade))
    assert schema["columns"][1]["type"] == "decimal256(76, 28)"
    batches = query.parquet.batches

    def first_only(
        files: Sequence[Path], *, columns: Sequence[str] | None = None, batch_size: int = 65536
    ) -> Iterator[Any]:
        assert files == [tmp_path / "trade.parquet"] and batch_size == 2
        yield next(batches(files, columns=columns, batch_size=batch_size))
        pytest.fail("Preview read more than its first bounded batch")

    monkeypatch.setattr(query.parquet, "batches", first_only)
    projected = await invoke(
        "head",
        str(trade),
        "--limit",
        "2",
        "--column",
        "trade_id",
        "--column",
        "price",
        as_json=False,
    )
    assert projected["projection"] == ["trade_id", "price"]
    assert projected["rows"] == [{"trade_id": i, "price": price} for i in ["2", "3"]]
