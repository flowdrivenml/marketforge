"""Test the catalogue transaction with isolated evidence and generated keys."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
from psycopg import AsyncConnection
from psycopg.rows import TupleRow

from forge.historical.archives.storage.acquire import root_key
from forge.historical.engine.managed import checksum
from forge.historical.normalization import datasets
from forge.shared.database.testing import TestingDatabaseError as DatabaseUnavailable
from forge.shared.paths import ProjectPaths

pytestmark = [pytest.mark.integration, pytest.mark.database]


@pytest.mark.asyncio
async def test_dataset_transactions(
    tmp_path: Path, database: AsyncConnection[TupleRow], monkeypatch: pytest.MonkeyPatch
) -> None:
    # This exercises registry transactions with synthetic evidence only.
    # Production Rust artifact verification is outside this database test.
    paths = ProjectPaths(tmp_path, tmp_path / "data")
    paths.raw_dir.mkdir(parents=True)
    archive = paths.raw_dir / "synthetic"
    archive.write_text("source fixture")
    digest = checksum(archive)

    async def add(query: Any, params: tuple[Any, ...] = ()) -> int:
        row = await (await database.execute(query, params)).fetchone()
        assert row
        return int(row[0])

    venue = await add(
        "INSERT INTO catalog.exchanges(code,name) VALUES(%s,'Fixture') RETURNING id",
        ("test_" + uuid4().hex[:12],),
    )
    remote = await add(
        """INSERT INTO historical.raw_objects(exchange_id,scope_key,scope_json,data_type,
        source_object_key,filename,start_at,end_at)
        VALUES(%s,'fixture','{}','trades','synthetic','synthetic','2024-01-01','2024-01-02')
        RETURNING id""",
        (venue,),
    )
    local = await add(
        """INSERT INTO historical.local_archives(remote_archive_id,storage_root_key,relative_path,
        status,bytes_written,sha256,downloaded_at)
        VALUES(%s,%s,'synthetic','complete',%s,%s,now()) RETURNING id""",
        (remote, root_key(paths.raw_dir), archive.stat().st_size, digest),
    )
    namespace = "fixture:" + uuid4().hex
    normalized_path = tmp_path / "normalized"

    def evidence(root: Path, op: str, engine_id: str) -> dict[str, Any]:
        return {
            "path": str(root),
            "manifest_path": str(root / "manifest.json"),
            "manifest": {
                "files": [
                    {
                        "path": f"tasks/1/trades/part-{i}.parquet",
                        "rows": i + 1,
                        "size_bytes": 8,
                        "start_timestamp_ns": 1727683200000000001 + i,
                        "end_timestamp_ns": 1727683200000000002 + i,
                    }
                    for i in range(2)
                ],
                "start_timestamp_ns": 1727683200000000001,
                "end_timestamp_ns": 1727683200000000002,
            },
            "operation": op,
            "content_type": "trades",
            "engine_dataset_id": engine_id,
            "instrument_namespace": namespace,
            "manifest_sha256": "a" * 64,
            "processing_job_sha256": "b" * 64,
            "inventory_sha256": {f"tasks/1/trades/part-{i}.parquet": "a" * 64 for i in range(2)},
            "resolved_job": {},
        }

    normalized = {
        **evidence(normalized_path, "normalize", "900000000000000000"),
        "processing_runtime_sha256": "c" * 64,
        "resolved_job": {
            "tasks": [
                {
                    "task_id": 1,
                    "input_path": str(archive),
                    "instrument_id": 2,
                    "exchange": "fixture",
                    "symbol": "XYZ",
                    "stream_id": "one",
                }
            ]
        },
        "expected_raw_sha256": {str(archive): digest},
    }

    @asynccontextmanager
    async def unavailable(_: str) -> AsyncIterator[AsyncConnection[TupleRow]]:
        raise DatabaseUnavailable("fixture outage")
        yield database

    @asynccontextmanager
    async def connect(_: str) -> AsyncIterator[AsyncConnection[TupleRow]]:
        yield database

    monkeypatch.setattr(datasets, "database_dsn", lambda root: "verified-fixture")
    monkeypatch.setattr(datasets, "database_connection", unavailable)
    pending = await datasets.register_verified(
        paths, [normalized], receipt_directory=tmp_path / "outage"
    )
    assert pending["registration"] == "pending"
    assert (tmp_path / "outage/registration-intent.json").is_file()

    monkeypatch.setattr(datasets, "database_connection", connect)
    registered = await datasets.register_verified(
        paths, [normalized], receipt_directory=tmp_path / "retry"
    )
    assert registered["registration"] == "registered", registered
    mapping = registered["catalogue_dataset_ids"]
    assert registered["counts"] == {"publications": 1, "primary_files": 2, "new": 2, "reused": 0}
    assert len(set(mapping.values())) == 2
    name = "tasks/1/trades/part-0.parquet"
    normalized_id = mapping[str(normalized_path / name)]
    assert str(normalized_id) != normalized["engine_dataset_id"]
    repeat = await datasets.register_verified(
        paths, [normalized], receipt_directory=tmp_path / "repeat"
    )
    assert repeat["catalogue_dataset_ids"] == mapping
    assert repeat["counts"]["reused"] == 2
    single = await datasets.register_verified(
        paths, [{**normalized, "selected_primary": name}], receipt_directory=tmp_path / "single"
    )
    assert single["catalogue_dataset_ids"] == {str(normalized_path / name): normalized_id}
    cur = await database.execute(
        "SELECT local_archive_id,input_sha256 FROM historical.dataset_raw_inputs WHERE dataset_id=%s",
        (normalized_id,),
    )
    assert await cur.fetchone() == (local, digest)
    conflict = await datasets.register_verified(
        paths,
        [{**normalized, "manifest_sha256": "b" * 64}],
        receipt_directory=tmp_path / "conflict",
    )
    assert conflict["registration"] == "failed" and "conflicts" in conflict["error"]

    merged = {
        **evidence(tmp_path / "merged", "merge", "900000000000000001"),
        "merge_inputs": [
            {
                "database_id": normalized_id,
                "primary_file": name,
                "file_sha256": "a" * 64,
                "path": str(normalized_path),
                "namespace": namespace,
                "engine_id": normalized["engine_dataset_id"],
                "manifest_sha256": normalized["manifest_sha256"],
            }
        ],
    }
    outcome = await datasets.register_verified(
        paths, [merged], receipt_directory=tmp_path / "merge"
    )
    assert outcome["registration"] == "registered", outcome
    cur = await database.execute(
        "SELECT operation,start_timestamp_ns FROM historical.datasets WHERE id=%s",
        (next(iter(outcome["catalogue_dataset_ids"].values())),),
    )
    assert await cur.fetchone() == ("merge", normalized["manifest"]["start_timestamp_ns"])
    cur = await database.execute(
        "SELECT evidence->'selected_dataset_inputs' FROM historical.dataset_publications WHERE operation='merge' AND instrument_namespace=%s",
        (namespace,),
    )
    assert await cur.fetchone() == ([normalized_id],)
