"""External revision identity and the shared local acquisition registry."""

import asyncio
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest
from psycopg import AsyncConnection
from psycopg.rows import TupleRow

import forge.historical.imports.service as imports
from forge.historical.imports.models import (
    HistoricalSpecs,
    ImportConfig,
    ImportIdentity,
    freeze_inputs,
)

ROOT = Path(__file__).resolve().parents[5]

pytestmark = [pytest.mark.integration, pytest.mark.database]


def config(root: Path) -> ImportConfig:
    return ImportConfig(
        source_key="TEST_" + uuid4().hex,
        root=str(root),
        files=["record.bin"],
        data_type="trades",
        identity=ImportIdentity(
            exchange="external", market_category="spot", instrument_type="spot", symbol="CUSTOM"
        ),
        specifications=HistoricalSpecs(
            as_of=datetime(2026, 1, 1, tzinfo=UTC),
            base_asset="BTC",
            quote_asset="USDT",
            quantity_type="base",
        ),
    )


@pytest.mark.asyncio
async def test_unknown_format_reuse_and_changed_revision_preserve_original(
    database: AsyncConnection[TupleRow], tmp_path: Path
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    path = source / "record.bin"
    path.write_bytes(b"PAR1 unsupported external bytes")
    selected = config(source)
    first = freeze_inputs(selected)[0]
    root = tmp_path / "raw"
    a = await imports.import_file(database, selected, first, root, "test")
    b = await imports.import_file(database, selected, first, root, "test")
    assert a["status"] == "completed" and b["status"] == "reused"
    assert a["local_archive_id"] == b["local_archive_id"]
    assert a["normalization_readiness"] == "requires_profile"
    original = (root / str(a["relative_path"])).read_bytes()
    path.write_bytes(b"changed source revision")
    revision = await imports.import_file(
        database, selected, freeze_inputs(selected)[0], root, "test"
    )
    assert revision["external_archive_id"] != a["external_archive_id"]
    assert (root / str(a["relative_path"])).read_bytes() == original
    assert path.read_bytes() == b"changed source revision"


@pytest.mark.asyncio
async def test_source_mutation_during_copy_never_completes(
    database: AsyncConnection[TupleRow], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    path = source / "record.bin"
    path.write_bytes(b"original")
    selected = config(source)
    frozen = freeze_inputs(selected)[0]
    original = imports.disk
    mutated = False

    async def controlled[**P, T](function: Callable[P, T], *args: P.args, **kwargs: P.kwargs) -> T:
        nonlocal mutated
        if getattr(function, "__name__", "") == "write" and not mutated:
            mutated = True
            await asyncio.to_thread(path.write_bytes, b"mutation")
        return await original(function, *args, **kwargs)

    monkeypatch.setattr(imports, "disk", controlled)
    with pytest.raises(Exception, match="changed during copying"):
        await imports.import_file(database, selected, frozen, tmp_path / "raw", "test")
    cur = await database.execute(
        "SELECT l.status FROM historical.local_archives l JOIN historical.external_archives a ON a.id=l.external_archive_id JOIN historical.external_sources s ON s.id=a.external_source_id WHERE s.source_key=%s",
        (selected.source_key,),
    )
    assert await cur.fetchall() == [("failed",)]


@pytest.mark.asyncio
async def test_publication_recovers_through_fresh_connection_without_copying(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from forge.shared.database.testing import testing_connection, testing_dsn

    dsn = testing_dsn(ROOT)
    source = tmp_path / "source"
    source.mkdir()
    (source / "record.bin").write_bytes(b"external raw binary")
    selected = config(source)
    frozen = freeze_inputs(selected)[0]
    root = tmp_path / "raw"
    original = imports.complete

    async def fail(c: AsyncConnection[TupleRow], local_id: int) -> None:
        raise RuntimeError("injected registration failure")

    try:
        monkeypatch.setattr(imports, "complete", fail)
        async with testing_connection(dsn) as c:
            with pytest.raises(RuntimeError):
                await imports.import_file(c, selected, frozen, root, "test")
        monkeypatch.setattr(imports, "complete", original)
        async with testing_connection(dsn) as c:
            result = await imports.import_file(c, selected, frozen, root, "test")
        assert result["status"] == "reused"
        assert (root / str(result["relative_path"])).is_file()
    finally:
        async with testing_connection(dsn) as c, c.transaction():
            await c.execute(
                """DELETE FROM historical.local_archives WHERE external_archive_id IN
                (SELECT a.id FROM historical.external_archives a JOIN historical.external_sources s ON s.id=a.external_source_id WHERE s.source_key=%s)""",
                (selected.source_key,),
            )
            await c.execute(
                "DELETE FROM historical.external_archives WHERE external_source_id IN (SELECT id FROM historical.external_sources WHERE source_key=%s)",
                (selected.source_key,),
            )
            await c.execute(
                "DELETE FROM historical.external_sources WHERE source_key=%s",
                (selected.source_key,),
            )
