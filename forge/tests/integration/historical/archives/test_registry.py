"""Remote archive registration against the explicit testing database."""

import os
from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
import pytest_asyncio
from psycopg import AsyncConnection
from psycopg.rows import TupleRow

from forge.historical.archives.config import AcquisitionInterval, ArchiveRequest
from forge.historical.archives.models import DiscoveredArchive
from forge.historical.archives.registry import (
    ArchiveRegistryError,
    list_available_remote_archives,
    register_remote_archive,
)
from forge.historical.archives.scans import (
    find_fresh_discovery,
    record_completed_discovery,
)

pytestmark = [pytest.mark.integration, pytest.mark.database]


@pytest_asyncio.fixture
async def connection() -> AsyncGenerator[AsyncConnection[TupleRow], None]:
    dsn = os.environ.get("MARKETFORGE_DATABASE_URL_TEST")

    if not dsn:
        pytest.skip("MARKETFORGE_DATABASE_URL_TEST is not set.")

    async with await AsyncConnection.connect(
        dsn,
        autocommit=True,
        connect_timeout=10,
    ) as connection:
        cursor = await connection.execute("SELECT current_database()")
        database = await cursor.fetchone()

        if database != ("marketforge_test",):
            pytest.fail("Registry tests require the marketforge_test database.")

        cursor = await connection.execute("""
            SELECT purpose
            FROM forge_meta.database_identity
            WHERE singleton = TRUE
            """)
        identity = await cursor.fetchone()

        if identity != ("testing",):
            pytest.fail("Registry tests require a testing database identity.")

        async with connection.transaction(force_rollback=True):
            yield connection


@pytest.fixture
def archive() -> DiscoveredArchive:
    key = uuid4().hex

    return DiscoveredArchive(
        request=ArchiveRequest.model_validate(
            {
                "exchange": "bybit",
                "instrument_type": "spot",
                "market_category": "spot",
                "data_type": "trades",
                "scope": {
                    "kind": "instrument",
                    "identifier": "BTCUSDT",
                },
            }
        ),
        source_object_key=f"integration-test/{key}",
        filename=f"{key}.csv.gz",
        interval=AcquisitionInterval(
            start=datetime(2026, 9, 1, tzinfo=UTC),
            end=datetime(2026, 9, 2, tzinfo=UTC),
        ),
        download_url="https://exchange.test/archive?signature=first",
    )


@pytest.mark.asyncio
async def test_rediscovery_updates_existing_row(
    connection: AsyncConnection[TupleRow],
    archive: DiscoveredArchive,
) -> None:
    first = await register_remote_archive(connection, archive)

    payload = archive.model_dump()
    payload["download_url"] = "https://exchange.test/archive?signature=second"
    refreshed = DiscoveredArchive.model_validate(payload)

    second = await register_remote_archive(connection, refreshed)

    assert second.id == first.id
    assert second.first_seen_at == first.first_seen_at
    assert second.last_seen_at >= first.last_seen_at
    assert second.download_url == refreshed.download_url

    cursor = await connection.execute(
        """
        SELECT count(*)
        FROM historical.remote_archives
        WHERE source_object_key = %s
        """,
        (archive.source_object_key,),
    )

    assert await cursor.fetchone() == (1,)


@pytest.mark.asyncio
async def test_can_clear_previous_download_url(
    connection: AsyncConnection[TupleRow],
    archive: DiscoveredArchive,
) -> None:
    first = await register_remote_archive(connection, archive)

    payload = archive.model_dump()
    payload["download_url"] = None
    refreshed = DiscoveredArchive.model_validate(payload)

    second = await register_remote_archive(connection, refreshed)

    assert second.id == first.id
    assert second.download_url is None


@pytest.mark.asyncio
async def test_rejects_conflicting_coverage(
    connection: AsyncConnection[TupleRow],
    archive: DiscoveredArchive,
) -> None:
    first = await register_remote_archive(connection, archive)

    payload = archive.model_dump()
    payload["interval"] = {
        "start": datetime(2026, 9, 1, tzinfo=UTC),
        "end": datetime(2026, 9, 3, tzinfo=UTC),
    }
    conflicting = DiscoveredArchive.model_validate(payload)

    with pytest.raises(ArchiveRegistryError, match="conflicting"):
        await register_remote_archive(connection, conflicting)

    cursor = await connection.execute(
        """
        SELECT start_at, end_at, download_url
        FROM historical.remote_archives
        WHERE id = %s
        """,
        (first.id,),
    )

    assert await cursor.fetchone() == (
        archive.interval.start,
        archive.interval.end,
        archive.download_url,
    )


@pytest.mark.asyncio
async def test_rejects_unknown_exchange(
    connection: AsyncConnection[TupleRow],
    archive: DiscoveredArchive,
) -> None:
    request_payload = archive.request.model_dump()
    request_payload["exchange"] = f"test_{uuid4().hex}"

    payload = archive.model_dump()
    payload["request"] = request_payload
    unknown = DiscoveredArchive.model_validate(payload)

    with pytest.raises(ArchiveRegistryError, match="Exchange is not registered"):
        await register_remote_archive(connection, unknown)


@pytest.mark.asyncio
async def test_records_successful_empty_discovery(
    connection: AsyncConnection[TupleRow],
    archive: DiscoveredArchive,
) -> None:
    scan = await record_completed_discovery(
        connection,
        request=archive.request,
        interval=archive.interval,
        archives_found=0,
        adapter_version="test-v1",
        fresh_for=timedelta(hours=6),
    )

    assert scan.archives_found == 0
    assert scan.refresh_after - scan.completed_at == timedelta(hours=6)

    cursor = await connection.execute(
        """
        SELECT status, searched_start_at, searched_end_at, adapter_version
        FROM historical.discovery_scans
        WHERE id = %s
        """,
        (scan.id,),
    )

    assert await cursor.fetchone() == (
        "complete",
        archive.interval.start,
        archive.interval.end,
        "test-v1",
    )


@pytest.mark.asyncio
async def test_archive_and_scan_roll_back_together(
    connection: AsyncConnection[TupleRow],
    archive: DiscoveredArchive,
) -> None:
    async with connection.transaction(force_rollback=True):
        registered = await register_remote_archive(connection, archive)

        scan = await record_completed_discovery(
            connection,
            request=archive.request,
            interval=archive.interval,
            archives_found=1,
            adapter_version="test-v1",
            fresh_for=timedelta(hours=1),
        )

    cursor = await connection.execute(
        "SELECT id FROM historical.remote_archives WHERE id = %s",
        (registered.id,),
    )
    assert await cursor.fetchone() is None

    cursor = await connection.execute(
        "SELECT id FROM historical.discovery_scans WHERE id = %s",
        (scan.id,),
    )
    assert await cursor.fetchone() is None


@pytest.mark.asyncio
async def test_rejects_nonpositive_discovery_freshness(
    connection: AsyncConnection[TupleRow],
    archive: DiscoveredArchive,
) -> None:
    with pytest.raises(ValueError, match="fresh_for"):
        await record_completed_discovery(
            connection,
            request=archive.request,
            interval=archive.interval,
            archives_found=0,
            adapter_version="test-v1",
            fresh_for=timedelta(0),
        )


@pytest.fixture
def cache_request(archive: DiscoveredArchive) -> ArchiveRequest:
    payload = archive.request.model_dump()
    payload["scope"] = {
        "kind": "instrument",
        "identifier": f"CACHE_TEST_{uuid4().hex}",
    }
    return ArchiveRequest.model_validate(payload)


@pytest.mark.asyncio
async def test_reuses_successful_empty_scan(
    connection: AsyncConnection[TupleRow],
    archive: DiscoveredArchive,
    cache_request: ArchiveRequest,
) -> None:
    recorded = await record_completed_discovery(
        connection,
        request=cache_request,
        interval=archive.interval,
        archives_found=0,
        adapter_version="test-v1",
        fresh_for=timedelta(hours=1),
    )

    cached = await find_fresh_discovery(
        connection,
        request=cache_request,
        interval=archive.interval,
        adapter_version="test-v1",
    )

    assert cached is not None
    assert cached.id == recorded.id
    assert cached.archives_found == 0


@pytest.mark.asyncio
async def test_reuses_scan_for_smaller_contained_interval(
    connection: AsyncConnection[TupleRow],
    archive: DiscoveredArchive,
    cache_request: ArchiveRequest,
) -> None:
    recorded = await record_completed_discovery(
        connection,
        request=cache_request,
        interval=archive.interval,
        archives_found=1,
        adapter_version="test-v1",
        fresh_for=timedelta(hours=1),
    )

    smaller = AcquisitionInterval(
        start=archive.interval.start + timedelta(hours=1),
        end=archive.interval.end - timedelta(hours=1),
    )

    cached = await find_fresh_discovery(
        connection,
        request=cache_request,
        interval=smaller,
        adapter_version="test-v1",
    )

    assert cached is not None
    assert cached.id == recorded.id


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mismatch",
    [
        "interval",
        "scope",
        "data_type",
        "exchange",
        "adapter_version",
        "expired",
        "partial",
        "failed",
    ],
)
async def test_does_not_reuse_incompatible_scan(
    connection: AsyncConnection[TupleRow],
    archive: DiscoveredArchive,
    cache_request: ArchiveRequest,
    mismatch: str,
) -> None:
    recorded = await record_completed_discovery(
        connection,
        request=cache_request,
        interval=archive.interval,
        archives_found=1,
        adapter_version="test-v1",
        fresh_for=timedelta(hours=1),
    )

    request = cache_request
    interval = archive.interval
    adapter_version = "test-v1"

    if mismatch == "interval":
        interval = AcquisitionInterval(
            start=archive.interval.start,
            end=archive.interval.end + timedelta(hours=1),
        )

    elif mismatch in {"scope", "data_type", "exchange"}:
        payload = cache_request.model_dump()

        if mismatch == "scope":
            payload["scope"] = {
                "kind": "instrument",
                "identifier": f"OTHER_{uuid4().hex}",
            }
        elif mismatch == "data_type":
            payload["data_type"] = "order_book_l2"
        else:
            payload["exchange"] = "binance"

        request = ArchiveRequest.model_validate(payload)

    elif mismatch == "adapter_version":
        adapter_version = "test-v2"

    elif mismatch == "expired":
        await connection.execute(
            """
            UPDATE historical.discovery_scans
            SET refresh_after = statement_timestamp() - INTERVAL '1 second'
            WHERE id = %s
            """,
            (recorded.id,),
        )

    else:
        await connection.execute(
            """
            UPDATE historical.discovery_scans
            SET status = %s
            WHERE id = %s
            """,
            (mismatch, recorded.id),
        )

    cached = await find_fresh_discovery(
        connection,
        request=request,
        interval=interval,
        adapter_version=adapter_version,
    )

    assert cached is None


@pytest.mark.asyncio
async def test_missing_scan_requires_discovery(
    connection: AsyncConnection[TupleRow],
    archive: DiscoveredArchive,
    cache_request: ArchiveRequest,
) -> None:
    cached = await find_fresh_discovery(
        connection,
        request=cache_request,
        interval=archive.interval,
        adapter_version="test-v1",
    )

    assert cached is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("shift_hours", "expected"),
    [
        (-24, False),
        (-12, True),
        (0, True),
        (12, True),
        (24, False),
    ],
)
async def test_lists_only_overlapping_archives(
    connection: AsyncConnection[TupleRow],
    archive: DiscoveredArchive,
    cache_request: ArchiveRequest,
    shift_hours: int,
    expected: bool,
) -> None:
    payload = archive.model_dump()
    payload["request"] = cache_request.model_dump()
    payload["interval"] = {
        "start": archive.interval.start + timedelta(hours=shift_hours),
        "end": archive.interval.end + timedelta(hours=shift_hours),
    }

    shifted = DiscoveredArchive.model_validate(payload)
    registered = await register_remote_archive(connection, shifted)

    records = await list_available_remote_archives(
        connection,
        request=cache_request,
        interval=archive.interval,
    )

    assert tuple(record.id for record in records) == (
        (registered.id,) if expected else ()
    )


@pytest.mark.asyncio
async def test_paginates_without_duplicates(
    connection: AsyncConnection[TupleRow],
    archive: DiscoveredArchive,
    cache_request: ArchiveRequest,
) -> None:
    registered_ids: list[int] = []

    for index in range(3):
        payload = archive.model_dump()
        payload["request"] = cache_request.model_dump()
        payload["source_object_key"] = f"{archive.source_object_key}/part-{index}"
        item = DiscoveredArchive.model_validate(payload)
        registered = await register_remote_archive(connection, item)
        registered_ids.append(registered.id)

    first = await list_available_remote_archives(
        connection,
        request=cache_request,
        interval=archive.interval,
        limit=2,
    )
    assert len(first) == 2

    second = await list_available_remote_archives(
        connection,
        request=cache_request,
        interval=archive.interval,
        after_id=first[-1].id,
        limit=2,
    )
    assert len(second) == 1

    final = await list_available_remote_archives(
        connection,
        request=cache_request,
        interval=archive.interval,
        after_id=second[-1].id,
        limit=2,
    )

    assert [record.id for record in (*first, *second)] == registered_ids
    assert final == ()


@pytest.mark.asyncio
@pytest.mark.parametrize("location", ["missing", "expired"])
async def test_keeps_archives_needing_location_refresh(
    connection: AsyncConnection[TupleRow],
    archive: DiscoveredArchive,
    cache_request: ArchiveRequest,
    location: str,
) -> None:
    payload = archive.model_dump()
    payload["request"] = cache_request.model_dump()

    if location == "missing":
        payload["download_url"] = None
        payload["url_expires_at"] = None
    else:
        payload["url_expires_at"] = datetime(2000, 1, 1, tzinfo=UTC)

    item = DiscoveredArchive.model_validate(payload)
    registered = await register_remote_archive(connection, item)

    records = await list_available_remote_archives(
        connection,
        request=cache_request,
        interval=archive.interval,
    )

    assert len(records) == 1
    assert records[0].id == registered.id
    assert records[0].download_url == item.download_url
    assert records[0].url_expires_at == item.url_expires_at


@pytest.mark.asyncio
async def test_excludes_unavailable_archives(
    connection: AsyncConnection[TupleRow],
    archive: DiscoveredArchive,
    cache_request: ArchiveRequest,
) -> None:
    payload = archive.model_dump()
    payload["request"] = cache_request.model_dump()
    item = DiscoveredArchive.model_validate(payload)

    registered = await register_remote_archive(connection, item)

    await connection.execute(
        """
        UPDATE historical.remote_archives
        SET remote_status = 'unavailable'
        WHERE id = %s
        """,
        (registered.id,),
    )

    records = await list_available_remote_archives(
        connection,
        request=cache_request,
        interval=archive.interval,
    )

    assert records == ()
