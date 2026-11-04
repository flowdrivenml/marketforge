"""Authoritative scan/cache behavior against the real testing registry."""

import asyncio
from collections.abc import AsyncIterator
from uuid import uuid4

import pytest
from psycopg import AsyncConnection
from psycopg.rows import TupleRow

from forge.historical.archives.config import AcquisitionInterval, ArchiveRequest, ArchiveScope
from forge.historical.archives.discovery import discover, scan_archives
from forge.historical.archives.models import DiscoveredArchive
from forge.historical.archives.scans import find_fresh_discovery
from forge.historical.archives.sources.base import DiscoveryError

pytestmark = [pytest.mark.integration, pytest.mark.database]


class FixtureSource:
    version = "fixture-v1"

    def __init__(self, items: list[DiscoveredArchive]) -> None:
        self.items = items
        self.calls = 0
        self.failure: BaseException | None = None

    def validate(self, request: ArchiveRequest) -> None:
        pass

    async def discover(
        self, request: ArchiveRequest, interval: AcquisitionInterval
    ) -> AsyncIterator[DiscoveredArchive]:
        self.calls += 1
        for item in self.items:
            yield item
        if self.failure is not None:
            raise self.failure


@pytest.fixture
def selection(archive: DiscoveredArchive) -> ArchiveRequest:
    return archive.request.model_copy(
        update={"scope": ArchiveScope(kind="base_coin", identifier="TEST_" + uuid4().hex)}
    )


@pytest.mark.asyncio
async def test_initial_map_and_fresh_cache_with_concrete_scopes(
    connection: AsyncConnection[TupleRow], archive: DiscoveredArchive, selection: ArchiveRequest
) -> None:
    source = FixtureSource([archive])
    first = await discover(connection, source, selection, archive.interval)
    second = await discover(connection, source, selection, archive.interval)
    assert first.cached is False
    assert second.cached is True
    assert source.calls == 1
    results = await scan_archives(connection, second.scan_id, archive.interval)
    assert len(results) == 1
    assert results[0].source_object_key == archive.source_object_key


@pytest.mark.asyncio
async def test_empty_refresh_reconciles_previous_results(
    connection: AsyncConnection[TupleRow], archive: DiscoveredArchive, selection: ArchiveRequest
) -> None:
    source = FixtureSource([archive])
    await discover(connection, source, selection, archive.interval)
    source.items = []
    refreshed = await discover(connection, source, selection, archive.interval, refresh=True)
    cached = await discover(connection, source, selection, archive.interval)
    assert refreshed.archives_found == cached.archives_found == 0
    assert cached.cached
    assert source.calls == 2
    assert await scan_archives(connection, cached.scan_id, archive.interval) == ()


@pytest.mark.asyncio
async def test_expiry_and_version_trigger_discovery(
    connection: AsyncConnection[TupleRow], archive: DiscoveredArchive, selection: ArchiveRequest
) -> None:
    source = FixtureSource([])
    initial = await discover(connection, source, selection, archive.interval)
    await connection.execute(
        "UPDATE historical.discovery_scans SET refresh_after = NOW() - INTERVAL '1 second' WHERE id = %s",
        (initial.scan_id,),
    )
    await discover(connection, source, selection, archive.interval)
    source.version = "fixture-v2"
    await discover(connection, source, selection, archive.interval)
    assert source.calls == 3


@pytest.mark.asyncio
@pytest.mark.parametrize("cancelled", [False, True])
async def test_partial_or_cancelled_pages_never_complete_coverage(
    connection: AsyncConnection[TupleRow],
    archive: DiscoveredArchive,
    selection: ArchiveRequest,
    cancelled: bool,
) -> None:
    # More than a persistence batch: partial observations really reach PostgreSQL.
    source = FixtureSource(
        [
            archive.model_copy(update={"source_object_key": f"{archive.source_object_key}/{index}"})
            for index in range(101)
        ]
    )
    source.failure = asyncio.CancelledError() if cancelled else DiscoveryError("Pagination failed")
    with pytest.raises(asyncio.CancelledError if cancelled else DiscoveryError):
        await discover(connection, source, selection, archive.interval)
    assert (
        await find_fresh_discovery(
            connection, request=selection, interval=archive.interval, adapter_version=source.version
        )
        is None
    )


@pytest.mark.asyncio
async def test_failed_refresh_keeps_prior_complete_scan(
    connection: AsyncConnection[TupleRow], archive: DiscoveredArchive, selection: ArchiveRequest
) -> None:
    source = FixtureSource([archive])
    first = await discover(connection, source, selection, archive.interval)
    source.failure = DiscoveryError("Page failed")
    with pytest.raises(DiscoveryError):
        await discover(connection, source, selection, archive.interval, refresh=True)
    cached = await discover(connection, source, selection, archive.interval)
    assert cached.cached
    assert cached.scan_id == first.scan_id


@pytest.mark.asyncio
async def test_signed_location_refresh_keeps_identity(
    connection: AsyncConnection[TupleRow], archive: DiscoveredArchive, selection: ArchiveRequest
) -> None:
    source = FixtureSource([archive])
    first = await discover(connection, source, selection, archive.interval)
    first_rows = await scan_archives(connection, first.scan_id, archive.interval)
    source.items = [
        archive.model_copy(update={"download_url": "https://exchange.test/new?signature=renewed"})
    ]
    second = await discover(connection, source, selection, archive.interval, refresh=True)
    second_rows = await scan_archives(connection, second.scan_id, archive.interval)
    assert first_rows[0].id == second_rows[0].id
    assert second_rows[0].download_url != first_rows[0].download_url


@pytest.mark.asyncio
async def test_location_refresh_does_not_truncate_superseded_download_plan(
    connection: AsyncConnection[TupleRow], archive: DiscoveredArchive, selection: ArchiveRequest
) -> None:
    source = FixtureSource([archive])
    first = await discover(connection, source, selection, archive.interval)
    await discover(connection, source, selection, archive.interval, refresh=True)
    assert await scan_archives(connection, first.scan_id, archive.interval) == ()
    retained = await scan_archives(
        connection, first.scan_id, archive.interval, allow_superseded=True
    )
    assert len(retained) == 1
    assert retained[0].source_object_key == archive.source_object_key
