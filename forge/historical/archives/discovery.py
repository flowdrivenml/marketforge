"""Bounded, transactional discovery with authoritative selection membership."""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import timedelta

from psycopg import AsyncConnection
from psycopg.rows import TupleRow, class_row
from psycopg.types.json import Jsonb

from forge.historical.archives.config import AcquisitionInterval, ArchiveRequest
from forge.historical.archives.models import (
    DiscoveredArchive,
    archive_scope_document,
    archive_scope_key,
)
from forge.historical.archives.registry import CachedRemoteArchive, register_remote_archive
from forge.historical.archives.scans import find_fresh_discovery
from forge.historical.archives.sources.base import ArchiveSource, DiscoveryError


@dataclass(frozen=True, slots=True)
class DiscoveryResult:
    scan_id: int
    cached: bool
    archives_found: int


async def discover(
    connection: AsyncConnection[TupleRow],
    source: ArchiveSource,
    request: ArchiveRequest,
    interval: AcquisitionInterval,
    *,
    refresh: bool = False,
    fresh_for: timedelta = timedelta(hours=6),
    on_activity: Callable[[str], None] | None = None,
    on_registered: Callable[[int], None] | None = None,
) -> DiscoveryResult:
    """Publish coverage only after every page and observation has persisted.

    Batches may persist partial observations, but only a complete scan supplies
    reusable membership. A successful refresh supersedes overlapping older
    scans for this selection. Removed members remain known remote identities;
    they are absent from the authoritative selection result. Incomplete scans
    never change previously completed coverage or mark objects unavailable.
    """
    source.validate(request)
    if not refresh:
        cached = await find_fresh_discovery(
            connection, request=request, interval=interval, adapter_version=source.version
        )
        if cached is not None:
            return DiscoveryResult(cached.id, True, cached.archives_found)
    async with connection.transaction():
        cursor = await connection.execute(
            """
            INSERT INTO historical.discovery_scans (
                exchange_id, scope_key, scope_json, data_type,
                searched_start_at, searched_end_at, adapter_version
            ) SELECT id, %s, %s, %s, %s, %s, %s
            FROM catalog.exchanges WHERE code = %s RETURNING id
            """,
            (
                archive_scope_key(request),
                Jsonb(archive_scope_document(request)),
                request.data_type,
                interval.start,
                interval.end,
                source.version,
                request.exchange,
            ),
        )
        row = await cursor.fetchone()
        if row is None:
            raise DiscoveryError("Exchange is not registered in catalog.exchanges.")
        scan_id: int = row[0]
    try:
        batch: list[DiscoveredArchive] = []
        async for item in source.discover(request, interval):
            batch.append(item)
            if len(batch) >= 100:
                if on_activity is not None:
                    on_activity("Saving discovery observations")
                await _persist_batch(connection, scan_id, batch)
                if on_registered is not None:
                    on_registered(len(batch))
                batch.clear()
        if batch:
            if on_activity is not None:
                on_activity("Saving discovery observations")
            await _persist_batch(connection, scan_id, batch)
            if on_registered is not None:
                on_registered(len(batch))
        async with connection.transaction():
            # Invalidate overlapping snapshots only after successful discovery.
            await connection.execute(
                """
                UPDATE historical.discovery_scans SET status = 'partial'
                WHERE exchange_id = (SELECT exchange_id FROM historical.discovery_scans WHERE id = %s)
                    AND scope_key = %s AND data_type = %s AND status = 'complete'
                    AND searched_start_at < %s AND searched_end_at > %s
                """,
                (
                    scan_id,
                    archive_scope_key(request),
                    request.data_type,
                    interval.end,
                    interval.start,
                ),
            )
            cursor = await connection.execute(
                """
                UPDATE historical.discovery_scans SET status = 'complete',
                    completed_at = statement_timestamp(),
                    refresh_after = statement_timestamp() + %s,
                    archives_found = (SELECT count(*) FROM historical.discovery_scan_archives WHERE scan_id = %s)
                WHERE id = %s RETURNING archives_found
                """,
                (fresh_for, scan_id, scan_id),
            )
            row = await cursor.fetchone()
            if row is None:
                raise DiscoveryError("Discovery scan disappeared before completion.")
            return DiscoveryResult(scan_id, False, row[0])
    except BaseException as error:
        async with connection.transaction():
            await connection.execute(
                "UPDATE historical.discovery_scans SET status = %s, error_message = %s WHERE id = %s",
                (
                    "failed" if isinstance(error, Exception) else "cancelled",
                    "Discovery did not complete; retry or inspect endpoint compatibility.",
                    scan_id,
                ),
            )
        raise


async def _persist_batch(
    connection: AsyncConnection[TupleRow], scan_id: int, batch: list[DiscoveredArchive]
) -> None:
    async with connection.transaction():
        for item in batch:
            registered = await register_remote_archive(connection, item)
            await connection.execute(
                """INSERT INTO historical.discovery_scan_archives VALUES (%s, %s)
                   ON CONFLICT DO NOTHING""",
                (scan_id, registered.id),
            )


async def scan_archives(
    connection: AsyncConnection[TupleRow],
    scan_id: int,
    interval: AcquisitionInterval,
    *,
    after_id: int = 0,
    allow_superseded: bool = False,
) -> tuple[CachedRemoteArchive, ...]:
    """Read a bounded page from the completed snapshot, including broad scopes."""
    async with connection.cursor(row_factory=class_row(CachedRemoteArchive)) as cursor:
        await cursor.execute(
            """
            SELECT a.id, a.source_object_key, a.filename, a.start_at, a.end_at,
                a.download_url, a.url_expires_at, a.source_etag, a.source_last_modified
            FROM historical.remote_archives a
            JOIN historical.discovery_scan_archives m ON m.remote_archive_id = a.id
            JOIN historical.discovery_scans s ON s.id = m.scan_id
            WHERE m.scan_id = %s AND (s.status = 'complete' OR (%s AND s.status = 'partial'))
                AND a.start_at < %s AND a.end_at > %s AND a.id > %s
            ORDER BY a.id LIMIT 100
            """,
            (scan_id, allow_superseded, interval.end, interval.start, after_id),
        )
        return tuple(await cursor.fetchall())
