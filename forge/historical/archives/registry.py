"""Persistence of discovered historical archives."""

from dataclasses import dataclass
from datetime import datetime

from psycopg import AsyncConnection
from psycopg.pq import TransactionStatus
from psycopg.rows import TupleRow, class_row
from psycopg.types.json import Jsonb

from forge.historical.archives.config import AcquisitionInterval, ArchiveRequest
from forge.historical.archives.models import (
    DiscoveredArchive,
    archive_scope_document,
    archive_scope_key,
)
from forge.shared.errors import MarketForgeError


class ArchiveRegistryError(MarketForgeError):
    """An archive cannot be registered consistently."""


@dataclass(frozen=True, slots=True)
class RegisteredArchive:
    """Registry identity and current remote location."""

    id: int
    download_url: str | None
    first_seen_at: datetime
    last_seen_at: datetime


@dataclass(frozen=True, slots=True)
class _Exchange:
    id: int


async def register_remote_archive(
    connection: AsyncConnection[TupleRow],
    archive: DiscoveredArchive,
) -> RegisteredArchive:
    """Insert or refresh one discovered archive inside a caller-owned transaction.

    Does not commit, create schemas, open connections, or download anything.
    The caller must start a transaction before calling this function.
    """
    if connection.info.transaction_status != TransactionStatus.INTRANS:
        raise ArchiveRegistryError("Archive registration requires an active transaction.")

    async with connection.cursor(row_factory=class_row(_Exchange)) as cursor:
        await cursor.execute(
            "SELECT id FROM catalog.exchanges WHERE code = %s",
            (archive.request.exchange,),
        )
        exchange = await cursor.fetchone()

    if exchange is None:
        raise ArchiveRegistryError(f"Exchange is not registered: {archive.request.exchange}.")

    async with connection.cursor(row_factory=class_row(RegisteredArchive)) as cursor:
        await cursor.execute(
            """
            INSERT INTO historical.remote_archives AS existing (
                exchange_id,
                scope_key,
                scope_json,
                data_type,
                source_object_key,
                filename,
                start_at,
                end_at,
                download_url,
                url_expires_at,
                source_etag,
                source_last_modified,
                remote_status,
                first_seen_at,
                last_seen_at,
                last_checked_at
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s,
                'available',
                statement_timestamp(),
                statement_timestamp(),
                statement_timestamp()
            )
            ON CONFLICT ON CONSTRAINT remote_archives_identity_key
            DO UPDATE SET
                filename = EXCLUDED.filename,
                download_url = EXCLUDED.download_url,
                url_expires_at = EXCLUDED.url_expires_at,
                source_etag = EXCLUDED.source_etag,
                source_last_modified = EXCLUDED.source_last_modified,
                remote_status = 'available',
                last_seen_at = EXCLUDED.last_seen_at,
                last_checked_at = EXCLUDED.last_checked_at
            WHERE
                existing.start_at = EXCLUDED.start_at
                AND existing.end_at = EXCLUDED.end_at
                AND existing.scope_json = EXCLUDED.scope_json
            RETURNING id, download_url, first_seen_at, last_seen_at
            """,
            (
                exchange.id,
                archive.scope_key,
                Jsonb(archive_scope_document(archive.request)),
                archive.request.data_type,
                archive.source_object_key,
                archive.filename,
                archive.interval.start,
                archive.interval.end,
                archive.download_url,
                archive.url_expires_at,
                archive.source_etag,
                archive.source_last_modified,
            ),
        )
        registered = await cursor.fetchone()

    if registered is None:
        raise ArchiveRegistryError(
            "Existing archive identity has conflicting time coverage or scope metadata."
        )

    return registered


@dataclass(frozen=True, slots=True)
class CachedRemoteArchive:
    """Available source archive and its current download location."""

    id: int
    source_object_key: str
    filename: str
    start_at: datetime
    end_at: datetime
    download_url: str | None
    url_expires_at: datetime | None
    source_etag: str | None
    source_last_modified: datetime | None


async def list_available_remote_archives(
    connection: AsyncConnection[TupleRow],
    *,
    request: ArchiveRequest,
    interval: AcquisitionInterval,
    after_id: int = 0,
    limit: int = 500,
) -> tuple[CachedRemoteArchive, ...]:
    """Read one bounded page of available archives overlapping an interval.

    Intervals use an exclusive end. Continue pagination using the last returned
    archive ID as after_id.

    Expired or missing download locations remain visible: their archive
    identity is known, but acquisition must resolve a usable location.

    An empty page does not establish that discovery searched the interval.
    """
    if isinstance(after_id, bool) or after_id < 0:
        raise ValueError("after_id must be a non-negative integer.")

    if isinstance(limit, bool) or not 1 <= limit <= 1000:
        raise ValueError("limit must be an integer from 1 to 1000.")

    async with connection.cursor(row_factory=class_row(CachedRemoteArchive)) as cursor:
        await cursor.execute(
            """
            SELECT
                archive.id,
                archive.source_object_key,
                archive.filename,
                archive.start_at,
                archive.end_at,
                archive.download_url,
                archive.url_expires_at,
                archive.source_etag,
                archive.source_last_modified
            FROM historical.remote_archives AS archive
            JOIN catalog.exchanges AS exchange
                ON exchange.id = archive.exchange_id
            WHERE exchange.code = %s
                AND archive.scope_key = %s
                AND archive.scope_json = %s
                AND archive.data_type = %s
                AND archive.remote_status = 'available'
                AND archive.start_at < %s
                AND archive.end_at > %s
                AND archive.id > %s
            ORDER BY archive.id
            LIMIT %s
            """,
            (
                request.exchange,
                archive_scope_key(request),
                Jsonb(archive_scope_document(request)),
                request.data_type,
                interval.end,
                interval.start,
                after_id,
                limit,
            ),
        )
        return tuple(await cursor.fetchall())
