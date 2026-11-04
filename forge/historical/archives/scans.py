"""Persistence of successfully completed archive discovery scans."""

from dataclasses import dataclass
from datetime import datetime, timedelta

from psycopg import AsyncConnection
from psycopg.pq import TransactionStatus
from psycopg.rows import TupleRow, class_row
from psycopg.types.json import Jsonb

from forge.historical.archives.config import AcquisitionInterval, ArchiveRequest
from forge.historical.archives.models import archive_scope_document, archive_scope_key
from forge.historical.archives.registry import ArchiveRegistryError


@dataclass(frozen=True, slots=True)
class CompletedDiscoveryScan:
    """Persisted search coverage and its freshness deadline."""

    id: int
    completed_at: datetime
    refresh_after: datetime
    archives_found: int


async def record_completed_discovery(
    connection: AsyncConnection[TupleRow],
    *,
    request: ArchiveRequest,
    interval: AcquisitionInterval,
    archives_found: int,
    adapter_version: str,
    fresh_for: timedelta,
) -> CompletedDiscoveryScan:
    """Record a fully successful search inside the caller's transaction.

    Call only after all discovery pages have succeeded and their archive
    observations have been registered in this same transaction.

    archives_found counts distinct archives observed by this search, including
    archives already present in the registry. Zero is a valid result.

    This function does not commit or perform network requests.
    """
    if connection.info.transaction_status != TransactionStatus.INTRANS:
        raise ArchiveRegistryError("Discovery registration requires an active transaction.")

    if isinstance(archives_found, bool) or archives_found < 0:
        raise ValueError("archives_found must be a non-negative integer.")

    if not adapter_version.strip():
        raise ValueError("adapter_version must not be blank.")

    if fresh_for <= timedelta(0):
        raise ValueError("fresh_for must be greater than zero.")

    async with connection.cursor(row_factory=class_row(CompletedDiscoveryScan)) as cursor:
        await cursor.execute(
            """
            INSERT INTO historical.discovery_scans (
                exchange_id,
                scope_key,
                scope_json,
                data_type,
                searched_start_at,
                searched_end_at,
                status,
                completed_at,
                refresh_after,
                archives_found,
                adapter_version
            )
            SELECT
                exchange.id,
                %s,
                %s,
                %s,
                %s,
                %s,
                'complete',
                statement_timestamp(),
                statement_timestamp() + %s,
                %s,
                %s
            FROM catalog.exchanges AS exchange
            WHERE exchange.code = %s
            RETURNING id, completed_at, refresh_after, archives_found
            """,
            (
                archive_scope_key(request),
                Jsonb(archive_scope_document(request)),
                request.data_type,
                interval.start,
                interval.end,
                fresh_for,
                archives_found,
                adapter_version,
                request.exchange,
            ),
        )
        scan = await cursor.fetchone()

    if scan is None:
        raise ArchiveRegistryError(f"Exchange is not registered: {request.exchange}.")

    return scan


async def find_fresh_discovery(
    connection: AsyncConnection[TupleRow],
    *,
    request: ArchiveRequest,
    interval: AcquisitionInterval,
    adapter_version: str,
) -> CompletedDiscoveryScan | None:
    """Find a fresh completed scan covering the entire requested interval.

    A missing result means discovery must run. It does not mean archives
    are unavailable.

    This function does not modify the database.
    """
    if not adapter_version.strip():
        raise ValueError("adapter_version must not be blank.")

    async with connection.cursor(row_factory=class_row(CompletedDiscoveryScan)) as cursor:
        await cursor.execute(
            """
            SELECT
                scan.id,
                scan.completed_at,
                scan.refresh_after,
                scan.archives_found
            FROM historical.discovery_scans AS scan
            JOIN catalog.exchanges AS exchange
                ON exchange.id = scan.exchange_id
            WHERE exchange.code = %s
                AND scan.scope_key = %s
                AND scan.scope_json = %s
                AND scan.data_type = %s
                AND scan.adapter_version = %s
                AND scan.status = 'complete'
                AND scan.searched_start_at <= %s
                AND scan.searched_end_at >= %s
                AND scan.refresh_after > statement_timestamp()
            ORDER BY scan.completed_at DESC, scan.id DESC
            LIMIT 1
            """,
            (
                request.exchange,
                archive_scope_key(request),
                Jsonb(archive_scope_document(request)),
                request.data_type,
                adapter_version,
                interval.start,
                interval.end,
            ),
        )
        return await cursor.fetchone()
