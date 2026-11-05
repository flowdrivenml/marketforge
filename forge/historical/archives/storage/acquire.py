"""Immutable acquisitions with PostgreSQL reservations and crash recovery."""

import hashlib
import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlsplit
from uuid import uuid4

from psycopg import AsyncConnection
from psycopg.rows import TupleRow, class_row

from forge.historical.archives.registry import CachedRemoteArchive
from forge.historical.archives.storage.files import (
    CHUNK_SIZE,
    ArchiveFileError,
    contained,
    disk,
    fingerprint,
    publish,
    sync_file,
    validate_file,
)
from forge.shared.http.client import HttpClient


@dataclass(frozen=True, slots=True)
class LocalArchive:
    id: int
    relative_path: str
    status: str
    bytes_written: int | None
    sha256: str | None
    source_etag: str | None
    source_last_modified: datetime | None


@dataclass(frozen=True, slots=True)
class AcquisitionResult:
    remote_archive_id: int | None
    local_archive_id: int
    status: str
    relative_path: str
    bytes_written: int
    sha256: str
    external_archive_id: int | None = None


def root_key(root: Path) -> str:
    return hashlib.sha256(str(root.resolve()).encode()).hexdigest()


async def complete(connection: AsyncConnection[TupleRow], local_id: int) -> None:
    """Idempotent final registration; checksum and destination were reserved first."""
    async with connection.transaction():
        await connection.execute(
            """UPDATE historical.local_archives SET status = 'complete',
                downloaded_at = COALESCE(downloaded_at, statement_timestamp()),
                last_verified_at = statement_timestamp(), updated_at = statement_timestamp(),
                failure_message = NULL WHERE id = %s""",
            (local_id,),
        )


async def _status(connection: AsyncConnection[TupleRow], local_id: int, status: str) -> None:
    async with connection.transaction():
        await connection.execute(
            """UPDATE historical.local_archives SET status = %s,
                updated_at = statement_timestamp() WHERE id = %s""",
            (status, local_id),
        )


async def reuse_registered(
    connection: AsyncConnection[TupleRow],
    root: Path,
    *,
    remote_archive_id: int | None = None,
    external_archive_id: int | None = None,
    source_etag: str | None = None,
    source_last_modified: datetime | None = None,
    cleanup_partials: bool = True,
) -> AcquisitionResult | None:
    if (remote_archive_id is None) == (external_archive_id is None):
        raise ArchiveFileError("Exactly one acquisition origin is required.")
    # Keyset pagination also bounds recovery when many old revisions exist.
    after_id = 0
    while True:
        async with connection.cursor(row_factory=class_row(LocalArchive)) as cursor:
            await cursor.execute(
                """SELECT id, relative_path, status, bytes_written, sha256,
                    source_etag, source_last_modified FROM historical.local_archives
                    WHERE (remote_archive_id = %s OR external_archive_id = %s) AND storage_root_key = %s AND id > %s
                      AND status IN ('complete', 'downloading') ORDER BY id LIMIT 100""",
                (remote_archive_id, external_archive_id, root_key(root), after_id),
            )
            rows = await cursor.fetchall()
        if not rows:
            return None
        for local in rows:
            after_id = local.id
            destination = contained(root, local.relative_path)
            partial = destination.with_name(destination.name + ".part")
            ready = local.bytes_written is not None and local.sha256 is not None
            exists = await disk(destination.is_file)
            if not ready:
                if cleanup_partials:
                    await disk(partial.unlink, missing_ok=True)
                await _status(connection, local.id, "failed")
                continue
            candidate = destination if exists else partial
            if not await disk(candidate.is_file):
                await _status(connection, local.id, "missing")
                continue
            actual = await disk(fingerprint, candidate)
            if actual != (local.bytes_written, local.sha256):
                await _status(connection, local.id, "corrupt")
                if candidate == partial and cleanup_partials:
                    await disk(partial.unlink, missing_ok=True)
                continue
            if remote_archive_id is not None:
                await disk(validate_file, candidate)
            if not exists:
                await disk(publish, partial, destination)
            else:
                if cleanup_partials:
                    await disk(partial.unlink, missing_ok=True)
            await complete(connection, local.id)
            # A revision gets a new acquisition; never mutate prior checksum/path.
            if (
                local.source_etag != source_etag
                or local.source_last_modified != source_last_modified
            ):
                continue
            return AcquisitionResult(
                remote_archive_id,
                local.id,
                "reused",
                local.relative_path,
                actual[0],
                actual[1],
                external_archive_id,
            )


def download_filename(filename: str) -> str:
    """Canonical downloader destination name (also used to prove partial ownership)."""
    name = re.sub(r"[^A-Za-z0-9_.-]", "_", filename)[-160:]
    return "archive" if name in {"", ".", ".."} else name


async def acquire(
    connection: AsyncConnection[TupleRow],
    http: HttpClient,
    remote: CachedRemoteArchive,
    root: Path,
    *,
    on_activity: Callable[[str], None] | None = None,
    refresh_location: Callable[[CachedRemoteArchive], Awaitable[CachedRemoteArchive]] | None = None,
) -> AcquisitionResult:
    """Reuse verified bytes, recover published reservations, or stream a new copy.

    The DB is the only registry. Before publication, the downloading reservation
    durably records size/hash. If final registration fails, a later invocation
    validates and finalizes the reserved final/temporary file without HTTP.
    Incomplete transfers have no checksum and are discarded on recovery.
    """
    if on_activity is not None:
        on_activity("Verifying local archive")
    existing = await reuse_registered(
        connection,
        root,
        remote_archive_id=remote.id,
        source_etag=remote.source_etag,
        source_last_modified=remote.source_last_modified,
    )
    if existing is not None:
        return existing
    if remote.download_url is None or (
        remote.url_expires_at is not None and remote.url_expires_at <= datetime.now(UTC)
    ):
        if refresh_location is None:
            raise ArchiveFileError(
                "Archive location is missing/expired; refresh discovery before acquisition."
            )
        if on_activity is not None:
            on_activity("Refreshing remote location")
        remote = await refresh_location(remote)
        if remote.download_url is None or (
            remote.url_expires_at is not None and remote.url_expires_at <= datetime.now(UTC)
        ):
            raise ArchiveFileError("Refreshed archive location is still missing/expired.")
    location = urlsplit(remote.download_url)
    if (
        location.scheme != "https"
        or not location.hostname
        or location.username
        or location.password
    ):
        raise ArchiveFileError("Archive URL must be public HTTPS without user credentials.")
    filename = download_filename(remote.filename)
    relative = f"{remote.id}/{uuid4().hex}/{filename}"
    destination = contained(root, relative)
    partial = destination.with_name(destination.name + ".part")
    await disk(destination.parent.mkdir, parents=True, exist_ok=True)
    async with connection.transaction():
        cursor = await connection.execute(
            """INSERT INTO historical.local_archives (
                remote_archive_id, storage_root_key, relative_path, status,
                source_etag, source_last_modified
            ) VALUES (%s, %s, %s, 'downloading', %s, %s) RETURNING id""",
            (remote.id, root_key(root), relative, remote.source_etag, remote.source_last_modified),
        )
        row = await cursor.fetchone()
        if row is None:
            raise ArchiveFileError("Could not reserve an acquisition.")
        local_id: int = row[0]
    durable = False
    try:
        digest = hashlib.sha256()
        size = 0
        if on_activity is not None:
            on_activity("Downloading")
        async with http.stream(remote.download_url) as response:
            if response.status_code != 200:
                raise ArchiveFileError("Archive transfer requires a full HTTP 200 response.")
            content_type = response.headers.get("content-type", "").lower()
            if "html" in content_type or "application/json" in content_type:
                raise ArchiveFileError("Upstream returned an error document instead of an archive.")
            output = await disk(partial.open, "xb")
            try:
                async for chunk in response.aiter_raw(chunk_size=CHUNK_SIZE):
                    await disk(output.write, chunk)
                    digest.update(chunk)
                    size += len(chunk)
            finally:
                await disk(output.close)
        length = response.headers.get("content-length")
        if length is not None and (
            not length.isascii() or not length.isdigit() or int(length) != size
        ):
            raise ArchiveFileError("Archive transfer length does not match its response header.")
        await disk(validate_file, partial)
        await disk(sync_file, partial)
        checksum = digest.hexdigest()
        async with connection.transaction():
            await connection.execute(
                """UPDATE historical.local_archives SET bytes_written = %s, sha256 = %s,
                    updated_at = statement_timestamp() WHERE id = %s""",
                (size, checksum, local_id),
            )
        durable = True
        if on_activity is not None:
            on_activity("Publishing validated archive")
        await disk(publish, partial, destination)
        await complete(connection, local_id)
        return AcquisitionResult(remote.id, local_id, "completed", relative, size, checksum)
    except BaseException:
        if not durable:
            await disk(partial.unlink, missing_ok=True)
            await _status(connection, local_id, "failed")
        # Once metadata is durable, preserve both paths for reconciliation.
        raise
