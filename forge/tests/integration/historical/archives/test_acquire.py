"""Stream, verify, and reconcile acquisitions using PostgreSQL and fixture HTTP."""

import asyncio
import gzip
from pathlib import Path

import httpx
import pytest
import pytest_asyncio
from psycopg import AsyncConnection
from psycopg.rows import TupleRow

import forge.historical.archives.storage.acquire as acquire_module
from forge.historical.archives.models import DiscoveredArchive
from forge.historical.archives.registry import (
    CachedRemoteArchive,
    list_available_remote_archives,
    register_remote_archive,
)
from forge.historical.archives.storage.acquire import acquire
from forge.historical.archives.storage.files import ArchiveFileError
from forge.shared.http.client import open_http_client
from forge.shared.http.rate_limit import RequestRateLimiter

pytestmark = [pytest.mark.integration, pytest.mark.database]


class ImmediateLimiter(RequestRateLimiter):
    async def wait(self) -> None:
        pass


@pytest_asyncio.fixture
async def remote(
    connection: AsyncConnection[TupleRow], archive: DiscoveredArchive
) -> CachedRemoteArchive:
    await register_remote_archive(connection, archive)
    records = await list_available_remote_archives(
        connection, request=archive.request, interval=archive.interval
    )
    return next(item for item in records if item.source_object_key == archive.source_object_key)


@pytest.mark.asyncio
async def test_repeated_acquisition_reuses_verified_file(
    connection: AsyncConnection[TupleRow], remote: CachedRemoteArchive, tmp_path: Path
) -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(
            200, stream=httpx.ByteStream(gzip.compress(b"timestamp,price\n1,10\n"))
        )

    async with open_http_client(
        limiter=ImmediateLimiter(1), transport=httpx.MockTransport(handler)
    ) as http:
        first = await acquire(connection, http, remote, tmp_path)
        second = await acquire(connection, http, remote, tmp_path)
    assert first.status == "completed"
    assert second.status == "reused"
    assert first.local_archive_id == second.local_archive_id
    assert first.sha256 == second.sha256
    assert calls == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("missing", [False, True])
async def test_corrupt_or_missing_file_is_not_reused(
    connection: AsyncConnection[TupleRow],
    remote: CachedRemoteArchive,
    tmp_path: Path,
    missing: bool,
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200, stream=httpx.ByteStream(gzip.compress(b"timestamp,price\n1,10\n"))
        )

    async with open_http_client(
        limiter=ImmediateLimiter(1), transport=httpx.MockTransport(handler)
    ) as http:
        first = await acquire(connection, http, remote, tmp_path)
        path = tmp_path / first.relative_path
        if missing:
            path.unlink()
        else:
            path.write_bytes(b"corrupt")
        second = await acquire(connection, http, remote, tmp_path)
    assert second.status == "completed"
    assert second.local_archive_id != first.local_archive_id
    assert second.relative_path != first.relative_path
    cursor = await connection.execute(
        "SELECT status FROM historical.local_archives WHERE id = %s", (first.local_archive_id,)
    )
    assert await cursor.fetchone() == ("missing" if missing else "corrupt",)


@pytest.mark.asyncio
async def test_published_file_recovers_after_registration_failure(
    connection: AsyncConnection[TupleRow],
    remote: CachedRemoteArchive,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(
            200, stream=httpx.ByteStream(gzip.compress(b"timestamp,price\n1,10\n"))
        )

    original = acquire_module.complete

    async def fail_registration(connection: AsyncConnection[TupleRow], local_id: int) -> None:
        raise RuntimeError("Injected registration failure")

    async with open_http_client(
        limiter=ImmediateLimiter(1), transport=httpx.MockTransport(handler)
    ) as http:
        monkeypatch.setattr(acquire_module, "complete", fail_registration)
        with pytest.raises(RuntimeError):
            await acquire(connection, http, remote, tmp_path)
        monkeypatch.setattr(acquire_module, "complete", original)
        recovered = await acquire(connection, http, remote, tmp_path)
    assert recovered.status == "reused"
    assert calls == 1
    assert (tmp_path / recovered.relative_path).is_file()


@pytest.mark.asyncio
async def test_error_document_never_registers_complete(
    connection: AsyncConnection[TupleRow], remote: CachedRemoteArchive, tmp_path: Path
) -> None:
    async with open_http_client(
        limiter=ImmediateLimiter(1),
        transport=httpx.MockTransport(
            lambda _: httpx.Response(
                200, stream=httpx.ByteStream(b"<!DOCTYPE html><html>denied</html>")
            )
        ),
    ) as http:
        with pytest.raises(ArchiveFileError):
            await acquire(connection, http, remote, tmp_path)
    cursor = await connection.execute(
        "SELECT status FROM historical.local_archives WHERE remote_archive_id = %s", (remote.id,)
    )
    assert await cursor.fetchall() == [("failed",)]
    assert await asyncio.to_thread(lambda: list(tmp_path.rglob("*.part"))) == []


@pytest.mark.asyncio
async def test_cancelled_body_cleans_partial_and_can_restart(
    connection: AsyncConnection[TupleRow], remote: CachedRemoteArchive, tmp_path: Path
) -> None:
    started = asyncio.Event()

    class BlockedBody(httpx.AsyncByteStream):
        async def __aiter__(self):
            started.set()
            yield b"first bytes"
            await asyncio.Event().wait()

    async with open_http_client(
        limiter=ImmediateLimiter(1),
        transport=httpx.MockTransport(lambda _: httpx.Response(200, stream=BlockedBody())),
    ) as http:
        task = asyncio.create_task(acquire(connection, http, remote, tmp_path))
        await asyncio.wait_for(started.wait(), 1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert await asyncio.to_thread(lambda: list(tmp_path.rglob("*.part"))) == []
    async with open_http_client(
        limiter=ImmediateLimiter(1),
        transport=httpx.MockTransport(
            lambda _: httpx.Response(200, stream=httpx.ByteStream(gzip.compress(b"a,b\n1,2\n")))
        ),
    ) as http:
        assert (await acquire(connection, http, remote, tmp_path)).status == "completed"


@pytest.mark.asyncio
async def test_interrupted_publication_recovers_validated_temporary_file(
    connection: AsyncConnection[TupleRow],
    remote: CachedRemoteArchive,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, stream=httpx.ByteStream(gzip.compress(b"a,b\n1,2\n")))

    original = acquire_module.publish

    def fail_publish(partial: Path, destination: Path) -> None:
        raise OSError("Injected publication interruption")

    async with open_http_client(
        limiter=ImmediateLimiter(1), transport=httpx.MockTransport(handler)
    ) as http:
        monkeypatch.setattr(acquire_module, "publish", fail_publish)
        with pytest.raises(OSError):
            await acquire(connection, http, remote, tmp_path)
        monkeypatch.setattr(acquire_module, "publish", original)
        result = await acquire(connection, http, remote, tmp_path)
    assert result.status == "reused"
    assert calls == 1


@pytest.mark.asyncio
async def test_revised_remote_etag_preserves_old_acquisition(
    connection: AsyncConnection[TupleRow], remote: CachedRemoteArchive, tmp_path: Path
) -> None:
    from dataclasses import replace

    content = b"a,b\n1,2\n"

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=httpx.ByteStream(gzip.compress(content)))

    async with open_http_client(
        limiter=ImmediateLimiter(1), transport=httpx.MockTransport(handler)
    ) as http:
        first = await acquire(connection, http, remote, tmp_path)
        original_bytes = (tmp_path / first.relative_path).read_bytes()
        content = b"a,b\n3,4\n"
        second = await acquire(
            connection, http, replace(remote, source_etag="new-revision"), tmp_path
        )
    assert first.local_archive_id != second.local_archive_id
    assert (tmp_path / first.relative_path).read_bytes() == original_bytes
    assert first.sha256 != second.sha256


@pytest.mark.asyncio
async def test_hard_interruption_discards_unverified_partial_reservation(
    connection: AsyncConnection[TupleRow], remote: CachedRemoteArchive, tmp_path: Path
) -> None:
    from forge.historical.archives.storage.acquire import root_key

    relative = "interrupted/archive.csv.gz"
    partial = tmp_path / (relative + ".part")
    await asyncio.to_thread(partial.parent.mkdir, parents=True)
    await asyncio.to_thread(partial.write_bytes, b"unfinished transfer")
    cursor = await connection.execute(
        """INSERT INTO historical.local_archives
            (remote_archive_id, storage_root_key, relative_path, status)
            VALUES (%s, %s, %s, 'downloading') RETURNING id""",
        (remote.id, root_key(tmp_path), relative),
    )
    old = await cursor.fetchone()
    assert old is not None
    async with open_http_client(
        limiter=ImmediateLimiter(1),
        transport=httpx.MockTransport(
            lambda _: httpx.Response(200, stream=httpx.ByteStream(gzip.compress(b"a,b\n1,2\n")))
        ),
    ) as http:
        result = await acquire(connection, http, remote, tmp_path)
    assert result.status == "completed"
    assert result.local_archive_id != old[0]
    assert not partial.exists()
    cursor = await connection.execute(
        "SELECT status FROM historical.local_archives WHERE id = %s", (old[0],)
    )
    assert await cursor.fetchone() == ("failed",)
