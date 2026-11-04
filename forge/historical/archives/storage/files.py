"""Bounded file I/O, archive validation, and durable atomic publication."""

import asyncio
import gzip
import hashlib
import os
import struct
import zipfile
import zlib
from collections.abc import Callable
from pathlib import Path

from forge.shared.errors import MarketForgeError

CHUNK_SIZE = 1024 * 1024


class ArchiveFileError(MarketForgeError):
    """Acquired bytes are missing, corrupt, or not an archive."""


async def disk[**P, T](function: Callable[P, T], *args: P.args, **kwargs: P.kwargs) -> T:
    """Join an in-progress filesystem call before propagating cancellation."""
    task = asyncio.create_task(asyncio.to_thread(function, *args, **kwargs))
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        await task
        raise


def fingerprint(path: Path) -> tuple[int, str]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as stream:
        while chunk := stream.read(CHUNK_SIZE):
            digest.update(chunk)
            size += len(chunk)
    return size, digest.hexdigest()


def _check_sample(sample: bytes) -> None:
    stripped = sample.lstrip().lower()
    if not stripped or stripped.startswith((b"<", b'{"error', b'{"code', b'{"retcode')):
        raise ArchiveFileError("Empty archive or upstream error document received.")


def validate_file(path: Path) -> None:
    """Validate compression integrity and reject empty/HTML error documents.

    Does not extract ZIP members to disk or interpret market-data schemas.
    """
    with path.open("rb") as stream:
        magic = stream.read(4)
    try:
        if magic.startswith(b"PK\x03\x04"):
            # ZipFile buffers the central directory; bound it before opening.
            with path.open("rb") as stream:
                size = stream.seek(0, os.SEEK_END)
                stream.seek(max(0, size - 65557))
                tail = stream.read(65557)
            offset = tail.rfind(b"PK\x05\x06")
            if offset < 0 or len(tail) - offset < 22:
                raise ArchiveFileError("ZIP has no complete directory record.")
            entries = struct.unpack_from("<H", tail, offset + 10)[0]
            directory_size = struct.unpack_from("<I", tail, offset + 12)[0]
            if entries > 10000 or directory_size > 8 * CHUNK_SIZE:
                raise ArchiveFileError("ZIP directory exceeds acquisition metadata limits.")
            with zipfile.ZipFile(path) as archive:
                members = [member for member in archive.infolist() if not member.is_dir()]
                if not members:
                    raise ArchiveFileError("Archive contains no files.")
                for member in members:
                    with archive.open(member) as stream:
                        _check_sample(stream.read(4096))
                        while stream.read(CHUNK_SIZE):
                            pass
        elif magic.startswith(b"\x1f\x8b"):
            with gzip.open(path, "rb") as stream:
                _check_sample(stream.read(4096))
                while stream.read(CHUNK_SIZE):
                    pass
        else:
            # Plain CSV is published by some Bybit trade listings.
            with path.open("rb") as stream:
                sample = stream.read(4096)
            _check_sample(sample)
            if b"\x00" in sample or (b"," not in sample and b"\t" not in sample):
                raise ArchiveFileError(
                    "Response is not a supported ZIP, gzip, or delimited archive."
                )
    except (zipfile.BadZipFile, EOFError, OSError, RuntimeError, zlib.error):
        raise ArchiveFileError("Archive compression/integrity validation failed.") from None


def sync_file(path: Path) -> None:
    with path.open("rb") as stream:
        os.fsync(stream.fileno())


def publish(partial: Path, destination: Path) -> None:
    """Publish without replacing anything, then fsync the directory.

    Both paths are in the same directory. A crash may leave both hard links;
    recovery validates the final file and removes the temporary link.
    """
    os.link(partial, destination)
    descriptor = os.open(destination.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
        partial.unlink(missing_ok=True)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def contained(root: Path, relative: str) -> Path:
    path = root / relative
    if path.is_symlink() or not path.resolve().is_relative_to(root.resolve()):
        raise ArchiveFileError("Registry location escapes the configured raw root.")
    return path
