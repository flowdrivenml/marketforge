"""Descriptor-relative inspection/unlink of regular, uniquely owned managed files."""

import os
import stat
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path, PurePosixPath

from pydantic import BaseModel, ConfigDict

from forge.shared.errors import MarketForgeError


class FileChanged(MarketForgeError):
    """The reviewed path or inode can no longer be safely removed."""


class FileSnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    exists: bool
    device: int | None = None
    inode: int | None = None
    size: int = 0
    mtime_ns: int | None = None
    ctime_ns: int | None = None
    mode: int | None = None
    links: int | None = None
    parent_device: int | None = None
    parent_inode: int | None = None


def relative_parts(relative: str) -> tuple[str, ...]:
    parts = PurePosixPath(relative).parts
    if (
        not parts
        or relative.startswith("/")
        or "\\" in relative
        or any(p in {".", "..", ""} for p in relative.split("/"))
    ):
        raise FileChanged("Path is not a managed relative file path.")
    return parts


@contextmanager
def parent_directory(root: Path, relative: str) -> Generator[tuple[int, str], None, None]:
    parts = relative_parts(relative)
    descriptor = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        for part in parts[:-1]:
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=descriptor)
            os.close(descriptor)
            descriptor = child
        yield descriptor, parts[-1]
    finally:
        os.close(descriptor)


def snapshot(root: Path, relative: str) -> FileSnapshot:
    try:
        with parent_directory(root, relative) as (parent, name):
            directory = os.fstat(parent)
            try:
                info = os.stat(name, dir_fd=parent, follow_symlinks=False)
            except FileNotFoundError:
                return FileSnapshot(
                    exists=False, parent_device=directory.st_dev, parent_inode=directory.st_ino
                )
            if not stat.S_ISREG(info.st_mode):
                raise FileChanged(
                    "Target is a symlink or non-regular file; ownership is not established."
                )
            if info.st_nlink != 1:
                raise FileChanged(
                    "Target has multiple hard links; original-source ownership is not established."
                )
            return FileSnapshot(
                exists=True,
                device=info.st_dev,
                inode=info.st_ino,
                size=info.st_size,
                mtime_ns=info.st_mtime_ns,
                ctime_ns=info.st_ctime_ns,
                mode=info.st_mode,
                links=info.st_nlink,
                parent_device=directory.st_dev,
                parent_inode=directory.st_ino,
            )
    except FileNotFoundError:
        return FileSnapshot(exists=False)
    except OSError as error:
        raise FileChanged(
            "Managed path could not be opened without symlinks; inspect ownership and permissions."
        ) from error


def unlink_reviewed(root: Path, relative: str, expected: FileSnapshot) -> None:
    if not expected.exists or snapshot(root, relative) != expected:
        raise FileChanged("File or parent identity changed since preview; nothing removed.")
    with parent_directory(root, relative) as (parent, name):
        directory = os.fstat(parent)
        if (directory.st_dev, directory.st_ino) != (expected.parent_device, expected.parent_inode):
            raise FileChanged("Managed parent directory changed since preview.")
        descriptor = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
        try:
            current = os.fstat(descriptor)
            if (
                current.st_dev,
                current.st_ino,
                current.st_size,
                current.st_mtime_ns,
                current.st_ctime_ns,
                current.st_nlink,
            ) != (
                expected.device,
                expected.inode,
                expected.size,
                expected.mtime_ns,
                expected.ctime_ns,
                expected.links,
            ):
                raise FileChanged("Opened file changed since preview.")
            # Walk the managed path again before unlinking through the pinned parent.
            if snapshot(root, relative) != expected:
                raise FileChanged("File ownership changed immediately before removal.")
            os.unlink(name, dir_fd=parent)
            os.fsync(parent)
        finally:
            os.close(descriptor)
