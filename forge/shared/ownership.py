"""Exclusive historical application ownership on Linux and macOS."""

from __future__ import annotations

import fcntl
import os
import stat
from pathlib import Path
from types import TracebackType
from typing import Self, TextIO

from forge.shared.errors import MarketForgeError
from forge.shared.paths import ProjectPaths


class OwnershipError(MarketForgeError, RuntimeError):
    """Raised when application ownership cannot be acquired."""


class OwnershipConflictError(OwnershipError):
    """Raised when another owner holds a required lock."""


def acquire_file_lock(path: Path) -> TextIO:
    """Open and exclusively lock a persistent lock file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND | os.O_NOFOLLOW, 0o600)
    if not stat.S_ISREG(os.fstat(descriptor).st_mode):
        os.close(descriptor)
        raise OwnershipError(f"Lock must be a regular file: {path}")
    handle = os.fdopen(descriptor, "a", encoding="utf-8")

    try:
        fcntl.flock(
            handle.fileno(),
            fcntl.LOCK_EX | fcntl.LOCK_NB,
        )
    except BlockingIOError as exc:
        handle.close()
        raise OwnershipConflictError(
            f"Another historical application instance holds: {path}"
        ) from exc
    except BaseException:
        handle.close()
        raise

    return handle


class WorkspaceOwnership:
    """Hold exclusive workspace and storage ownership.

    Acquisition is nonblocking. If any lock fails, locks already acquired
    by this object are released.

    Lock files must not be deleted or replaced while applications run.
    """

    def __init__(self, paths: ProjectPaths) -> None:
        self._lock_paths = tuple(
            dict.fromkeys(
                (
                    paths.root / ".marketforge.lock",
                    paths.data_root / ".marketforge.lock",
                ),
            ),
        )
        self._handles: list[TextIO] = []

    def acquire(self) -> None:
        """Acquire all required locks."""
        if self._handles:
            raise OwnershipError("This ownership handle is already acquired.")

        try:
            for path in self._lock_paths:
                self._handles.append(acquire_file_lock(path))
        except BaseException:
            self.release()
            raise

    def descriptors(self) -> tuple[int, ...]:
        """Held open-file descriptions for a cooperating local worker."""
        if len(self._handles) != len(self._lock_paths):
            raise OwnershipError("Workspace ownership must be acquired before delegation.")
        return tuple(handle.fileno() for handle in self._handles)

    def release(self) -> None:
        """Release ownership by closing its file handles."""
        while self._handles:
            self._handles.pop().close()

    def __enter__(self) -> Self:
        self.acquire()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.release()
