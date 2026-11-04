"""Exclusive historical application ownership on Linux and macOS."""

from __future__ import annotations

import fcntl
from pathlib import Path
from types import TracebackType
from typing import Self, TextIO

from forge.shared.errors import MarketForgeError
from forge.shared.paths import ProjectPaths


class OwnershipError(MarketForgeError, RuntimeError):
    """Raised when application ownership cannot be acquired."""


class OwnershipConflictError(OwnershipError):
    """Raised when another owner holds a required lock."""


def _acquire_file(path: Path) -> TextIO:
    """Open and exclusively lock a persistent lock file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("a", encoding="utf-8")

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
                self._handles.append(_acquire_file(path))
        except BaseException:
            self.release()
            raise

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
