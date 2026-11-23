"""Add a boundary around visible CLI output on each interactive terminal."""

import os
import re
import sys
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import TextIO, cast

_CONTROL = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]|\x1b\][^\x07\x1b]*(?:\x07|\x1b\\)")


@dataclass
class _Boundary:
    last_stream: TextIO | None = None
    newlines: int = 0

    def finish(self) -> None:
        if self.last_stream is not None:
            self.last_stream.write("\n" * max(0, 2 - self.newlines))
            self.last_stream.flush()


class _TerminalStream:
    """Forward terminal controls directly and track visible writes."""

    def __init__(self, stream: TextIO, boundary: _Boundary) -> None:
        self._stream = stream
        self._boundary = boundary

    def __getattr__(self, name: str) -> object:
        return getattr(self._stream, name)

    def write(self, text: str) -> int:
        visible = _CONTROL.sub("", text).replace("\r", "")
        boundary = self._boundary
        if visible:
            if boundary.last_stream is None and not visible.startswith("\n"):
                self._stream.write("\n")
            boundary.last_stream = self._stream
            trailing = len(visible) - len(visible.rstrip("\n"))
            boundary.newlines = (
                boundary.newlines + trailing if not visible.rstrip("\n") else trailing
            )
        return self._stream.write(text)


@contextmanager
def terminal_spacing(*, enabled: bool) -> Generator[None]:
    """Keep redirected streams, machine output and silent invocations untouched."""
    stdout, stderr = sys.stdout, sys.stderr
    boundaries: dict[str | int, _Boundary] = {}

    def wrap(stream: TextIO) -> TextIO:
        if not stream.isatty():
            return stream
        device: str | int
        try:
            device = os.ttyname(stream.fileno())
        except (OSError, ValueError):
            device = id(stream)
        return cast(TextIO, _TerminalStream(stream, boundaries.setdefault(device, _Boundary())))

    if not enabled:
        yield
        return
    sys.stdout, sys.stderr = wrap(stdout), wrap(stderr)
    try:
        yield
    finally:
        sys.stdout, sys.stderr = stdout, stderr
        for boundary in boundaries.values():
            boundary.finish()
