"""Bounded retained-log reads and explicit full-file export."""

import json
import os
import stat
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import BinaryIO, cast

from forge.historical.engine.runtime_config import directory_fd
from forge.shared.errors import MarketForgeError


@contextmanager
def open_log(path: Path) -> Generator[BinaryIO, None, None]:
    with directory_fd(path.parent) as directory:
        handle = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
        with os.fdopen(handle, "rb") as stream:
            if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
                raise MarketForgeError("Expected a regular retained log file.")
            yield stream


def read_lines(
    path: Path,
    first: int,
    last: int,
    *,
    search: str = "",
    severity: str = "",
    max_bytes: int = 64_000,
) -> str:
    if not 1 <= first <= last or last - first >= 1000:
        raise MarketForgeError("Select an inclusive range of 1 to 1,000 lines.")
    output = bytearray()
    with open_log(path) as stream:
        for number in range(1, last + 1):
            line = stream.readline(max_bytes + 1)
            if not line:
                break
            if len(line) > max_bytes:
                if number >= first:
                    raise MarketForgeError(
                        f"Line {number} exceeds the copy limit. Export the log for the complete line."
                    )
                while line and not line.endswith(b"\n"):
                    line = stream.readline(max_bytes + 1)
                continue
            if number < first:
                continue
            if search and search.casefold() not in line.decode(errors="replace").casefold():
                continue
            if severity:
                try:
                    event = json.loads(line)
                except (ValueError, UnicodeError):
                    continue
                if not isinstance(event, dict):
                    continue
                record = cast(dict[str, object], event)
                if (
                    str(record.get("level", record.get("severity", ""))).casefold()
                    != severity.casefold()
                ):
                    continue
            if len(output) + len(line) > max_bytes:
                raise MarketForgeError(
                    "The selected range exceeds 64,000 bytes. Narrow the range or export the full log."
                )
            output.extend(line)
    try:
        return output.decode()
    except UnicodeError:
        raise MarketForgeError(
            "The selected bytes are not UTF-8. Export the log for an exact byte copy."
        ) from None


def export_log(source: Path, destination: Path) -> None:
    """Copy retained bytes to a new, owner-readable file without overwriting."""
    with open_log(source) as stream, TemporaryDirectory(dir=destination.parent) as directory:
        candidate = Path(directory) / "log"
        with candidate.open("xb") as output:
            os.fchmod(output.fileno(), 0o600)
            size = os.fstat(stream.fileno()).st_size
            remaining = size
            while remaining:
                block = stream.read(min(remaining, 1024 * 1024))
                if not block:
                    raise MarketForgeError("Log changed while being exported. Retry the export.")
                output.write(block)
                remaining -= len(block)
            output.flush()
            os.fsync(output.fileno())
        os.link(candidate, destination)
