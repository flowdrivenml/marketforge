"""Async ownership of noninteractive historical engine processes."""

import asyncio
import contextlib
import os
import signal
import subprocess
import sys
import threading
from collections.abc import Awaitable, Callable, Generator
from pathlib import Path

from forge.historical.archives.storage.files import disk as joined_io
from forge.shared.errors import MarketForgeError

Lifecycle = Callable[[str], None]
EngineRunner = Callable[[list[str], Path, Path, bool], Awaitable[tuple[int, bool]]]
PreparationObserver = Callable[[Path, Path], None]


class EngineOutcomeUnknown(MarketForgeError):
    """Keep the work claim for reconciliation when an Engine may have run."""


def ignore_preparation(_job: Path, _report: Path) -> None:
    """Default observer for callers that do not retain a pipeline checkpoint."""


async def supervise(
    argv: list[str],
    root: Path,
    directory: Path,
    *,
    grace: float = 10,
    log_prefix: str = "engine",
    tui: bool = False,
    foreground: bool = False,
) -> tuple[int, bool]:
    """Keep process groups and log readers within the lifetime of the caller."""
    launch = asyncio.create_task(
        asyncio.create_subprocess_exec(
            *argv,
            *(["--tui"] if tui else []),
            cwd=root,
            stdin=None if tui else asyncio.subprocess.DEVNULL,
            stdout=None if tui else asyncio.subprocess.PIPE,
            stderr=None if tui else asyncio.subprocess.PIPE,
            start_new_session=not tui,
        )
    )
    interrupted = False
    try:
        process = await asyncio.shield(launch)
    except asyncio.CancelledError:
        process = await launch
        interrupted = True

    async def capture(stream: asyncio.StreamReader, name: str) -> None:
        remaining = 16 * 1024 * 1024
        failure: OSError | None = None
        output = None
        try:
            try:
                output = (directory / name).open("wb")
            except OSError as error:
                failure = error
            while chunk := await stream.read(65536):
                if foreground:
                    display = sys.stderr.buffer if "stderr" in name else sys.stdout.buffer
                    with contextlib.suppress(OSError):
                        display.write(chunk)
                        display.flush()
                if remaining and output is not None and failure is None:
                    try:
                        remaining -= output.write(chunk[:remaining])
                    except OSError as error:
                        failure = error
            if failure:
                raise failure
        finally:
            if output is not None:
                output.close()

    streams = ((process.stdout, "stdout"), (process.stderr, "stderr"))
    readers = asyncio.gather(
        *(
            capture(stream, f"{log_prefix}-{name}.log")
            for stream, name in streams
            if stream is not None
        ),
        return_exceptions=True,
    )
    waiting = asyncio.create_task(process.wait())

    def signal_group(number: int) -> None:
        with contextlib.suppress(ProcessLookupError):
            if tui:
                process.send_signal(number)
            else:
                os.killpg(process.pid, number)

    try:
        if not interrupted:
            await asyncio.shield(waiting)
            await asyncio.shield(readers)
    except asyncio.CancelledError:
        interrupted = True
    if interrupted:
        signal_group(signal.SIGTERM)
        try:
            async with asyncio.timeout(grace):
                await asyncio.shield(waiting)
                await asyncio.shield(readers)
        except TimeoutError:
            signal_group(signal.SIGKILL)
            await waiting
            await readers
    for error in readers.result():
        if isinstance(error, OSError):
            raise error
    code = waiting.result()
    return code, interrupted or code in {
        130,
        -signal.SIGINT,
        -signal.SIGTERM,
        -signal.SIGKILL,
        -signal.SIGHUP,
    }


async def check(
    argv: list[str], root: Path, directory: Path, label: str
) -> subprocess.CompletedProcess[str]:
    """Run a bounded engine preflight without blocking presentation or losing diagnostics."""
    async with asyncio.timeout(30) as duration:
        code, interrupted = await supervise(argv, root, directory, log_prefix=label)
    if duration.expired():
        raise MarketForgeError(f"Engine {label} timed out; inspect {directory}.")
    if interrupted:
        raise asyncio.CancelledError
    output = await joined_io(
        lambda: (directory / f"{label}-stdout.log").read_text(errors="replace")
    )
    if code:
        errors = await joined_io(
            lambda: (directory / f"{label}-stderr.log").read_text(errors="replace")
        )
        output += errors
    return subprocess.CompletedProcess(argv, code, output)


class ProcessControl:
    """Track a subprocess owned by an existing synchronous service."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._child: subprocess.Popen[str] | subprocess.Popen[bytes] | None = None
        self._signal: int | None = None

    @contextlib.contextmanager
    def track(self, child: subprocess.Popen[str] | subprocess.Popen[bytes]) -> Generator[None]:
        with self._lock:
            self._child = child
            if self._signal is not None:
                self._signal_child(self._signal)
        try:
            yield
        finally:
            with self._lock:
                self._child = None

    def signal(self, number: int) -> None:
        with self._lock:
            self._signal = number
            self._signal_child(number)

    def _signal_child(self, number: int) -> None:
        if self._child is not None:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(self._child.pid, number)


async def controlled_thread[T](operation: Callable[[ProcessControl], T]) -> T:
    """Cancel a registered process group and join its synchronous owner."""
    control = ProcessControl()
    worker = asyncio.create_task(asyncio.to_thread(operation, control))
    try:
        return await asyncio.shield(worker)
    except asyncio.CancelledError:
        control.signal(signal.SIGTERM)
        try:
            async with asyncio.timeout(5):
                await asyncio.shield(worker)
        except TimeoutError:
            control.signal(signal.SIGKILL)
            with contextlib.suppress(Exception):
                await worker
        except Exception:
            pass
        raise
