"""Async ownership of noninteractive historical engine processes."""

import asyncio
import contextlib
import os
import signal
import subprocess
import sys
import threading
from collections.abc import Awaitable, Callable, Generator
from contextvars import ContextVar
from dataclasses import dataclass, field
from pathlib import Path

from forge.historical.archives.storage.files import disk as joined_io
from forge.shared.errors import MarketForgeError

Lifecycle = Callable[[str], None]
EngineRunner = Callable[[list[str], Path, Path, bool], Awaitable[tuple[int, bool]]]
PreparationObserver = Callable[[Path, Path], None]


@dataclass
class Shutdown:
    """Cancellation timing shared by the current operation and its supervisors."""

    emergency: bool = False
    unverified: bool = False
    children: set[Callable[[], bool]] = field(default_factory=set[Callable[[], bool]])

    def process_count(self) -> int:
        return sum(alive() for alive in tuple(self.children))


shutdown: ContextVar[Shutdown | None] = ContextVar("operation_shutdown", default=None)


def cancellation_grace(normal: float, emergency: float = 0.5) -> float:
    state = shutdown.get()
    return emergency if state and state.emergency else normal


def track_until_exit(process: asyncio.subprocess.Process, waiting: asyncio.Task[int]) -> None:
    if (state := shutdown.get()) is not None:

        def alive() -> bool:
            return process.returncode is None

        state.children.add(alive)
        waiting.add_done_callback(lambda _: state.children.discard(alive))


def require_dispatch() -> None:
    if (state := shutdown.get()) is not None and state.emergency:
        raise asyncio.CancelledError


def signal_isolated_child(pid: int, number: int) -> None:
    """Signal a directly owned session leader and its isolated process group."""
    with contextlib.suppress(ProcessLookupError, ChildProcessError):
        if hasattr(os, "waitid"):
            # WNOWAIT leaves reaping to the subprocess transport or Popen owner.
            exited = os.waitid(os.P_PID, pid, os.WEXITED | os.WNOHANG | os.WNOWAIT)
            if exited is not None:
                return
        if os.getpgid(pid) == pid and os.getsid(pid) == pid and os.getpgrp() != pid:
            os.killpg(pid, number)


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
    require_dispatch()
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
    track_until_exit(process, waiting)

    def signal_group(number: int) -> None:
        with contextlib.suppress(ProcessLookupError):
            if process.returncode is not None:
                return
            if tui:
                process.send_signal(number)
            else:
                signal_isolated_child(process.pid, number)

    try:
        if not interrupted:
            await asyncio.shield(waiting)
            await asyncio.shield(readers)
    except asyncio.CancelledError:
        interrupted = True
    if interrupted:
        signal_group(signal.SIGTERM)
        try:
            async with asyncio.timeout(cancellation_grace(grace)):
                await asyncio.shield(waiting)
                await asyncio.shield(readers)
        except TimeoutError:
            signal_group(signal.SIGKILL)
            try:
                async with asyncio.timeout(cancellation_grace(grace)):
                    await asyncio.shield(waiting)
                    await asyncio.shield(readers)
            except TimeoutError:
                if (state := shutdown.get()) is not None and state.emergency:
                    state.unverified = True
                readers.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await readers
                raise EngineOutcomeUnknown(
                    f"Engine shutdown could not be verified. Inspect retained logs in {directory}."
                ) from None
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
        self._state = shutdown.get()
        self._lock = threading.Lock()
        self._child: subprocess.Popen[str] | subprocess.Popen[bytes] | None = None
        self._signal: int | None = None

    @contextlib.contextmanager
    def track(self, child: subprocess.Popen[str] | subprocess.Popen[bytes]) -> Generator[None]:
        with self._lock:
            self._child = child
            if self._state is not None:
                self._state.children.add(self.child_running)
            if self._signal is not None:
                self._signal_child(self._signal)
        try:
            yield
        finally:
            with self._lock:
                self._child = None
                if self._state is not None:
                    self._state.children.discard(self.child_running)

    def child_running(self) -> bool:
        with self._lock:
            return self._child is not None and self._child.returncode is None

    def signal(self, number: int) -> None:
        with self._lock:
            self._signal = number
            self._signal_child(number)

    def _signal_child(self, number: int) -> None:
        if self._child is not None and self._child.poll() is None:
            signal_isolated_child(self._child.pid, number)


async def controlled_thread[T](operation: Callable[[ProcessControl], T]) -> T:
    """Cancel a registered process group and join its synchronous owner."""
    require_dispatch()
    control = ProcessControl()
    worker = asyncio.create_task(asyncio.to_thread(operation, control))
    try:
        return await asyncio.shield(worker)
    except asyncio.CancelledError:
        control.signal(signal.SIGTERM)
        try:
            async with asyncio.timeout(cancellation_grace(5)):
                await asyncio.shield(worker)
        except TimeoutError:
            control.signal(signal.SIGKILL)
            if cancellation_grace(0, 1):
                await asyncio.wait({worker}, timeout=0.5)
            else:
                with contextlib.suppress(Exception):
                    await worker
        except Exception:
            pass
        raise
