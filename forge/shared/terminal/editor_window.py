"""Supervise a Ghostty editor independently of the TUI and its engine workers."""

import asyncio
import signal
import subprocess
import sys
from collections.abc import Callable
from contextlib import suppress
from pathlib import Path
from tempfile import mkdtemp

from forge.shared.terminal.editor import EditorError, file_command
from forge.shared.terminal.window import GhosttyWindow, Window, WindowUnavailable


class EditorCompletionUnconfirmed(EditorError):
    """A started editor supplied no trustworthy exit status."""


class EditorInterrupted(EditorError):
    """The editor exited due to a signal."""


class EditorLaunchFailed(EditorError):
    """The editor or its terminal could not start."""


async def stop_window(window: Window, waiting: asyncio.Task[int]) -> None:
    if not waiting.done():
        with suppress(ProcessLookupError):
            window.terminate()
        try:
            await asyncio.wait_for(asyncio.shield(waiting), 2)
        except TimeoutError:
            with suppress(ProcessLookupError):
                window.kill()
            await waiting


async def edit_in_window(
    path: Path,
    *,
    cwd: Path,
    editor: str | None,
    readonly: bool = False,
    filetype: str = "",
    on_ready: Callable[[], None] | None = None,
) -> None:
    try:
        launcher = await asyncio.to_thread(GhosttyWindow.detect)
        _, argv = await asyncio.to_thread(
            file_command, path, editor=editor, fallback="nvim", readonly=readonly, filetype=filetype
        )
    except WindowUnavailable as error:
        raise EditorLaunchFailed(str(error)) from error

    # Keep receipts next to the draft while the editor is active. The worker can
    # still report completion if the TUI exits during emergency shutdown.
    receipt = Path(mkdtemp(prefix=".window-", dir=path.parent))
    launch = asyncio.create_task(
        launcher.launch_command(
            (
                sys.executable,
                str(Path(__file__).with_name("editor_worker.py").resolve()),
                str(receipt),
                *argv,
            ),
            cwd=cwd,
            title=f"MarketForge · {'Inspect' if readonly else 'Edit'} · {path.name}",
            detached_lifetime=not readonly,
        )
    )
    ready_path = receipt / "ready"
    result_path = receipt / "result"
    window: Window | None = None
    waiting: asyncio.Task[int] | None = None
    ready = False
    finished = False
    try:
        try:
            window = await asyncio.shield(launch)
        except asyncio.CancelledError:
            window = await launch
            raise
        assert window is not None
        waiting = asyncio.create_task(window.wait())
        loop = asyncio.get_running_loop()
        deadline = loop.time() + 15
        reconcile: float | None = None
        while True:
            # Observe the result first. A fast editor can publish both receipts
            # between checks, and the supervisor always publishes ready first.
            finished = result_path.is_file()
            if not ready and ready_path.is_file():
                ready = True
                if on_ready is not None:
                    on_ready()
            if finished:
                status = result_path.read_text(encoding="utf-8")
                if status == "0" and ready:
                    return
                if ready and status.startswith("interrupted:"):
                    _, number, code = status.split(":", 2)
                    raise EditorInterrupted(
                        f"Editor interrupted by signal {number} (status {code})."
                    )
                if status.lstrip("-").isdigit() and ready:
                    code = int(status)
                    if code < 0:
                        raise EditorInterrupted(f"Editor interrupted by signal {-code}.")
                    if code in (128 + signal.SIGINT, 128 + signal.SIGHUP, 128 + signal.SIGTERM):
                        raise EditorInterrupted(f"Editor interrupted (status {code}).")
                    raise EditorError(f"Editor exited with status {code}.")
                if not ready:
                    raise EditorLaunchFailed(status)
                raise EditorError(status)
            now = loop.time()
            if waiting.done():
                # Ghostty's lifetime is separate from the editor worker's.
                if reconcile is None:
                    reconcile = now + 1
                if now >= reconcile:
                    if ready:
                        raise EditorCompletionUnconfirmed(
                            "The editor window closed and its exit status could not be verified."
                        )
                    raise EditorLaunchFailed(
                        f"Ghostty exited ({waiting.result()}) before starting the editor. "
                        "Check your Wayland or X11 display and the environment inherited by tmux."
                    )
                await asyncio.sleep(min(0.05, reconcile - now))
            elif not ready and now >= deadline:
                raise EditorLaunchFailed("Ghostty did not start the editor in time.")
            else:
                await asyncio.wait({waiting}, timeout=0.1)
    except (OSError, subprocess.SubprocessError) as error:
        if ready:
            raise EditorCompletionUnconfirmed(
                f"Could not verify the editor receipt: {error}"
            ) from error
        raise EditorLaunchFailed(f"Could not open the Ghostty editor window: {error}") from error
    finally:
        if window is not None:
            if waiting is None:
                waiting = asyncio.create_task(window.wait())
            if finished:
                # Ghostty normally closes itself as the supervisor exits.
                await asyncio.wait({waiting}, timeout=2)
            if readonly or finished or not (ready or ready_path.is_file()):
                await stop_window(window, waiting)
