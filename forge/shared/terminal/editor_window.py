"""Supervise a Ghostty editor independently of the TUI and its engine workers."""

import asyncio
import subprocess
import sys
from collections.abc import Callable
from contextlib import suppress
from pathlib import Path
from tempfile import mkdtemp

from forge.shared.terminal.editor import EditorError, file_command
from forge.shared.terminal.window import GhosttyWindow, Window, WindowUnavailable


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
        raise EditorError(str(error)) from error

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
        deadline = asyncio.get_running_loop().time() + 15
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
                if status.lstrip("-").isdigit():
                    raise EditorError(f"Editor exited with status {status}.")
                raise EditorError(status)
            if waiting.done():
                if result_path.is_file():
                    continue
                if ready:
                    raise EditorError("The editor window closed without a completion receipt.")
                raise EditorError(
                    f"Ghostty exited ({waiting.result()}) before starting the editor. "
                    "Check your Wayland or X11 display and the environment inherited by tmux."
                )
            if not ready and asyncio.get_running_loop().time() >= deadline:
                raise EditorError("Ghostty did not start the editor in time.")
            await asyncio.wait({waiting}, timeout=0.1)
    except (OSError, subprocess.SubprocessError) as error:
        raise EditorError(f"Could not open the Ghostty editor window: {error}") from error
    finally:
        if window is not None:
            if waiting is None:
                waiting = asyncio.create_task(window.wait())
            if finished:
                # Ghostty normally closes itself as the supervisor exits.
                await asyncio.wait({waiting}, timeout=2)
            if readonly or finished or not (ready or ready_path.is_file()):
                await stop_window(window, waiting)
