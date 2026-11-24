"""Start isolated graphical terminals for Forge-owned window commands."""

import asyncio
import os
import shutil
import subprocess
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, Self, TypedDict

from forge.shared.errors import MarketForgeError


class WindowUnavailable(MarketForgeError):
    """A separate terminal window could not start."""


class Window(Protocol):
    async def wait(self) -> int: ...
    def terminate(self) -> None: ...
    def kill(self) -> None: ...


@dataclass(frozen=True)
class DetachedWindow:
    """Reap a live window without tying its lifetime to an asyncio transport."""

    process: subprocess.Popen[bytes]

    async def wait(self) -> int:
        while (status := self.process.poll()) is None:  # noqa: ASYNC110
            await asyncio.sleep(0.05)
        return status

    def terminate(self) -> None:
        self.process.terminate()

    def kill(self) -> None:
        self.process.kill()


class SpawnOptions(TypedDict):
    cwd: Path
    env: dict[str, str]
    stdin: int
    stdout: int
    stderr: int
    start_new_session: bool


@dataclass(frozen=True)
class GhosttyWindow:
    executable: str
    name = "Ghostty"

    @classmethod
    def detect(cls) -> Self:
        if sys.platform == "darwin":
            raise WindowUnavailable(
                "Managed graphical windows currently require Linux Ghostty (macOS is unsupported)."
            )
        if sys.platform != "linux" or not (
            os.environ.get("WAYLAND_DISPLAY") or os.environ.get("DISPLAY")
        ):
            raise WindowUnavailable(
                "No Linux graphical display is available. In tmux, start a shell with the current desktop environment."
            )
        executable = shutil.which("ghostty")
        if not executable:
            raise WindowUnavailable("Ghostty was not found on PATH.")
        try:
            probe = subprocess.run(
                [executable, "+show-config", "--default"],
                capture_output=True,
                text=True,
                timeout=3,
                check=True,
            )
        except (OSError, subprocess.SubprocessError):
            raise WindowUnavailable(
                "Cannot inspect Ghostty's window options. Check the installation."
            ) from None
        options = (
            "gtk-single-instance",
            "initial-window",
            "wait-after-command",
            "quit-after-last-window-closed",
            "abnormal-command-exit-runtime",
        )
        if not all(f"{option} =" in probe.stdout for option in options):
            raise WindowUnavailable("This Ghostty build lacks the required GTK window options.")
        return cls(executable)

    async def launch_command(
        self, argv: Sequence[str], *, cwd: Path, title: str, detached_lifetime: bool = False
    ) -> Window:
        # Isolate this window from any existing Ghostty instance or tmux session.
        env = {
            key: value
            for key, value in os.environ.items()
            if key not in {"TMUX", "TMUX_PANE", "MARKETFORGE_TUI_HOLD"}
        }
        command = (
            self.executable,
            "--config-default-files=false",
            "--gtk-single-instance=false",
            "--initial-window=true",
            "--wait-after-command=false",
            "--quit-after-last-window-closed=true",
            "--abnormal-command-exit-runtime=0",
            f"--working-directory={cwd}",
            f"--title={title}",
            "-e",
            *argv,
        )
        options = SpawnOptions(
            cwd=cwd,
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        if detached_lifetime:
            return DetachedWindow(await asyncio.to_thread(subprocess.Popen, command, **options))
        return await asyncio.create_subprocess_exec(*command, **options)
