"""Launch an external editor for an existing configuration file."""

from __future__ import annotations

import os
import shlex
import shutil
import subprocess
import termios
from contextlib import suppress
from pathlib import Path

from forge.shared.errors import MarketForgeError


class EditorError(MarketForgeError):
    """Raised when an external editor cannot complete successfully."""


def editor_command(editor: str | None = None, *, fallback: str | None = None) -> list[str]:
    """Resolve the same editor command for preflight and interactive handoff."""
    selected_editor = (
        editor
        if editor is not None
        else os.environ.get("VISUAL")
        or os.environ.get("EDITOR")
        or fallback
        or ("nvim" if shutil.which("nvim") else "vi")
    )

    try:
        command = shlex.split(selected_editor)
    except ValueError as exc:
        raise EditorError("The editor command has invalid quoting.") from exc

    if not command:
        raise EditorError("The editor command must not be empty.")

    return command


def file_command(
    path: Path,
    *,
    editor: str | None = None,
    readonly: bool = False,
    fallback: str | None = None,
    filetype: str = "",
) -> tuple[Path, list[str]]:
    """Build the editor argv after checking the file exists."""
    source = path.expanduser().resolve()

    if not source.is_file():
        raise EditorError(f"Cannot edit a missing or non-file path: {source}")

    command = editor_command(editor, fallback=fallback)
    if readonly:
        # User startup files and command-line arguments can change write policy.
        executable = command[0] if Path(command[0]).name == "nvim" else "nvim"
        command = [
            executable,
            "-u",
            "NONE",
            "-i",
            "NONE",
            "-n",
            "-R",
            "-M",
            "--cmd",
            "set nomodeline",
        ]
        if filetype in {"json", "toml"}:
            command.extend(("-c", "syntax enable", "-c", f"setfiletype {filetype}"))
        command.append("--")
    return source, [*command, str(source)]


def edit_file(
    path: Path,
    *,
    editor: str | None = None,
    readonly: bool = False,
    fallback: str | None = None,
    filetype: str = "",
) -> Path:
    """Open a file and wait for the configured editor to exit."""
    source, command = file_command(
        path, editor=editor, readonly=readonly, fallback=fallback, filetype=filetype
    )
    terminal = None
    with suppress(termios.error):
        if os.isatty(0):
            terminal = termios.tcgetattr(0)
    returncode = 0
    interrupted = False
    try:
        returncode = subprocess.run(
            command,
            check=False,
        ).returncode
    except OSError as exc:
        raise EditorError(f"Cannot launch editor {command[0]!r}: {exc}") from exc
    except KeyboardInterrupt:
        interrupted = True
        if readonly:
            raise EditorError("Neovim inspection interrupted.") from None
        raise
    finally:
        if terminal is not None:
            with suppress(termios.error):
                termios.tcsetattr(0, termios.TCSANOW, terminal)
            if os.isatty(1) and (interrupted or returncode):
                with suppress(OSError):
                    os.write(1, b"\x1b[?25h\x1b[?1049l")

    if returncode != 0:
        raise EditorError(f"Editor exited unsuccessfully with code {returncode}.")

    return source
