"""Launch an external editor for an existing configuration file."""

from __future__ import annotations

import os
import shlex
import subprocess
from pathlib import Path

from forge.shared.errors import MarketForgeError


class EditorError(MarketForgeError):
    """Raised when an external editor cannot complete successfully."""


def edit_file(path: Path, *, editor: str | None = None) -> Path:
    """Open a file and wait for the configured editor to exit."""
    source = path.expanduser().resolve()

    if not source.is_file():
        raise EditorError(f"Cannot edit a missing or non-file path: {source}")

    selected_editor = (
        editor
        if editor is not None
        else os.environ.get("VISUAL") or os.environ.get("EDITOR") or "vi"
    )

    try:
        command = shlex.split(selected_editor)
    except ValueError as exc:
        raise EditorError("The editor command has invalid quoting.") from exc

    if not command:
        raise EditorError("The editor command must not be empty.")

    try:
        result = subprocess.run(
            [*command, str(source)],
            check=False,
        )
    except OSError as exc:
        raise EditorError(f"Cannot launch editor {command[0]!r}: {exc}") from exc

    if result.returncode != 0:
        raise EditorError(f"Editor exited unsuccessfully with code {result.returncode}.")

    return source
