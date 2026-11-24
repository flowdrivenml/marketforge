"""Run an editor on the graphical window's PTY and publish its outcome."""

import os
import subprocess
import sys
from pathlib import Path


def receipt(directory: Path, name: str, value: str) -> None:
    scratch = directory / f"{name}.tmp"
    scratch.write_text(value, encoding="utf-8")
    scratch.replace(directory / name)


def main() -> None:
    directory = Path(sys.argv[1])
    if not all(os.isatty(fd) for fd in (0, 1, 2)):
        receipt(directory, "result", "Ghostty did not provide a terminal to the editor.")
        return
    try:
        with subprocess.Popen(sys.argv[2:]) as editor:
            receipt(directory, "ready", str(editor.pid))
            status = editor.wait()
        result = str(status)
    except OSError as exc:
        result = f"Could not start the editor: {exc}"
    receipt(directory, "result", result)


if __name__ == "__main__":
    main()
