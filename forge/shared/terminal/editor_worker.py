"""Run an editor on the graphical window's PTY and publish its outcome."""

import os
import signal
import subprocess
import sys
from contextlib import suppress
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
    interrupted: int | None = None
    editor: subprocess.Popen[bytes] | None = None

    def interrupt(number: int, _frame: object) -> None:
        nonlocal interrupted
        interrupted = number
        if editor is not None and editor.returncode is None:
            with suppress(ProcessLookupError):
                os.kill(editor.pid, number)

    for number in (signal.SIGHUP, signal.SIGINT, signal.SIGTERM):
        signal.signal(number, interrupt)
    try:
        with subprocess.Popen(sys.argv[2:]) as editor:
            receipt(directory, "ready", str(editor.pid))
            if interrupted is not None:
                editor.send_signal(interrupted)
            status = editor.wait()
            result = (
                f"interrupted:{interrupted}:{status}"
                if interrupted is not None and status != 0
                else str(status)
            )
    except OSError as exc:
        result = f"Could not {'start' if editor is None else 'supervise'} the editor: {exc}"
    receipt(directory, "result", result)


if __name__ == "__main__":
    main()
