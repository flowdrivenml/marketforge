"""Run the authorized Forge CLI with the Ghostty PTY as its real terminal."""

import asyncio
import errno
import os
import signal
import socket
import sys
import threading
from contextlib import suppress
from hashlib import sha256
from pathlib import Path
from typing import Any, cast

from forge.cli.main import main as cli
from forge.historical.engine.process import Shutdown, shutdown
from forge.historical.normalization.service import write_job
from forge.orchestration.registry import Execution, execution_started
from forge.shared.config.models import AppSettings
from forge.shared.database.connection import (
    database_environment,
    pinned_registry,
    selected_registry_id,
)
from forge.shared.errors import MarketForgeError
from forge.shared.paths import ProjectPaths
from forge.shared.terminal.console import create_console
from forge.startup.context import ApplicationContext
from forge.tui.execution_monitor import TerminalRequest, read_object


def run(directory: Path, control: socket.socket) -> int:
    request = TerminalRequest(**cast("dict[str, Any]", read_object(directory / "request.json")))
    paths = ProjectPaths.resolve(root=Path(request.root), data_root=Path(request.storage))
    settings = AppSettings.model_validate(request.settings)
    context = ApplicationContext(
        settings,
        paths,
        Path(request.settings_file) if request.settings_file else None,
        create_console(settings.presentation),
        create_console(settings.presentation, stderr=True),
        request.testing,
    )
    state = Shutdown()
    shutdown.set(state)
    done = threading.Event()
    execution: Execution | None = None

    def started(current: Execution) -> None:
        nonlocal execution
        execution = current
        with suppress(OSError):
            write_job(
                directory / "run.json", {"run_id": current.id, "outcome": str(current.outcome)}
            )

    interrupted = False

    def watch() -> None:
        with suppress(OSError):
            instruction = control.recv(1)
            if not done.is_set():
                state.emergency = instruction == b"E"
                os.kill(os.getpid(), signal.SIGTERM)

    def interrupt(_number: int, _frame: object) -> None:
        nonlocal interrupted
        if not interrupted and not done.is_set():
            interrupted = True
            os.kill(os.getpid(), signal.SIGINT)

    execution_started.set(started)
    for number in (signal.SIGHUP, signal.SIGTERM):
        signal.signal(number, interrupt)
    signal.signal(signal.SIGTSTP, signal.SIG_IGN)
    exit_status = 2
    diagnostic = None
    authorized = False
    try:
        if not all(os.isatty(fd) for fd in (0, 1, 2)):
            raise MarketForgeError("Ghostty did not provide a terminal to the CLI worker.")
        with database_environment(testing=request.testing), pinned_registry(request.registry):
            registry = asyncio.run(selected_registry_id(paths.root))
            control.sendall(b"R")
            if control.recv(1) != b"G":
                return 2
            authorized = True
            if request.reviewed_file and (
                sha256(Path(request.reviewed_file).read_bytes()).hexdigest()
                != request.reviewed_sha256
            ):
                raise MarketForgeError(
                    "The input changed after review. Reopen it and confirm the current version."
                )
            context.console.print(
                f"MarketForge · {'TESTING' if request.testing else 'PRODUCTION'} · "
                f"{request.reference}\nRegistry {registry}\n"
                "Ctrl+C or closing this window interrupts the operation.\n",
                markup=False,
            )
            with pinned_registry(registry):
                threading.Thread(target=watch, daemon=True).start()
                exit_status = cli(request.arguments, context=context)
    except KeyboardInterrupt:
        exit_status = 130
        diagnostic = "Execution interrupted."
    except (MarketForgeError, OSError, ValueError) as error:
        if interrupted and isinstance(error, OSError) and error.errno in (errno.EIO, errno.EPIPE):
            exit_status = 130
            diagnostic = "The execution terminal closed during interruption."
        else:
            diagnostic = str(error)
    except SystemExit as error:
        exit_status = error.code if isinstance(error.code, int) else 2
    finally:
        done.set()
    status = "completed" if exit_status == 0 else "interrupted" if exit_status == 130 else "failed"
    result: dict[str, object] = {"status": status, "exit_code": exit_status}
    if diagnostic:
        result["error"] = diagnostic
    if execution is not None:
        result["run_id"] = execution.id
        if execution.outcome.is_file():
            saved = read_object(execution.outcome)
            summary = saved.get("summary")
            if isinstance(summary, dict):
                result.update(summary)
            result["status"] = saved["status"]
            result["report_path"] = saved.get("report_path")
        else:
            result["status"] = "completion unconfirmed"
            result["error"] = "Inspect the retained execution intent in Runs before retrying."
    if state.unverified or state.process_count():
        result["status"] = "completion unconfirmed"
        result["error"] = "Process shutdown could not be verified. Inspect Runs before retrying."
    # A hung-up PTY can reject writes while retained execution evidence is
    # still available. Always report completion to the parent in that case.
    with suppress(OSError):
        if diagnostic:
            context.error_console.print(diagnostic, markup=False)
        if execution is not None:
            context.console.print(f"Run {execution.id}", markup=False)
        if result.get("report_path"):
            context.console.print(f"Retained report {result['report_path']}", markup=False)
        context.console.print(f"CLI finished with exit status {exit_status}.", markup=False)
    write_job(directory / "result.json", result)
    with suppress(OSError):
        control.sendall(b"C" if authorized else b"X")
    return exit_status


if __name__ == "__main__":
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as channel:
        root = Path(sys.argv[1])
        channel.connect(str(root / "control"))
        raise SystemExit(run(root, channel))
