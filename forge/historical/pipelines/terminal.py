"""Launch and supervise a dedicated desktop terminal for a pipeline stage."""

from __future__ import annotations

import asyncio
import os
import shutil
import socket
import subprocess
import sys
import tempfile
from collections.abc import Awaitable, Callable, Sequence
from contextlib import suppress
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Protocol
from uuid import uuid4

from forge.historical.engine.managed import checksum
from forge.historical.engine.process import EngineOutcomeUnknown
from forge.historical.normalization.service import write_job
from forge.orchestration.registry import current_execution
from forge.shared.errors import MarketForgeError
from forge.shared.identity import PIPELINE_STAGE_LABELS as STAGE_LABELS


class WindowUnavailable(MarketForgeError):
    """The worker was never authorized to execute."""


class WindowLost(EngineOutcomeUnknown):
    """A dispatched worker's outcome could not be confirmed."""


class Window(Protocol):
    async def wait(self) -> int: ...
    def terminate(self) -> None: ...
    def kill(self) -> None: ...


@dataclass(frozen=True)
class EngineRequest:
    argv: list[str]
    directory: str
    intent_sha256: str
    tui: bool


@dataclass
class WindowCompletion:
    result: asyncio.Future[dict[str, Any]]
    release: asyncio.Event


@dataclass(frozen=True)
class StageRequest:
    run_id: str
    pipeline_attempt: str
    stage: str
    dispatch_id: str
    environment: str
    registry_id: str
    project_root: str
    storage_root: str
    working_directory: str
    report_sha256: str
    channel: str
    outcome: str
    engine: EngineRequest | None

    def identity(self) -> dict[str, str]:
        return {
            "run_id": self.run_id,
            "pipeline_attempt": self.pipeline_attempt,
            "stage": self.stage,
            "dispatch_id": self.dispatch_id,
            "environment": self.environment,
            "registry_id": self.registry_id,
        }


@dataclass(frozen=True)
class Ghostty:
    executable: str
    name = "Ghostty"

    @classmethod
    def detect(cls) -> Ghostty:
        if sys.platform == "darwin":
            raise WindowUnavailable(
                "Managed stage windows currently require Linux Ghostty. Use --stage-terminal inline on macOS."
            )
        if sys.platform != "linux" or not (
            os.environ.get("WAYLAND_DISPLAY") or os.environ.get("DISPLAY")
        ):
            raise WindowUnavailable(
                "No Linux graphical display is available. In tmux, start a shell with the current desktop environment, or use --stage-terminal inline."
            )
        executable = shutil.which("ghostty")
        if not executable:
            raise WindowUnavailable("Install Ghostty or use --stage-terminal inline.")
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

    async def launch(self, request: Path, *, cwd: Path, title: str) -> Window:
        return await self.launch_module(
            "forge.historical.pipelines.stage_worker", request, cwd=cwd, title=title
        )

    async def launch_module(self, module: str, request: Path, *, cwd: Path, title: str) -> Window:
        # Isolate this window from any existing Ghostty instance or tmux session.
        env = {
            key: value
            for key, value in os.environ.items()
            if key not in {"TMUX", "TMUX_PANE", "MARKETFORGE_TUI_HOLD"}
        }
        return await asyncio.create_subprocess_exec(
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
            sys.executable,
            "-m",
            module,
            str(request),
            cwd=cwd,
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )


class Launcher(Protocol):
    name: str

    def launch(self, request: Path, *, cwd: Path, title: str) -> Awaitable[Window]: ...


async def close_window(window: Window, waiting: asyncio.Task[int], *, grace: float) -> None:
    try:
        await asyncio.wait_for(asyncio.shield(waiting), grace)
    except TimeoutError:
        with suppress(ProcessLookupError):
            window.terminate()
        try:
            await asyncio.wait_for(asyncio.shield(waiting), 2)
        except TimeoutError:
            with suppress(ProcessLookupError):
                window.kill()
            await waiting


@dataclass
class StageWindows:
    launcher: Launcher
    descriptors: Sequence[int]
    fallback: bool
    diagnostic: Callable[[str], None]
    startup_timeout: float = 15
    shutdown_timeout: float = 15
    started_stage: str | None = None

    async def dispatch(
        self,
        report: dict[str, Any],
        report_path: Path,
        stage: str,
        *,
        save: Callable[[], None],
        engine: EngineRequest | None = None,
        completion: WindowCompletion | None = None,
    ) -> dict[str, Any]:
        execution = current_execution()
        assert execution is not None
        dispatch_id = uuid4().hex
        outcome = execution.outcome.parent / "stage-windows" / f"{stage}-{dispatch_id}.json"
        cwd = Path.cwd()
        with tempfile.TemporaryDirectory(prefix="mf-stage-", dir="/tmp") as temporary:
            directory = Path(temporary)
            identity = {
                "run_id": execution.id,
                "pipeline_attempt": str(execution.attempt_id),
                "stage": stage,
                "dispatch_id": dispatch_id,
                "environment": execution.purpose,
                "registry_id": execution.registry,
            }
            report["terminal_dispatch"] = identity
            save()
            request = StageRequest(
                execution.id,
                str(execution.attempt_id),
                stage,
                dispatch_id,
                execution.purpose,
                execution.registry,
                str(execution.paths.root),
                str(execution.paths.data_root),
                str(cwd),
                checksum(report_path),
                str(directory / "socket"),
                str(outcome),
                engine,
            )
            path = directory / "request.json"
            write_job(path, asdict(request))
            title = f"MarketForge · {STAGE_LABELS[stage]} · {report['name']}"

            def outcome_ready() -> dict[str, Any]:
                result = self.read_outcome(request, report_path)
                if completion is not None and not completion.result.done():
                    completion.result.set_result(result)
                return result

            try:
                await self.exchange(
                    request,
                    path,
                    cwd=cwd,
                    title=title,
                    outcome_ready=outcome_ready,
                    completion=completion,
                )
            except asyncio.CancelledError:
                if completion is not None:
                    with suppress(WindowLost):
                        outcome_ready()
                raise
            return (
                self.read_outcome(request, report_path)
                if completion is None
                else completion.result.result()
            )

    @staticmethod
    def read_outcome(request: StageRequest, report_path: Path) -> dict[str, Any]:
        from forge.historical.pipelines.service import read

        try:
            result = read(Path(request.outcome))
            if result.get("identity") != request.identity() or result.get("report") != str(
                report_path
            ):
                raise ValueError
            if request.engine is None and (
                result.get("report_sha256") != checksum(report_path)
                or result.get("state") not in {"complete", "failed", "blocked", "interrupted"}
            ):
                raise ValueError
            if request.engine is not None and (
                result.get("report_sha256") != request.report_sha256
                or type(result.get("exit_code")) is not int
                or type(result.get("interrupted")) is not bool
            ):
                raise ValueError
        except (OSError, ValueError, MarketForgeError):
            raise WindowLost(
                f"{STAGE_LABELS[request.stage]} window ended without matching completion evidence. "
                f"Inspect {report_path} and resume the pipeline to reconcile retained work."
            ) from None
        return {**result, "outcome_path": request.outcome}

    async def exchange(
        self,
        request: StageRequest,
        path: Path,
        *,
        cwd: Path,
        title: str,
        outcome_ready: Callable[[], dict[str, Any]],
        completion: WindowCompletion | None,
    ) -> None:
        loop = asyncio.get_running_loop()
        connection: socket.socket | None = None
        window: Window | None = None
        waiting: asyncio.Task[int] | None = None
        authorized = False
        try:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as server:
                server.bind(request.channel)
                server.listen(1)
                server.setblocking(False)
                launch = asyncio.ensure_future(self.launcher.launch(path, cwd=cwd, title=title))
                try:
                    started_window = await asyncio.shield(launch)
                except asyncio.CancelledError:
                    cancelled_window = await launch
                    window = cancelled_window
                    waiting = asyncio.create_task(cancelled_window.wait())
                    raise
                window = started_window
                waiting = asyncio.create_task(started_window.wait())
                accept = asyncio.create_task(loop.sock_accept(server))
                try:
                    async with asyncio.timeout(self.startup_timeout):
                        done, _ = await asyncio.wait(
                            {accept, waiting}, return_when=asyncio.FIRST_COMPLETED
                        )
                        if accept not in done:
                            raise WindowUnavailable(
                                f"Ghostty exited ({waiting.result()}) before connecting. "
                                "Check the current Wayland/X11 display, especially in restored tmux sessions."
                            )
                        connection, _ = accept.result()
                        if socket.send_fds(connection, [b"L"], self.descriptors) != 1:
                            raise WindowUnavailable(
                                "Could not transfer workspace ownership to the stage worker."
                            )
                        connection.setblocking(False)
                        if await loop.sock_recv(connection, 1) != b"R":
                            error_path = path.with_name("startup-error.json")
                            message = "Stage worker could not validate its startup request."
                            with suppress(OSError, ValueError, MarketForgeError):
                                from forge.historical.pipelines.service import read

                                diagnostic = read(error_path).get("error")
                                if isinstance(diagnostic, str) and diagnostic:
                                    message = f"Stage worker could not start: {diagnostic}"
                            raise WindowUnavailable(message)
                        # After authorization is attempted the stage is never retried inline.
                        authorized = True
                        await loop.sock_sendall(connection, b"G")
                        if await loop.sock_recv(connection, 1) != b"S":
                            raise WindowLost(
                                "Stage worker disconnected after receiving authorization."
                            )
                        self.started_stage = request.stage
                        # The request and socket path are disposable once the peer is authorized.
                        with suppress(OSError):
                            os.unlink(path)
                            os.unlink(request.channel)
                            os.rmdir(path.parent)
                finally:
                    if not accept.done():
                        accept.cancel()
                        with suppress(asyncio.CancelledError):
                            await accept
                    elif (
                        connection is None and not accept.cancelled() and accept.exception() is None
                    ):
                        accept.result()[0].close()
            assert connection is not None
            receive = asyncio.create_task(loop.sock_recv(connection, 1))
            try:
                done, _ = await asyncio.wait(
                    {receive, waiting}, return_when=asyncio.FIRST_COMPLETED
                )
                if receive not in done:
                    try:
                        await asyncio.wait_for(asyncio.shield(receive), self.shutdown_timeout)
                    except TimeoutError:
                        raise WindowLost(
                            "The stage window closed before its worker reported an outcome."
                        ) from None
                notification = receive.result()
            finally:
                if not receive.done():
                    receive.cancel()
                    with suppress(asyncio.CancelledError):
                        await receive
            if notification not in {b"C", b""}:
                raise WindowLost("Invalid stage completion notification.")
            outcome_ready()
            if completion is not None and notification == b"C":
                await completion.release.wait()
                with suppress(OSError):
                    await loop.sock_sendall(connection, b"F")
        except TimeoutError:
            if authorized:
                raise WindowLost(
                    "Stage authorization was interrupted. Inspect the retained pipeline before resuming."
                ) from None
            raise WindowUnavailable(
                "Ghostty did not start a validated stage worker in time."
            ) from None
        except OSError as error:
            if authorized:
                raise WindowLost(
                    "Lost communication with the active stage worker. Inspect the run before resuming."
                ) from error
            raise WindowUnavailable(
                f"Could not start the Ghostty stage window ({error.strerror or type(error).__name__})."
            ) from None
        finally:
            self.started_stage = None
            if connection is not None:
                connection.close()
            if window is not None and waiting is not None:
                await close_window(
                    window, waiting, grace=self.shutdown_timeout if authorized else 0.25
                )
