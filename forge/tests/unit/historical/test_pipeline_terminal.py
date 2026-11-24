"""Local stage windows use an authorization handshake and retained outcomes."""

import asyncio
import json
import os
import socket
import sys
from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from forge.historical.engine.managed import checksum
from forge.historical.normalization.service import write_job
from forge.historical.pipelines.terminal import (
    EngineRequest,
    Ghostty,
    StageWindows,
    WindowCompletion,
    WindowLost,
    WindowUnavailable,
)
from forge.orchestration.registry import Execution, active
from forge.shared.identity import RunOperation
from forge.shared.ownership import OwnershipConflictError, WorkspaceOwnership
from forge.shared.paths import ProjectPaths


class MockWindow:
    def __init__(self, worker: asyncio.Task[int]) -> None:
        self.worker = worker

    async def wait(self) -> int:
        try:
            return await self.worker
        except asyncio.CancelledError:
            return -15

    def terminate(self) -> None:
        self.worker.cancel()

    def kill(self) -> None:
        self.worker.cancel()


class MockLauncher:
    name = "Mock terminal"

    def __init__(self, mode: str) -> None:
        self.mode = mode
        self.started = asyncio.Event()
        self.cancelled = asyncio.Event()
        self.launches: list[tuple[Path, Path, str, MockWindow]] = []

    async def launch(self, request: Path, *, cwd: Path, title: str) -> MockWindow:
        document = json.loads(await asyncio.to_thread(request.read_text))
        window = MockWindow(asyncio.create_task(self.worker(document)))
        self.launches.append((request, cwd, title, window))
        return window

    async def worker(self, request: dict[str, Any]) -> int:
        if self.mode == "exited":
            return 0
        if self.mode == "no-start":
            await asyncio.sleep(60)
            return 0
        loop = asyncio.get_running_loop()
        with socket.socket(socket.AF_UNIX) as channel:
            channel.setblocking(False)
            await loop.sock_connect(channel, request["channel"])
            while True:
                try:
                    message, descriptors, _, _ = socket.recv_fds(channel, 1, 2)
                    break
                except BlockingIOError:
                    await asyncio.sleep(0.005)
            try:
                assert message == b"L"
                # File descriptions survive closure of the parent's copy.
                assert len(descriptors) == 2 and all(os.fstat(fd).st_ino for fd in descriptors)
                await loop.sock_sendall(channel, b"R")
                assert await loop.sock_recv(channel, 1) == b"G"
                await loop.sock_sendall(channel, b"S")
                self.started.set()
                if self.mode == "lost":
                    return 0
                if self.mode == "cancel":
                    with suppress(ConnectionResetError):
                        assert await loop.sock_recv(channel, 1) == b""
                    self.cancelled.set()
                    return 130
                engine = request["engine"]
                exit_code = 1 if self.mode == "failure" else 0
                result: dict[str, object] = {
                    "identity": {
                        key: request[key]
                        for key in (
                            "run_id",
                            "pipeline_attempt",
                            "stage",
                            "dispatch_id",
                            "environment",
                            "registry_id",
                        )
                    },
                    "state": "failed" if self.mode == "failure" else "complete",
                    "error": "retained diagnostic" if self.mode == "failure" else None,
                    "report": str(Path(request["outcome"]).parents[3] / "report.json"),
                    "report_sha256": request["report_sha256"],
                    "exit_code": exit_code,
                    "interrupted": False,
                }
                write_job(Path(request["outcome"]), result)
                await loop.sock_sendall(channel, b"C")
                if engine is not None:
                    assert await loop.sock_recv(channel, 1) == b"F"
                return exit_code
            finally:
                for fd in descriptors:
                    os.close(fd)


@pytest.fixture
def execution(tmp_path: Path) -> Execution:
    paths = ProjectPaths(tmp_path, tmp_path / "data")
    identifier = f"pipeline-{uuid4()}"
    root = paths.runs_dir / identifier
    root.mkdir(parents=True)
    return Execution(
        paths,
        identifier,
        RunOperation.PIPELINE,
        "run",
        uuid4(),
        1,
        datetime.now(UTC),
        str(uuid4()),
        "testing",
        root,
        "unused",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ("acquire", "normalize", "merge", "convert"))
@pytest.mark.parametrize("mode", ("success", "failure"))
async def test_window_completion(execution: Execution, stage: str, mode: str) -> None:
    launcher = MockLauncher(mode)
    report: dict[str, object] = {"name": "fixture"}
    path = execution.root / "report.json"
    engine = EngineRequest([], "", "", False) if stage != "acquire" else None
    completion = (
        WindowCompletion(asyncio.get_running_loop().create_future(), asyncio.Event())
        if engine
        else None
    )
    with WorkspaceOwnership(execution.paths) as ownership, active(execution):
        windows = StageWindows(launcher, ownership.descriptors(), False, lambda _: None)
        task = asyncio.create_task(
            windows.dispatch(
                report,
                path,
                stage,
                save=lambda: write_job(path, report),
                engine=engine,
                completion=completion,
            )
        )
        if completion:
            outcome = await asyncio.wait_for(completion.result, 2)
            assert launcher.started.is_set() and not task.done()
            completion.release.set()
        outcome = await asyncio.wait_for(task, 2)
    assert outcome["state"] == ("complete" if mode == "success" else "failed")
    if mode == "failure":
        assert outcome["error"] == "retained diagnostic"
    request, cwd, title, window = launcher.launches[0]
    assert not request.parent.exists() and window.worker.done()
    assert cwd == Path.cwd() and title.startswith("MarketForge · ") and title.endswith(" · fixture")
    assert len(list(execution.outcome.parent.glob("stage-windows/*.json"))) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mode,error",
    (("exited", WindowUnavailable), ("no-start", WindowUnavailable), ("lost", WindowLost)),
)
async def test_invalid_window_outcome(
    execution: Execution, mode: str, error: type[Exception]
) -> None:
    launcher = MockLauncher(mode)
    report: dict[str, object] = {"name": "fixture"}
    path = execution.root / "report.json"
    with WorkspaceOwnership(execution.paths) as ownership, active(execution):
        windows = StageWindows(
            launcher, ownership.descriptors(), False, lambda _: None, startup_timeout=0.1
        )
        with pytest.raises(error):
            await windows.dispatch(report, path, "acquire", save=lambda: write_job(path, report))
    assert launcher.started.is_set() == (mode == "lost")
    assert not launcher.launches[0][0].parent.exists()
    assert not list(execution.outcome.parent.glob("stage-windows/*.json"))


@pytest.mark.asyncio
async def test_cancel_notifies_and_joins_window(execution: Execution) -> None:
    launcher = MockLauncher("cancel")
    report: dict[str, object] = {"name": "fixture"}
    path = execution.root / "report.json"
    with WorkspaceOwnership(execution.paths) as ownership, active(execution):
        windows = StageWindows(launcher, ownership.descriptors(), False, lambda _: None)
        task = asyncio.create_task(
            windows.dispatch(report, path, "acquire", save=lambda: write_job(path, report))
        )
        await asyncio.wait_for(launcher.started.wait(), 2)
        ownership.release()
        with pytest.raises(OwnershipConflictError):
            WorkspaceOwnership(execution.paths).acquire()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert launcher.cancelled.is_set()
    assert launcher.launches[0][3].worker.done()
    assert path.is_file() and checksum(path)


@pytest.mark.asyncio
async def test_ghostty_uses_new_instance_and_worker_argv(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("TMUX", "/tmp/tmux/socket")
    monkeypatch.setenv("TMUX_PANE", "%1")
    monkeypatch.setenv("MARKETFORGE_TUI_HOLD", "1")
    launch = AsyncMock()
    monkeypatch.setattr(asyncio, "create_subprocess_exec", launch)
    request = tmp_path / "request with space.json"
    await Ghostty("/usr/bin/ghostty").launch(request, cwd=tmp_path, title="MarketForge · Merge")
    call = launch.await_args
    assert call is not None
    args, kwargs = call.args, call.kwargs
    assert args[0] == "/usr/bin/ghostty"
    assert "--gtk-single-instance=false" in args and "--wait-after-command=false" in args
    assert "--title=MarketForge · Merge" in args and f"--working-directory={tmp_path}" in args
    assert args[-5:] == (
        "-e",
        sys.executable,
        "-m",
        "forge.historical.pipelines.stage_worker",
        str(request),
    )
    assert kwargs["cwd"] == tmp_path and kwargs["start_new_session"]
    assert not {"TMUX", "TMUX_PANE", "MARKETFORGE_TUI_HOLD"} & kwargs["env"].keys()


@pytest.mark.parametrize("platform,expected", (("darwin", "macOS"), ("linux", "graphical display")))
def test_unavailable_displays(
    monkeypatch: pytest.MonkeyPatch, platform: str, expected: str
) -> None:
    monkeypatch.setattr(sys, "platform", platform)
    monkeypatch.delenv("DISPLAY", raising=False)
    monkeypatch.delenv("WAYLAND_DISPLAY", raising=False)
    with pytest.raises(WindowUnavailable, match=expected):
        Ghostty.detect()


@pytest.mark.parametrize(
    "mode,interactive,as_json,available,expected",
    [
        ("auto", True, False, True, "window"),
        ("auto", True, False, False, "fallback"),
        ("auto", False, False, True, "inline"),
        ("auto", True, True, True, "inline"),
        ("inline", True, False, True, "inline"),
        ("window", False, True, True, "window"),
        ("window", True, False, False, "error"),
    ],
)
def test_cli_mode_selection(
    execution: Execution,
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
    interactive: bool,
    as_json: bool,
    available: bool,
    expected: str,
) -> None:
    from argparse import Namespace
    from io import StringIO

    from rich.console import Console

    from forge.cli.commands.pipeline import stage_windows
    from forge.shared.config.models import AppSettings
    from forge.startup.context import ApplicationContext

    class Terminal(StringIO):
        def isatty(self) -> bool:
            return interactive

    def detect() -> Ghostty:
        if not available:
            raise WindowUnavailable("Ghostty unavailable.")
        return Ghostty("/fake/ghostty")

    for stream in ("stdin", "stdout", "stderr"):
        monkeypatch.setattr(sys, stream, Terminal())
    monkeypatch.setattr(Ghostty, "detect", detect)
    output = StringIO()
    console = Console(file=output)
    context = ApplicationContext(AppSettings(), execution.paths, None, console, console)
    with WorkspaceOwnership(execution.paths) as owner:
        args = Namespace(stage_terminal=mode, as_json=as_json)
        if expected == "error":
            with pytest.raises(WindowUnavailable):
                stage_windows(args, context, owner)
        else:
            result = stage_windows(args, context, owner)
            assert (result is not None) == (expected == "window")
    assert ("Continuing inline." in output.getvalue()) == (expected == "fallback")
